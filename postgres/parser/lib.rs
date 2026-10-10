use pg_query::ParseResult;
use thiserror::Error;

pub mod translator;

/// libpg_query's bindings, for callers that edit a parse tree (the frontend's table rebuild).
pub use pg_query;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("{0}")]
    ParseError(String),
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
    pg_query::parse(sql).map_err(|e| ParseError::ParseError(e.to_string()))
}

/// Deparse a parse tree back to SQL text (libpg_query's deparser).
pub fn deparse(protobuf: &pg_query::protobuf::ParseResult) -> Result<String, ParseError> {
    count_libpg_query_call();
    pg_query::deparse(protobuf).map_err(|e| ParseError::ParseError(e.to_string()))
}

/// The highest parameter number a statement may hold: Bind counts parameters in 16 bits. A `$n`
/// outside 1..=MAX_PARAMETER names no parameter (42P02) on every path, a branch call's included,
/// before anything is sized by it (wire review 8 item 3, review 9 item 1); one above i32::MAX is
/// 42601, as PostgreSQL 18's scanner refuses it ([`checked_param_numbers`]; review 12 item 1).
pub const MAX_PARAMETER: u32 = 65535;

/// Every parameter number ($n) in a parse tree, sorted and without repeats, from the WHOLE tree:
/// WITH, ON CONFLICT, RETURNING, set operations and every other clause, whether or not the
/// translator or the engine keeps it (the engine registers only the $n it compiles: a $n in a clause
/// it folds away has no slot; wire review 8 item 5). No libpg_query call: the tree is serialized
/// (pg_query's protobuf types derive Serialize) and each `"ParamRef":{"number":N,"location":L`
/// read off it, a pattern a string value in the tree cannot hold, since its quotes are escaped.
/// The numbers are libpg_query's, which wrap above i32::MAX: [`checked_param_numbers`] reads them
/// as PostgreSQL 18 does. Only for a statement whose text holds a '$'.
pub fn param_numbers(parse: &ParseResult) -> Vec<i32> {
    let mut numbers: Vec<i32> = param_refs(parse)
        .unwrap_or_default()
        .into_iter()
        .map(|(number, _)| number)
        .collect();
    numbers.sort_unstable();
    numbers.dedup();
    numbers
}

/// [`param_numbers`], each number read again from `sql` (the text `parse` parsed) at its ParamRef's
/// location, as PostgreSQL 18's scanner reads it. libpg_query reads a `$n` with atol into a 32-bit
/// int (PostgreSQL 17's scanner), so `$4294967297` is $1 in its tree, and a branch call of it bound
/// the wrong parameter and created a branch (wire review 12 item 1). A number above i32::MAX is
/// PostgreSQL 18's "parameter number too large" (42601); a tree number that is not the text's, or a
/// tree whose parameters cannot be read, is refused (fail closed).
pub fn checked_param_numbers(parse: &ParseResult, sql: &str) -> Result<Vec<i32>, ParseError> {
    let unreadable = || {
        ParseError::ParseError(
            "could not read the statement's parameters from its parse tree".to_string(),
        )
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
                )))
            }
        }
    }
    numbers.sort_unstable();
    numbers.dedup();
    Ok(numbers)
}

/// Each ParamRef's number and byte location in a parse tree, read off its serialization (see
/// [`param_numbers`]); None if the tree cannot be serialized or a ParamRef not read.
fn param_refs(parse: &ParseResult) -> Option<Vec<(i32, usize)>> {
    const KEY: &str = "\"ParamRef\":{\"number\":";
    const LOCATION: &str = ",\"location\":";
    let json = serde_json::to_string(&parse.protobuf).ok()?;
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

/// One byte of PostgreSQL's whitespace (its lexer's `space`): space, tab, newline, carriage return,
/// form feed and vertical tab (which `is_ascii_whitespace` omits), and no byte of a multi-byte
/// character, which PostgreSQL lexes as an identifier byte (wire review 13 item 1). The one
/// definition the server's verb reader, empty-statement test and CHECKPOINT test and the
/// branch-call fast path read (wire review 14 item 6).
pub fn pg_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b)
}

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
    /// the parser refuses (42601), so no count is taken from it.
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

/// [`ParamScan`] of `sql`, read as PostgreSQL's lexer reads it: a `$` followed by digits is a
/// parameter unless it continues an identifier (`a$3`, whose `$` is an identifier byte); nothing
/// inside a string (standard, `E'..'` with backslash escapes, `U&'..'`, `B'..'`, `X'..'`), a quoted
/// identifier, a dollar-quoted string or a comment (`--`, nested `/* */`) is a parameter. One pass,
/// no allocation but the first word's. The server reads it at Parse to count a Bind's values
/// before anything runs: it tested for a `$` byte, so a `$` in a comment or a quoted name skipped
/// the count and Execute's prepare performed a SET before refusing it (wire review 17 item 2).
pub fn scan_params(sql: &str) -> ParamScan {
    let b = sql.as_bytes();
    let ident_start = |c: u8| c.is_ascii_alphabetic() || c == b'_' || c >= 0x80;
    let ident_cont = |c: u8| ident_start(c) || c.is_ascii_digit() || c == b'$';
    // The end of the quoted token opening at `i` (its closing quote doubled inside it; with
    // `backslash`, a backslash escapes the next byte), or the text's end when it never closes.
    let quoted = |i: usize, q: u8, backslash: bool| {
        let mut j = i + 1;
        while j < b.len() {
            if backslash && b[j] == b'\\' {
                j += 2;
            } else if b[j] == q {
                if b.get(j + 1) == Some(&q) {
                    j += 2;
                } else {
                    return j + 1;
                }
            } else {
                j += 1;
            }
        }
        b.len()
    };
    let mut scan = ParamScan::default();
    let mut highest: u64 = 0;
    let mut first: Option<String> = None;
    let mut depth = 0usize;
    // CREATE TABLE ... AS <query>, read at depth 0: TABLE seen before an AS, and the AS followed
    // by a query.
    let (mut saw_table, mut after_as) = (false, false);
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if pg_space(c) {
            i += 1;
            continue;
        }
        match sql_comment(&b[i..]) {
            Some(Some(len)) => {
                i += len;
                continue;
            }
            Some(None) => break,
            None => {}
        }
        let at_depth = depth;
        let token_start = i;
        let mut word: Option<&[u8]> = None;
        match c {
            b'\'' => i = quoted(i, b'\'', false),
            b'"' => i = quoted(i, b'"', false),
            b'$' if b.get(i + 1).is_some_and(u8::is_ascii_digit) => {
                let mut j = i + 1;
                let mut n: u64 = 0;
                while j < b.len() && b[j].is_ascii_digit() {
                    n = n.saturating_mul(10).saturating_add(u64::from(b[j] - b'0'));
                    j += 1;
                }
                highest = highest.max(n);
                i = j;
            }
            b'$' => {
                // A dollar quote: $tag$ ... $tag$, the tag empty or an identifier without `$`.
                let mut j = i + 1;
                if j < b.len() && ident_start(b[j]) {
                    while j < b.len() && (ident_start(b[j]) || b[j].is_ascii_digit()) {
                        j += 1;
                    }
                }
                if b.get(j) == Some(&b'$') {
                    let delimiter = &b[i..=j];
                    let body = j + 1;
                    i = b[body..]
                        .windows(delimiter.len())
                        .position(|w| w == delimiter)
                        .map_or(b.len(), |p| body + p + delimiter.len());
                } else {
                    i += 1;
                }
            }
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            c if ident_start(c) => {
                while i < b.len() && ident_cont(b[i]) {
                    i += 1;
                }
                let w = &b[token_start..i];
                if w.eq_ignore_ascii_case(b"e") && b.get(i) == Some(&b'\'') {
                    // E'..': a string whose backslash escapes the next byte.
                    i = quoted(i, b'\'', true);
                } else {
                    word = Some(w);
                }
            }
            c if c.is_ascii_digit() => {
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'.' || b[i] == b'_')
                {
                    i += 1;
                }
            }
            _ => i += 1,
        }
        let paren = c == b'(';
        if first.is_none() {
            first = Some(match word {
                Some(w) => String::from_utf8_lossy(w).to_ascii_uppercase(),
                None if paren => "(".to_string(),
                None => String::new(),
            });
        }
        if first.as_deref() == Some("CREATE") && at_depth == 0 {
            let is = |k: &str| word.is_some_and(|w| w.eq_ignore_ascii_case(k.as_bytes()));
            if after_as {
                if saw_table
                    && (paren
                        || ["SELECT", "WITH", "VALUES", "TABLE", "EXECUTE"]
                            .iter()
                            .any(|k| is(k)))
                {
                    scan.analysed = true;
                }
                after_as = false;
            }
            if is("AS") {
                after_as = true;
            } else if is("TABLE") {
                saw_table = true;
            }
        }
    }
    scan.highest = u32::try_from(highest).ok().filter(|n| *n <= MAX_PARAMETER);
    if let Some(first) = first.as_deref() {
        if matches!(
            first,
            "SELECT"
                | "VALUES"
                | "TABLE"
                | "WITH"
                | "("
                | "INSERT"
                | "UPDATE"
                | "DELETE"
                | "MERGE"
                | "CALL"
                | "EXPLAIN"
                | "DECLARE"
        ) {
            scan.analysed = true;
        }
    }
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

/// Split a multi-statement SQL string into individual statements.
/// Uses pg_query's scanner which correctly handles semicolons inside
/// string literals, comments, and dollar-quoted strings.
/// Returns the individual statement strings (without trailing semicolons).
///
/// The scanner emits a statement only where it saw a keyword, and skips every other stretch of the
/// text: `COMMIT<NBSP>` (one identifier) between two statements was dropped, so `INSERT ..;
/// COMMIT<NBSP>; INSERT ..` ran both INSERTs and nothing answered 42601. So every stretch before,
/// between and after the statements must be blank (whitespace, comments, `;`), or the text is
/// refused here: the caller then prepares it whole, and the parser refuses it, as PostgreSQL parses
/// the whole string before it runs any of it (wire review 16 item 1).
pub fn split_statements(sql: &str) -> Result<Vec<String>, ParseError> {
    count_libpg_query_call();
    let parts =
        pg_query::split_with_scanner(sql).map_err(|e| ParseError::ParseError(e.to_string()))?;
    let b = sql.as_bytes();
    let blank = |gap: &[u8]| skip_blank(gap, 0) == Some(gap.len());
    let stray = || ParseError::ParseError("a part of the text is no statement".to_string());
    let mut at = 0;
    for part in &parts {
        // Each part is a slice of `sql` (split_with_scanner returns `&query[start..end]`).
        let start = (part.as_ptr() as usize)
            .checked_sub(sql.as_ptr() as usize)
            .filter(|start| *start >= at && *start + part.len() <= b.len())
            .ok_or_else(stray)?;
        if !blank(&b[at..start]) {
            return Err(stray());
        }
        at = start + part.len();
    }
    if !blank(&b[at..]) {
        return Err(stray());
    }
    Ok(parts
        .into_iter()
        .map(|s| pg_trim(&s).to_string())
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
    pg_query::normalize(sql).map_err(|e| ParseError::ParseError(e.to_string()))
}

/// Get a fingerprint for a query (for caching/deduplication)
pub fn fingerprint(sql: &str) -> Result<String, ParseError> {
    count_libpg_query_call();
    pg_query::fingerprint(sql)
        .map(|fp| fp.hex)
        .map_err(|e| ParseError::ParseError(e.to_string()))
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

    /// param_numbers finds every $n in the tree, in clauses the engine folds away or the translator
    /// never reads (WITH, ON CONFLICT, a false AND, HAVING), and none in a string, a comment, a
    /// quoted identifier or a dollar-quoted string that looks like one (wire review 8 item 5).
    #[test]
    fn param_numbers_reads_the_whole_tree() {
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
            assert_eq!(param_numbers(&parsed), want, "{sql}");
        }
    }
}
