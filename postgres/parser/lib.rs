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
/// before anything is sized by it (wire review 8 item 3, review 9 item 1).
pub const MAX_PARAMETER: u32 = 65535;

/// Every parameter number ($n) in a parse tree, sorted and without repeats, from the WHOLE tree:
/// WITH, ON CONFLICT, RETURNING, set operations and every other clause, whether or not the
/// translator or the engine keeps it (the engine registers only the $n it compiles: a $n in a clause
/// it folds away has no slot; wire review 8 item 5). No libpg_query call: the tree is serialized
/// (pg_query's protobuf types derive Serialize) and each `"ParamRef":{"number":N` read off it, a
/// pattern a string value in the tree cannot hold, since its quotes are escaped. A ParamRef's first
/// field is its number. Only for a statement whose text holds a '$'.
pub fn param_numbers(parse: &ParseResult) -> Vec<i32> {
    const KEY: &str = "\"ParamRef\":{\"number\":";
    let Ok(json) = serde_json::to_string(&parse.protobuf) else {
        return Vec::new();
    };
    let mut numbers: Vec<i32> = json
        .match_indices(KEY)
        .filter_map(|(at, _)| {
            let rest = &json[at + KEY.len()..];
            let end = rest
                .find(|c: char| !(c.is_ascii_digit() || c == '-'))
                .unwrap_or(rest.len());
            rest[..end].parse().ok()
        })
        .collect();
    numbers.sort_unstable();
    numbers.dedup();
    numbers
}

/// Split a multi-statement SQL string into individual statements.
/// Uses pg_query's scanner which correctly handles semicolons inside
/// string literals, comments, and dollar-quoted strings.
/// Returns the individual statement strings (without trailing semicolons).
pub fn split_statements(sql: &str) -> Result<Vec<String>, ParseError> {
    count_libpg_query_call();
    let parts =
        pg_query::split_with_scanner(sql).map_err(|e| ParseError::ParseError(e.to_string()))?;
    Ok(parts
        .into_iter()
        .map(|s| s.trim().to_string())
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
