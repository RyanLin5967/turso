//! fastest-engine red (second adversary of the REVIEW-engine-2bdcd32c6 HIGH 3 fix): the expression
//! printer may add parentheses only where SQLite's parser would read the text back as another tree.
//! A tree SQLite's parser built must print as its source did (up to spacing), so stored and shown SQL
//! (CREATE INDEX text, triggers, CHECKs, view columns) does not change. The forms below are the ones
//! a precedence-only printer wrapped needlessly: a prefix NOT on the right of an operator, and a
//! postfix IN / ISNULL / NOTNULL on the left. Each printed text must also print again unchanged.

use turso_parser::parser::Parser;

fn print(sql: &str) -> Result<String, String> {
    match Parser::new(sql.as_bytes()).next_cmd() {
        Ok(Some(cmd)) => Ok(cmd.to_string()),
        Ok(None) => Err("no statement".to_string()),
        Err(e) => Err(e.to_string()),
    }
}

fn squeeze(s: &str) -> String {
    s.chars().filter(|c| !c.is_whitespace() && *c != ';').collect()
}

#[test]
fn a_parsed_expression_prints_as_its_source() {
    let sources = [
        "SELECT x = NOT y",
        "SELECT 1 + NOT a = b",
        "SELECT x BETWEEN NOT a AND b",
        "SELECT x LIKE NOT a",
        "SELECT -NOT a",
        "SELECT a IN (1, 2) + 1",
        "SELECT a NOT IN (1, 2) * 2",
        "SELECT a ISNULL || 'x'",
        "SELECT a NOTNULL + 1",
        "SELECT a + b * c",
        "SELECT (a + b) * c",
        "SELECT a - (b - c)",
        "SELECT a - b - c",
        "SELECT NOT a AND b",
        "SELECT NOT (a AND b)",
        "SELECT a OR b AND c",
        "SELECT (a OR b) AND c",
        "SELECT -a * b",
        "SELECT -(a * b)",
        "SELECT a BETWEEN 1 AND 2 + 3",
        "SELECT a LIKE b ESCAPE c",
        "SELECT a = b ISNULL",
        "SELECT a || b + c",
        "SELECT a < b = c",
        "SELECT a = (b < c)",
    ];
    let mut failed = Vec::new();
    for src in sources {
        match print(src) {
            Ok(text) => {
                if squeeze(&text) != squeeze(src) {
                    failed.push(format!("`{src}` printed `{text}`"));
                }
                match print(&text) {
                    Ok(again) if again == text => {}
                    Ok(again) => failed.push(format!("`{src}` printed `{text}`, which prints again as `{again}`")),
                    Err(e) => failed.push(format!("`{src}` printed `{text}`, which does not parse: {e}")),
                }
            }
            Err(e) => failed.push(format!("`{src}`: {e}")),
        }
    }
    assert!(failed.is_empty(), "CLAIM: a parsed expression prints as its source: {failed:#?}");
}
