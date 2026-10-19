//! COPY FROM text format parser.
//!
//! Implements PostgreSQL's default COPY text format:
//! - One row per line (`\n`)
//! - Columns separated by tab (`\t`) by default
//! - `\N` = NULL
//! - Backslash escapes: `\\` = `\`, `\t` = tab, `\n` = newline, `\r` = CR
//! - Empty field = empty string (not NULL)
//! - Lines starting with `\.` = end of data

use crate::{LimboError, Result};
use turso_parser::sql_error::{SqlErrorKind, SqlErrorMessage};

/// A parsed row from COPY text format: each element is None for NULL, Some for a value.
type CopyRow = Vec<Option<String>>;

/// [`parse_copy_rows`] for `num_columns` columns named column1, column2, ...
#[cfg(test)]
pub fn parse_copy_text_format(
    data: &str,
    delimiter: char,
    null_string: &str,
    num_columns: usize,
) -> Result<Vec<CopyRow>> {
    let columns: Vec<String> = (1..=num_columns).map(|i| format!("column{i}")).collect();
    parse_copy_rows(data, delimiter, null_string, &columns)
}

/// Parse COPY text format data into rows of values for `columns`, the columns COPY loads, in
/// order. A line with fewer fields than columns, or more, is 22P04 as PostgreSQL's CopyReadAttributes
/// answers it ("missing data for column \"v\"", "extra data after last expected column"); it was
/// 42601 "COPY: line N: expected 2 columns, got 1" (recorded, cli/tests/copy-stdin-pg18.txt).
pub fn parse_copy_rows(
    data: &str,
    delimiter: char,
    null_string: &str,
    columns: &[String],
) -> Result<Vec<CopyRow>> {
    let mut rows = Vec::new();
    let bad_format = |text: String| {
        LimboError::InvalidArgument(SqlErrorMessage::classified(SqlErrorKind::BadCopyFileFormat, text))
    };

    for line in data.lines() {
        // End-of-data marker (used in STDIN mode, but handle it for files too)
        if line == "\\." {
            break;
        }

        // Every line is a row, an empty one included: one empty field, as PostgreSQL's
        // CopyReadAttributesText reads it (an empty string for a one-column COPY, else 22P04;
        // recorded, copy-stdin-pg18.txt `escapes`). Empty lines were skipped.
        let fields = split_copy_fields(line, delimiter);
        if fields.len() > columns.len() {
            return Err(bad_format("extra data after last expected column".to_string()));
        }
        if let Some(missing) = columns.get(fields.len()) {
            return Err(bad_format(format!("missing data for column \"{missing}\"")));
        }

        let row: CopyRow = fields
            .iter()
            .map(|field| {
                if *field == null_string {
                    None
                } else {
                    Some(unescape_copy_field(field))
                }
            })
            .collect();

        rows.push(row);
    }

    Ok(rows)
}

/// A line's fields as written (escapes still in them), split at each delimiter that no backslash
/// escapes: a backslash and the character after it are one unit of data, so `x\<TAB>y` is one
/// field, as in PostgreSQL's text format. The line was split at every delimiter.
fn split_copy_fields(line: &str, delimiter: char) -> Vec<&str> {
    let mut fields = Vec::new();
    let mut start = 0;
    let mut chars = line.char_indices();
    while let Some((i, c)) = chars.next() {
        if c == '\\' {
            chars.next();
        } else if c == delimiter {
            fields.push(&line[start..i]);
            start = i + c.len_utf8();
        }
    }
    fields.push(&line[start..]);
    fields
}

/// Unescape backslash sequences in a COPY text field, as PostgreSQL's text format reads them:
/// `\b \f \n \r \t \v`, an octal `\N`, `\NN` or `\NNN`, a hex `\xH` or `\xHH` (a byte; one that
/// is not ASCII is kept as its character), and a backslash before any other character is that
/// character.
fn unescape_copy_field(field: &str) -> String {
    let mut result = String::with_capacity(field.len());
    let mut chars = field.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\\') => result.push('\\'),
                Some('t') => result.push('\t'),
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('b') => result.push('\x08'),
                Some('f') => result.push('\x0C'),
                Some('v') => result.push('\x0B'),
                Some(d @ '0'..='7') => {
                    let mut value = d.to_digit(8).unwrap_or(0);
                    for _ in 0..2 {
                        match chars.peek().and_then(|c| c.to_digit(8)) {
                            Some(digit) => {
                                value = value * 8 + digit;
                                chars.next();
                            }
                            None => break,
                        }
                    }
                    result.push(char::from((value & 0xff) as u8));
                }
                Some('x') if chars.peek().is_some_and(|c| c.is_ascii_hexdigit()) => {
                    let mut value = 0;
                    for _ in 0..2 {
                        match chars.peek().and_then(|c| c.to_digit(16)) {
                            Some(digit) => {
                                value = value * 16 + digit;
                                chars.next();
                            }
                            None => break,
                        }
                    }
                    result.push(char::from(value as u8));
                }
                Some(other) => {
                    result.push(other);
                }
                None => {
                    result.push('\\');
                }
            }
        } else {
            result.push(c);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_tsv() {
        let data = "1\thello\n2\tworld\n";
        let rows = parse_copy_text_format(data, '\t', "\\N", 2).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![Some("1".into()), Some("hello".into())]);
        assert_eq!(rows[1], vec![Some("2".into()), Some("world".into())]);
    }

    #[test]
    fn test_null_values() {
        let data = "1\t\\N\n\\N\thello\n";
        let rows = parse_copy_text_format(data, '\t', "\\N", 2).unwrap();
        assert_eq!(rows[0], vec![Some("1".into()), None]);
        assert_eq!(rows[1], vec![None, Some("hello".into())]);
    }

    #[test]
    fn test_empty_string() {
        let data = "1\t\n";
        let rows = parse_copy_text_format(data, '\t', "\\N", 2).unwrap();
        assert_eq!(rows[0], vec![Some("1".into()), Some(String::new())]);
    }

    #[test]
    fn test_backslash_escapes() {
        let data = "hello\\\\world\tline1\\nline2\n";
        let rows = parse_copy_text_format(data, '\t', "\\N", 2).unwrap();
        assert_eq!(
            rows[0],
            vec![Some("hello\\world".into()), Some("line1\nline2".into())]
        );
    }

    #[test]
    fn test_end_of_data_marker() {
        let data = "1\thello\n\\.\n2\tworld\n";
        let rows = parse_copy_text_format(data, '\t', "\\N", 2).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![Some("1".into()), Some("hello".into())]);
    }

    #[test]
    fn test_wrong_column_count() {
        let data = "1\t2\t3\n";
        let result = parse_copy_text_format(data, '\t', "\\N", 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_custom_delimiter() {
        let data = "1,hello\n2,world\n";
        let rows = parse_copy_text_format(data, ',', "\\N", 2).unwrap();
        assert_eq!(rows[0], vec![Some("1".into()), Some("hello".into())]);
    }

    #[test]
    fn test_custom_null_string() {
        let data = "1\tNULL\n";
        let rows = parse_copy_text_format(data, '\t', "NULL", 2).unwrap();
        assert_eq!(rows[0], vec![Some("1".into()), None]);
    }

    #[test]
    fn test_header_skip() {
        let data = "id\tname\n1\thello\n";
        let rows = parse_copy_text_format(data, '\t', "\\N", 2).unwrap();
        assert_eq!(rows.len(), 2);
    }
}
