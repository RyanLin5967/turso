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

    for line in split_copy_lines(data)? {
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
                    Ok(None)
                } else {
                    unescape_copy_field(field).map(Some)
                }
            })
            .collect::<Result<_>>()?;

        rows.push(row);
    }

    Ok(rows)
}

fn bad_format(text: String) -> LimboError {
    LimboError::InvalidArgument(SqlErrorMessage::classified(SqlErrorKind::BadCopyFileFormat, text))
}

/// PostgreSQL's 22021 for bytes that are not UTF-8 (the client's encoding), naming the first bad
/// sequence as PostgreSQL does (`0xff`, `0xc3 0x28`); a NUL byte is one too (0x00), as PostgreSQL
/// refuses it in text.
pub fn invalid_utf8(bad: &[u8]) -> LimboError {
    let shown: Vec<String> = bad.iter().map(|b| format!("0x{b:02x}")).collect();
    LimboError::InvalidArgument(SqlErrorMessage::classified(
        SqlErrorKind::CharacterNotInRepertoire,
        format!("invalid byte sequence for encoding \"UTF8\": {}", shown.join(" ")),
    ))
}

/// `bytes` as UTF-8 text with no NUL, or [`invalid_utf8`] naming the first bad sequence.
pub fn copy_text(bytes: &[u8]) -> Result<&str> {
    let text = std::str::from_utf8(bytes).map_err(|e| {
        let bad = &bytes[e.valid_up_to()..];
        invalid_utf8(&bad[..e.error_len().unwrap_or(bad.len()).min(bad.len())])
    })?;
    if text.contains('\0') {
        return Err(invalid_utf8(&[0]));
    }
    Ok(text)
}

/// The data's lines, as PostgreSQL's CopyReadLineText reads them: a newline ends a line (a
/// carriage return before it is dropped) unless a backslash escapes it, so `a\<LF>b` is one line
/// holding a newline in its field; `\.` alone on its line ends the data, and `\.` anywhere else is
/// 22P04 "end-of-copy marker is not alone on its line" (PostgreSQL 18; recorded,
/// copy-stdin-pg18.txt `bsnewline`, `dotnotalone`). `lines()` ended a line at an escaped newline,
/// and loaded `abc\.` as `abc.`.
fn split_copy_lines(data: &str) -> Result<Vec<&str>> {
    let b = data.as_bytes();
    let mut lines = Vec::new();
    let (mut start, mut i) = (0, 0);
    while i < b.len() {
        match b[i] {
            b'\\' if b.get(i + 1) == Some(&b'.') => {
                let ends = matches!(b.get(i + 2), None | Some(b'\n') | Some(b'\r'));
                if i == start && ends {
                    return Ok(lines);
                }
                return Err(bad_format("end-of-copy marker is not alone on its line".to_string()));
            }
            // The escaped byte is data: ASCII (a newline, the delimiter) or the lead byte of a
            // character, whose continuation bytes are never a newline or a backslash.
            b'\\' => i += 2,
            b'\n' => {
                let end = if i > start && b[i - 1] == b'\r' { i - 1 } else { i };
                lines.push(&data[start..end]);
                i += 1;
                start = i;
            }
            _ => i += 1,
        }
    }
    if start < b.len() {
        let end = if b[b.len() - 1] == b'\r' { b.len() - 1 } else { b.len() };
        lines.push(&data[start..end.max(start)]);
    }
    Ok(lines)
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
/// `\b \f \n \r \t \v`, an octal `\N`, `\NN` or `\NNN` and a hex `\xH` or `\xHH`, each one BYTE,
/// and a backslash before any other character is that character. The bytes must make UTF-8 with
/// no NUL, else 22021 as PostgreSQL's pg_verifymbstr answers (recorded, copy-stdin-pg18.txt
/// `utf8esc`, `badbyte`, `nul`): `\303\251` is é. Each escape was taken as the character of its
/// number, so `\303\251` stored Ã© and `\377` stored ÿ.
fn unescape_copy_field(field: &str) -> Result<String> {
    if !field.contains('\\') {
        return Ok(field.to_string());
    }
    let mut result: Vec<u8> = Vec::with_capacity(field.len());
    let mut chars = field.chars().peekable();
    fn push(result: &mut Vec<u8>, c: char) {
        let mut buf = [0u8; 4];
        result.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
    }

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\\') => result.push(b'\\'),
                Some('t') => result.push(b'\t'),
                Some('n') => result.push(b'\n'),
                Some('r') => result.push(b'\r'),
                Some('b') => result.push(0x08),
                Some('f') => result.push(0x0C),
                Some('v') => result.push(0x0B),
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
                    result.push((value & 0xff) as u8);
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
                    result.push(value as u8);
                }
                Some(other) => push(&mut result, other),
                None => result.push(b'\\'),
            }
        } else {
            push(&mut result, c);
        }
    }

    copy_text(&result)?;
    Ok(String::from_utf8(result).expect("checked by copy_text"))
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
