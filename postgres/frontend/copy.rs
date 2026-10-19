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
pub type CopyRow = Vec<Option<String>>;

/// [`parse_copy_rows`] for `num_columns` columns named column1, column2, ...
#[cfg(test)]
pub fn parse_copy_text_format(
    data: &str,
    delimiter: char,
    null_string: &str,
    num_columns: usize,
) -> Result<Vec<CopyRow>> {
    let columns: Vec<String> = (1..=num_columns).map(|i| format!("column{i}")).collect();
    parse_copy_rows(data, delimiter, null_string, &columns, false)
}

/// Every row of `data` at once, through the one line reader ([`CopyLines`]) and row reader
/// ([`copy_row`]) COPY streams with: for this module's tests.
#[cfg(test)]
pub fn parse_copy_rows(
    data: &str,
    delimiter: char,
    null_string: &str,
    columns: &[String],
    header: bool,
) -> Result<Vec<CopyRow>> {
    let mut lines = CopyLines::default();
    let (ranges, _) = lines.split(data.as_bytes(), true)?;
    ranges
        .into_iter()
        .skip(usize::from(header))
        .map(|r| copy_row(&data.as_bytes()[r], delimiter, null_string, columns))
        .collect()
}

/// One line's row for `columns`, the columns COPY loads, in order: the line must be UTF-8 with no
/// NUL (22021), and hold one field per column, an empty line being one empty field (22P04 as
/// PostgreSQL's CopyReadAttributesText answers: "missing data for column \"v\"", "extra data after
/// last expected column"). A field equal to the NULL string as written is NULL; any other is
/// unescaped ([`unescape_copy_field`]).
pub fn copy_row(
    line: &[u8],
    delimiter: char,
    null_string: &str,
    columns: &[String],
) -> Result<CopyRow> {
    let line = copy_text(line)?;
    let fields = split_copy_fields(line, delimiter);
    if fields.len() > columns.len() {
        return Err(bad_format("extra data after last expected column".to_string()));
    }
    if let Some(missing) = columns.get(fields.len()) {
        return Err(bad_format(format!("missing data for column \"{missing}\"")));
    }
    fields
        .iter()
        .map(|field| {
            if *field == null_string {
                Ok(None)
            } else {
                unescape_copy_field(field).map(Some)
            }
        })
        .collect()
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
        // As report_invalid_encoding names it: as many bytes as the lead byte says the
        // character has, as far as there are any (`\303(` is 0xc3 0x28; recorded `partial`).
        let bad = &bytes[e.valid_up_to()..];
        let len = match bad[0] {
            0xc0..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf7 => 4,
            _ => 1,
        };
        invalid_utf8(&bad[..len.min(bad.len())])
    })?;
    if text.contains('\0') {
        return Err(invalid_utf8(&[0]));
    }
    Ok(text)
}


/// A line end: LF, CR or CRLF.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Eol {
    Nl,
    Cr,
    CrNl,
}

/// COPY's line reader, as PostgreSQL 18.6's CopyReadLineText reads lines, over data that arrives
/// in pieces ([`CopyLines::split`]). The first line's end fixes the style (LF, CR or CRLF), and any
/// other end is 22P04 "literal carriage return found in data" / "literal newline found in data";
/// a backslash makes the byte after it data, so `a\<LF>b` is one line holding a newline (and an
/// escaped CR is data); `\.` alone on its line ends the data, its own end in the data's style
/// ("end-of-copy marker does not match previous newline style"), and `\.` anywhere else is
/// "end-of-copy marker is not alone on its line" (recorded, cli/tests/copy-stdin-pg18.txt
/// `bsnewline`, `dotnotalone`, `escapedcr`, parts 6 `S3` .. `dotcrnl`). The lines were cut at
/// each LF with a CR before it dropped, so a bare CR was data, mixed ends loaded and CR-only data
/// was one line (review wire-rev-copystdin M2).
#[derive(Clone, Debug, Default)]
pub struct CopyLines {
    eol: Option<Eol>,
    /// `\.` was read: the rest of the data is not read.
    ended: bool,
}

impl CopyLines {
    /// The complete lines at the front of `buf` (byte ranges, their ends cut off) and how many
    /// bytes they take; the rest is an unfinished line, for the caller to hold until more data
    /// comes. With `last` (the data ends at `buf`'s end) the rest is the last line.
    pub fn split(
        &mut self,
        buf: &[u8],
        last: bool,
    ) -> Result<(Vec<std::ops::Range<usize>>, usize)> {
        let mut lines = Vec::new();
        if self.ended {
            return Ok((lines, buf.len()));
        }
        let literal = |what: &str| Err(bad_format(format!("literal {what} found in data")));
        let (mut start, mut i) = (0, 0);
        while i < buf.len() {
            match buf[i] {
                b'\\' if buf.get(i + 1) == Some(&b'.') => {
                    if i != start {
                        return Err(bad_format(
                            "end-of-copy marker is not alone on its line".to_string(),
                        ));
                    }
                    let after = i + 2;
                    let end = match (buf.get(after), buf.get(after + 1)) {
                        (None, _) if !last => return Ok((lines, start)),
                        (None, _) => None,
                        (Some(b'\n'), _) => Some(Eol::Nl),
                        (Some(b'\r'), None) if !last => return Ok((lines, start)),
                        (Some(b'\r'), Some(b'\n')) => Some(Eol::CrNl),
                        (Some(b'\r'), _) => Some(Eol::Cr),
                        _ => {
                            return Err(bad_format(
                                "end-of-copy marker is not alone on its line".to_string(),
                            ))
                        }
                    };
                    if let (Some(style), Some(end)) = (self.eol, end) {
                        if style != end {
                            return Err(bad_format(
                                "end-of-copy marker does not match previous newline style"
                                    .to_string(),
                            ));
                        }
                    }
                    self.ended = true;
                    return Ok((lines, buf.len()));
                }
                // The escaped byte is data: ASCII (a newline, a CR, the delimiter) or the lead
                // byte of a character, whose continuation bytes are never special.
                b'\\' => {
                    if i + 1 == buf.len() && !last {
                        return Ok((lines, start));
                    }
                    i += 2;
                }
                b'\r' => {
                    let next = buf.get(i + 1).copied();
                    if next.is_none() && !last && self.eol != Some(Eol::Cr) {
                        // CR or CRLF: the next byte decides.
                        return Ok((lines, start));
                    }
                    let style = match self.eol {
                        Some(style) => style,
                        None if next == Some(b'\n') => Eol::CrNl,
                        None => Eol::Cr,
                    };
                    self.eol = Some(style);
                    match style {
                        Eol::Nl => return literal("carriage return"),
                        Eol::Cr => {
                            lines.push(start..i);
                            i += 1;
                        }
                        Eol::CrNl if next == Some(b'\n') => {
                            lines.push(start..i);
                            i += 2;
                        }
                        Eol::CrNl => return literal("carriage return"),
                    }
                    start = i;
                }
                b'\n' => {
                    match self.eol {
                        None | Some(Eol::Nl) => self.eol = Some(Eol::Nl),
                        Some(_) => return literal("newline"),
                    }
                    lines.push(start..i);
                    i += 1;
                    start = i;
                }
                _ => i += 1,
            }
        }
        if last && start < buf.len() {
            lines.push(start..buf.len());
            start = buf.len();
        }
        Ok((lines, start))
    }
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
