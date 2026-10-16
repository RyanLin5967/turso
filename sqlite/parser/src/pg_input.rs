/// One byte of PostgreSQL's whitespace (its lexer's `space`): space, tab, newline, carriage return,
/// form feed and vertical tab (which `is_ascii_whitespace` omits), and no byte of a multi-byte
/// character, which PostgreSQL lexes as an identifier byte (wire review 13 item 1). The one
/// definition the server's verb reader, empty-statement test and CHECKPOINT test and the
/// branch-call fast path read (wire review 14 item 6).
pub fn pg_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b)
}

/// PostgreSQL's signed integer input grammar, bounded to the destination type.
/// `None` means invalid syntax; `Some(None)` means out of range. The early
/// multiplication check intentionally precedes later syntax checks, as in
/// PostgreSQL's pg_strtoint16/32/64_safe: `32768x` is syntax for int2, while
/// `327680x` is range. Checking only the final i64 value loses that distinction.
pub fn pg_integer(text: &str, min: i64, max: i64) -> Option<Option<i64>> {
    debug_assert!(min < 0 && max >= 0 && min.unsigned_abs() == max as u64 + 1);
    let s = text.trim_matches(|c: char| c.is_ascii() && pg_space(c as u8));
    let (negative, s) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let (radix, digits) = match s.get(..2).map(|p| p.to_ascii_lowercase()).as_deref() {
        Some("0x") => (16, &s[2..]),
        Some("0o") => (8, &s[2..]),
        Some("0b") => (2, &s[2..]),
        _ => (10, s),
    };
    // An underscore may follow a base prefix ('0b_10_0101' is 37), never begin a decimal
    // (wire review 16 item 23: it was refused in every base).
    if digits.is_empty() || (radix == 10 && digits.starts_with('_')) {
        return None;
    }
    let mut magnitude = 0_u64;
    let cutoff = min.unsigned_abs() / u64::from(radix);
    let mut chars = digits.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '_' {
            chars.peek().and_then(|c| c.to_digit(radix))?;
            continue;
        }
        let d = c.to_digit(radix)?;
        if magnitude > cutoff {
            return Some(None);
        }
        magnitude = magnitude * u64::from(radix) + u64::from(d);
    }
    let limit = if negative {
        min.unsigned_abs()
    } else {
        max as u64
    };
    if magnitude > limit {
        return Some(None);
    }
    let value = if negative {
        -(magnitude as i128) as i64
    } else {
        magnitude as i64
    };
    Some(Some(value))
}
