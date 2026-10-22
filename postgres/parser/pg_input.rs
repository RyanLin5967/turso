//! PostgreSQL 18.6's readers of a constant's text, shared by the translator (a column DEFAULT,
//! stored as written and translated again at each schema load, so its coercion lives here) and the
//! frontend (assignments, casts, int2in/int4in/int8in).

/// What PostgreSQL 18.6's pg_strtoint16/32/64_safe (numutils.c) reads from `input` for an integer
/// of `bits` bits.
pub enum PgInt {
    Value(i64),
    Invalid,
    OutOfRange,
}

/// pg_strtoint16/32/64_safe: blanks around, one sign, then decimal digits or a 0x, 0o or 0b prefix
/// and its digits, each underscore between two digits; a value past the type met while the digits
/// are read is out of range before any later character is judged ('21474836490.5' is 22003 for
/// int4, '2147483649.5' is 22P02). Recorded: rec27-literal-assign (artie 0f7b0647a8).
pub fn pg_strtoint(input: &str, bits: u32) -> PgInt {
    let s = input.as_bytes();
    let blank = |b: u8| matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c);
    let mut i = 0;
    while i < s.len() && blank(s[i]) {
        i += 1;
    }
    let negative = i < s.len() && s[i] == b'-';
    if i < s.len() && (s[i] == b'-' || s[i] == b'+') {
        i += 1;
    }
    let base: u64 = match (s.get(i), s.get(i + 1)) {
        (Some(b'0'), Some(b'x' | b'X')) => 16,
        (Some(b'0'), Some(b'o' | b'O')) => 8,
        (Some(b'0'), Some(b'b' | b'B')) => 2,
        _ => 10,
    };
    if base != 10 {
        i += 2;
    }
    let digit = |b: u8| (b as char).to_digit(base as u32).map(u64::from);
    // -(PG_INTn_MIN / base): a value above it has no room for one more digit.
    let room = (1u64 << (bits - 1)) / base;
    let first = i;
    let mut value: u64 = 0;
    while i < s.len() {
        if let Some(d) = digit(s[i]) {
            if value > room {
                return PgInt::OutOfRange;
            }
            value = value * base + d;
            i += 1;
        } else if s[i] == b'_' {
            // Not first among decimal digits, and followed by a digit.
            if base == 10 && i == first {
                return PgInt::Invalid;
            }
            i += 1;
            if !s.get(i).is_some_and(|b| digit(*b).is_some()) {
                return PgInt::Invalid;
            }
        } else {
            break;
        }
    }
    if i == first {
        return PgInt::Invalid;
    }
    while i < s.len() && blank(s[i]) {
        i += 1;
    }
    if i != s.len() {
        return PgInt::Invalid;
    }
    let limit = (1u64 << (bits - 1)) - u64::from(!negative);
    if value > limit {
        return PgInt::OutOfRange;
    }
    PgInt::Value(if negative { (value as i64).wrapping_neg() } else { value as i64 })
}

/// A decimal constant's value rounded half away from zero, as numeric's int2/int4/int8 casts round
/// it (-0.5 is -1, 2.5e-1 is 0; recorded, rec29-constant-assign, artie c9722422e1); i128::MAX for
/// one past any integer type. None for a spelling that is not plain decimal (a 0x prefix, an
/// underscore), which the caller leaves alone.
pub fn round_decimal(text: &str) -> Option<i128> {
    let t = text.trim();
    let (negative, t) = match t.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let (mantissa, exponent) = match t.find(['e', 'E']) {
        Some(p) => (&t[..p], t[p + 1..].parse::<i64>().ok()?),
        None => (t, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if (whole.is_empty() && fraction.is_empty())
        || !whole.bytes().chain(fraction.bytes()).all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let all = format!("{whole}{fraction}");
    let digits = all.trim_start_matches('0');
    if digits.is_empty() {
        return Some(0);
    }
    // The value is digits x 10^shift.
    let shift = exponent.saturating_sub(fraction.len() as i64);
    let kept = digits.len() as i64 + shift;
    if kept > 30 {
        return Some(i128::MAX);
    }
    let (integer, next) = if shift >= 0 {
        (format!("{digits}{}", "0".repeat(shift as usize)), 0)
    } else if kept > 0 {
        (digits[..kept as usize].to_string(), digits.as_bytes()[kept as usize] - b'0')
    } else if kept == 0 {
        (String::new(), digits.as_bytes()[0] - b'0')
    } else {
        (String::new(), 0)
    };
    let mut value: i128 = if integer.is_empty() { 0 } else { integer.parse().ok()? };
    if next >= 5 {
        value += 1;
    }
    Some(if negative { -value } else { value })
}
