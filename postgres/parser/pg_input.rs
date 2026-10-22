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
/// one past any integer type. A 0x, 0o or 0b integer and underscores between digits are read;
/// None for any other spelling, which the caller leaves alone.
pub fn round_decimal(text: &str) -> Option<i128> {
    let t = text.trim();
    // A 0x, 0o or 0b integer constant (PostgreSQL 16+), underscores between its digits:
    // i128::MAX past every integer type, as for a decimal.
    {
        let (negative, unsigned) = match t.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, t.strip_prefix('+').unwrap_or(t)),
        };
        let radix = match unsigned.get(..2).map(|p| p.to_ascii_lowercase()) {
            Some(p) if p == "0x" => Some(16),
            Some(p) if p == "0o" => Some(8),
            Some(p) if p == "0b" => Some(2),
            _ => None,
        };
        if let Some(radix) = radix {
            let body = unsigned[2..].trim_start_matches('_');
            if body.is_empty() || body.ends_with('_') || body.contains("__") || !body.chars().all(|c| c == '_' || c.is_digit(radix)) {
                return None;
            }
            let digits: String = body.chars().filter(|c| *c != '_').collect();
            let v = match u128::from_str_radix(&digits, radix) {
                Ok(v) if v <= i128::MAX as u128 => v as i128,
                _ => return Some(i128::MAX),
            };
            return Some(if negative { -v } else { v });
        }
    }
    // Underscores between decimal digits (PostgreSQL 16+) are dropped first.
    let owned;
    let t = if t.contains('_') {
        if t.contains("__") || t.starts_with('_') || t.ends_with('_') {
            return None;
        }
        owned = t.replace('_', "");
        owned.as_str()
    } else {
        t
    };
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

/// What float4in/float8in (C's strtod) read from `text`: blanks around; NaN, Infinity and inf in any
/// case and sign; a hexadecimal mantissa with an optional binary exponent (0x1F, 0x1.8p3); a
/// decimal. `Err(true)` for a value past the type (overflow, or a nonzero one that reads as 0),
/// `Err(false)` for a spelling it refuses (recorded, rec42-float-numeric-bool-assign).
pub fn pg_float_in(text: &str, float4: bool) -> Result<f64, bool> {
    let t = text.trim_matches(|c: char| matches!(c, ' ' | '\t' | '\n' | '\r' | '\u{b}' | '\u{c}'));
    let (negative, unsigned) = match t.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let sign = if negative { -1.0 } else { 1.0 };
    if unsigned.eq_ignore_ascii_case("nan") {
        return Ok(f64::NAN);
    }
    if unsigned.eq_ignore_ascii_case("inf") || unsigned.eq_ignore_ascii_case("infinity") {
        return Ok(sign * f64::INFINITY);
    }
    let value = if let Some(hex) = unsigned.strip_prefix("0x").or_else(|| unsigned.strip_prefix("0X")) {
        let (mantissa, exponent) = match hex.find(['p', 'P']) {
            Some(i) => (&hex[..i], hex[i + 1..].parse::<i32>().map_err(|_| false)?),
            None => (hex, 0),
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        if whole.is_empty() && fraction.is_empty() {
            return Err(false);
        }
        let mut v = 0f64;
        for c in whole.chars() {
            v = v * 16.0 + f64::from(c.to_digit(16).ok_or(false)?);
        }
        let mut scale = 1.0 / 16.0;
        for c in fraction.chars() {
            v += f64::from(c.to_digit(16).ok_or(false)?) * scale;
            scale /= 16.0;
        }
        v * 2f64.powi(exponent)
    } else {
        if !unsigned.bytes().all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'E' | b'+' | b'-')) || !unsigned.bytes().any(|b| b.is_ascii_digit()) {
            return Err(false);
        }
        unsigned.parse::<f64>().map_err(|_| false)?
    };
    // A nonzero mantissa that reads as 0 underflowed: strtod's ERANGE, PostgreSQL's 22003.
    let hex_digits = unsigned.get(..2).is_some_and(|p| p.eq_ignore_ascii_case("0x"));
    let mantissa_digits: &str = if hex_digits {
        unsigned[2..].split(['p', 'P']).next().unwrap_or("")
    } else {
        unsigned.split(['e', 'E']).next().unwrap_or("")
    };
    let nonzero = mantissa_digits.bytes().any(|b| if hex_digits { b.is_ascii_hexdigit() && b != b'0' } else { matches!(b, b'1'..=b'9') });
    let stored = if float4 { f64::from(value as f32) } else { value };
    if stored.is_infinite() || (stored == 0.0 && nonzero) {
        return Err(true);
    }
    Ok(sign * stored)
}

/// numeric_out's text of a decimal constant's spelling (1e39 is 1 and 39 zeros, 1e-50 is 0.0...01,
/// 1.50 keeps its scale), for the messages PostgreSQL words with it; None for one it is not.
pub fn numeric_out(text: &str) -> Option<String> {
    let t = text.trim();
    let (negative, unsigned) = match t.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(i) => (&unsigned[..i], unsigned[i + 1..].parse::<i64>().ok()?),
        None => (unsigned, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if (whole.is_empty() && fraction.is_empty()) || !whole.bytes().chain(fraction.bytes()).all(|b| b.is_ascii_digit()) {
        return None;
    }
    if exponent.abs() > 100_000 {
        return None;
    }
    let digits = format!("{whole}{fraction}");
    // The point sits `point` digits into `digits`.
    let point = whole.len() as i64 + exponent;
    let scale = (fraction.len() as i64 - exponent).max(0);
    let (int_part, frac_part) = if point <= 0 {
        (String::from("0"), format!("{}{}", "0".repeat((-point) as usize), digits))
    } else if point as usize >= digits.len() {
        (format!("{digits}{}", "0".repeat(point as usize - digits.len())), String::new())
    } else {
        (digits[..point as usize].to_string(), digits[point as usize..].to_string())
    };
    let int_part = int_part.trim_start_matches('0');
    let int_part = if int_part.is_empty() { "0" } else { int_part };
    let mut frac = frac_part;
    frac.truncate(scale as usize);
    while (frac.len() as i64) < scale {
        frac.push('0');
    }
    let body = if frac.is_empty() { int_part.to_string() } else { format!("{int_part}.{frac}") };
    let zero = body.bytes().all(|b| b == b'0' || b == b'.');
    Some(if negative && !zero { format!("-{body}") } else { body })
}

/// What boolin reads from `text`: blanks around, then a unique prefix of true, false, yes or no,
/// on or off (two letters at least), or 1 or 0, in any case; None for one it refuses.
pub fn pg_boolin(text: &str) -> Option<bool> {
    let t = text.trim_matches(|c: char| c.is_ascii_whitespace()).to_ascii_lowercase();
    let prefix = |word: &str| !t.is_empty() && word.starts_with(t.as_str());
    match t.as_bytes().first()? {
        b't' if prefix("true") => Some(true),
        b'y' if prefix("yes") => Some(true),
        b'f' if prefix("false") => Some(false),
        b'n' if prefix("no") => Some(false),
        b'o' if t.len() >= 2 && prefix("on") => Some(true),
        b'o' if t.len() >= 2 && prefix("off") => Some(false),
        b'1' if t == "1" => Some(true),
        b'0' if t == "0" => Some(false),
        _ => None,
    }
}

/// What numeric_in reads from `text`, as the plain decimal text the engine's numeric takes: blanks
/// around, NaN or Infinity (inf, any case or sign), a 0x, 0o or 0b integer, underscores between
/// digits, a decimal with an exponent; None for a spelling it refuses (rec42).
pub fn pg_numeric_in(text: &str) -> Option<String> {
    let t = text.trim_matches(|c: char| c.is_ascii_whitespace());
    let (sign, unsigned) = match t.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", t.strip_prefix('+').unwrap_or(t)),
    };
    if unsigned.eq_ignore_ascii_case("nan") {
        return (sign.is_empty()).then(|| "NaN".to_string());
    }
    if unsigned.eq_ignore_ascii_case("inf") || unsigned.eq_ignore_ascii_case("infinity") {
        return Some(format!("{sign}Infinity"));
    }
    let radix = match unsigned.get(..2).map(|p| p.to_ascii_lowercase()) {
        Some(p) if p == "0x" => Some(16),
        Some(p) if p == "0o" => Some(8),
        Some(p) if p == "0b" => Some(2),
        _ => None,
    };
    let underscores_ok = |s: &str, digit: &dyn Fn(char) -> bool| {
        let parts: Vec<&str> = s.split('_').collect();
        parts.iter().all(|p| !p.is_empty() && p.chars().all(digit))
    };
    if let Some(radix) = radix {
        let body = &unsigned[2..];
        if !underscores_ok(body, &|c: char| c.is_digit(radix)) {
            return None;
        }
        let v = u128::from_str_radix(&body.replace('_', ""), radix).ok()?;
        return Some(format!("{sign}{v}"));
    }
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(i) => (&unsigned[..i], Some(&unsigned[i + 1..])),
        None => (unsigned, None),
    };
    let (whole, fraction) = match mantissa.split_once('.') {
        Some((w, f)) => (w, Some(f)),
        None => (mantissa, None),
    };
    let digit = |c: char| c.is_ascii_digit();
    let whole_ok = whole.is_empty() || underscores_ok(whole, &digit);
    let fraction_ok = fraction.is_none_or(|f| f.is_empty() || underscores_ok(f, &digit));
    if !whole_ok || !fraction_ok || (whole.is_empty() && fraction.is_none_or(|f| f.is_empty())) {
        return None;
    }
    let exponent_text = match exponent {
        Some(e) => {
            let (es, ed) = match e.strip_prefix('-') {
                Some(r) => ("-", r),
                None => ("", e.strip_prefix('+').unwrap_or(e)),
            };
            if ed.is_empty() || !underscores_ok(ed, &digit) {
                return None;
            }
            format!("e{es}{}", ed.replace('_', ""))
        }
        None => String::new(),
    };
    let fraction_text = fraction.map(|f| format!(".{}", f.replace('_', ""))).unwrap_or_default();
    Some(format!("{sign}{}{fraction_text}{exponent_text}", whole.replace('_', "")))
}
