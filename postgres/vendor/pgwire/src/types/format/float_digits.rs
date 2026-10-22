use crate::error::PgWireError;

/// extra_float_digits sets the number of digits displayed for floating-point
/// values.
///
/// Valid values [-15, 3], default value: 1
#[derive(Debug)]
pub struct ExtraFloatDigits(pub i8);

impl TryFrom<&str> for ExtraFloatDigits {
    type Error = PgWireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let value = value
            .parse::<i8>()
            .map_err(|_| PgWireError::InvalidOptionValue(value.to_string()))?;

        if !(-15..=3).contains(&value) {
            Err(PgWireError::InvalidOptionValue(value.to_string()))
        } else {
            Ok(Self(value))
        }
    }
}

impl ExtraFloatDigits {
    const FLT_DIG: i8 = 6;
    const DBL_DIG: i8 = 15;

    pub fn format_f64(&self, value: f64) -> String {
        let extra = self.0.clamp(-15, 3);

        // Handle special cases
        if value.is_nan() {
            return "NaN".to_string();
        }
        // PostgreSQL's float8out / float4out spellings, and a negative zero keeps its sign.
        if value.is_infinite() {
            return if value.is_sign_positive() {
                "Infinity".to_string()
            } else {
                "-Infinity".to_string()
            };
        }

        if value == 0.0 {
            return if value.is_sign_negative() { "-0" } else { "0" }.to_string();
        }

        // For extra_float_digits >= 1, the shortest exact digits in PostgreSQL's layout
        // This preserves the original binary float value exactly
        // At most 17 digits for f64
        if extra >= 1 {
            return postgres_shortest(&strictly_shortest_f64(value), Self::DBL_DIG as i32);
        }

        // For negative values, round to DBL_DIG + extra_float_digits significant digits
        // DBL_DIG = 15 for f64
        let sig_digits = (Self::DBL_DIG + extra).max(0) as usize;
        format_with_significant_digits(value, sig_digits)
    }

    pub fn format_f32(&self, value: f32) -> String {
        let extra = self.0.clamp(-15, 3);

        // Handle special cases
        if value.is_nan() {
            return "NaN".to_string();
        }
        // PostgreSQL's float8out / float4out spellings, and a negative zero keeps its sign.
        if value.is_infinite() {
            return if value.is_sign_positive() {
                "Infinity".to_string()
            } else {
                "-Infinity".to_string()
            };
        }

        if value == 0.0 {
            return if value.is_sign_negative() { "-0" } else { "0" }.to_string();
        }

        // For extra_float_digits >= 1, the shortest exact digits in PostgreSQL's layout
        // This preserves the original binary float value exactly
        // At most 9 digits for f32
        if extra >= 1 {
            return postgres_shortest(&strictly_shortest_f32(value), Self::FLT_DIG as i32);
        }

        // For negative values, round to FLT_DIG + extra_float_digits significant digits
        // FLT_DIG = 6 for f32
        let sig_digits = (Self::FLT_DIG + extra).max(0) as usize;
        format_with_significant_digits(value as f64, sig_digits)
    }
}

/// float8out's digits (src/common/d2s.c, built without STRICTLY_SHORTEST, so acceptBounds is
/// false): the shortest that read back as `value` from strictly inside its rounding interval, as
/// [`strictly_shortest_f32`] for float4 (20197275871653528 is 2.0197275871653528e+16, not Rust's
/// 2.019727587165353e+16; turso fastest-wire, PG 18.6 recorded, rec36-float8-ties). The edge test is
/// exact ([`on_f64_edge`]).
fn strictly_shortest_f64(value: f64) -> String {
    let shortest = format!("{value:e}");
    if !on_f64_edge(value, &shortest) {
        return shortest;
    }
    for digits in 2..=17 {
        let candidate = format!("{value:.*e}", digits - 1);
        if candidate.parse::<f64>() == Ok(value) && !on_f64_edge(value, &candidate) {
            return candidate;
        }
    }
    shortest
}

/// Whether the decimal `candidate` (Rust's `{:e}` text) is exactly halfway between `value` and a
/// neighbouring double. With the candidate as D x 10^e and the halfway point as M x 2^k (M odd),
/// they are equal exactly when D's powers of two make up k and D's odd part times 5^e is M (each
/// side's power of five moved across), all within u128: M has at most 55 bits and D at most 17
/// digits, so a power of five past 5^27 already makes the sides differ.
fn on_f64_edge(value: f64, candidate: &str) -> bool {
    let Some((d, e)) = decimal_parts(candidate) else { return false };
    if d == 0 {
        return false;
    }
    let bits = value.abs().to_bits();
    let parts = |b: u64| -> (u128, i64) {
        let exp = ((b >> 52) & 0x7ff) as i64;
        let frac = u128::from(b & ((1u64 << 52) - 1));
        if exp == 0 { (frac, -1074) } else { (frac | (1u128 << 52), exp - 1075) }
    };
    let (dv2, dodd) = (d.trailing_zeros() as i64, d >> d.trailing_zeros());
    let halfway = |n: u64| -> bool {
        if (n >> 52) & 0x7ff == 0x7ff {
            return false;
        }
        let ((a, ea), (b, eb)) = (parts(bits), parts(n));
        let e0 = ea.min(eb);
        let sum = (a << (ea - e0)) + (b << (eb - e0));
        let tz = sum.trailing_zeros() as i64;
        let (m, k) = (sum >> tz, e0 - 1 + tz);
        if dv2 + e != k {
            return false;
        }
        let five = |p: i64| (0..p).try_fold(1u128, |acc, _| acc.checked_mul(5));
        match (dodd.checked_mul(five(e.max(0)).unwrap_or(0)), m.checked_mul(five((-e).max(0)).unwrap_or(0))) {
            (Some(l), Some(r)) => l != 0 && l == r,
            _ => false,
        }
    };
    halfway(bits + 1) || (bits > 0 && halfway(bits - 1))
}

/// A decimal text `[-]d.ddde[-]N` (or without the point or exponent) as its digits D and the power
/// of ten e with value D x 10^e; None past u128.
fn decimal_parts(text: &str) -> Option<(u128, i64)> {
    let t = text.trim_start_matches('-');
    let (mantissa, exp) = match t.split_once(['e', 'E']) {
        Some((m, x)) => (m, x.parse::<i64>().ok()?),
        None => (t, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    let mut d: u128 = 0;
    for b in whole.bytes().chain(fraction.bytes()) {
        if !b.is_ascii_digit() {
            return None;
        }
        d = d.checked_mul(10)?.checked_add(u128::from(b - b'0'))?;
    }
    Some((d, exp - fraction.len() as i64))
}

/// float4out's digits (src/common/f2s.c, built without STRICTLY_SHORTEST, so acceptBounds is
/// false): the shortest that read back as `value` from strictly inside its rounding interval. Rust's
/// `{:e}` may give a decimal on the interval's edge, which reads back only by ties-to-even:
/// -337198016 was -3.37198e+08, PostgreSQL's -3.3719802e+08 (turso fastest-wire, numfuzz seeds 343
/// and 1144; PG 18.6 recorded, rec30-float4-ties). The edges, halfway to each neighbour, are exact
/// in an f64, and a candidate of at most 9 digits equal to one parses to it exactly. float8's
/// halfway points need 54 bits, so [`on_f64_edge`] tests them exactly instead.
fn strictly_shortest_f32(value: f32) -> String {
    let shortest = format!("{value:e}");
    let x = f64::from(value);
    let bits = value.to_bits();
    let up = f64::from(f32::from_bits(bits.wrapping_add(1)));
    let down = f64::from(f32::from_bits(bits.wrapping_sub(1)));
    let on_edge = |s: &str| s.parse::<f64>().is_ok_and(|v| v == (x + up) / 2.0 || v == (x + down) / 2.0);
    if !on_edge(&shortest) {
        return shortest;
    }
    for digits in 2..=9 {
        let candidate = format!("{value:.*e}", digits - 1);
        if candidate.parse::<f32>() == Ok(value) && !on_edge(&candidate) {
            return candidate;
        }
    }
    shortest
}

/// PostgreSQL's shortest-exact float text (float8out / float4out at extra_float_digits >= 1,
/// float.c's double_to_shortest_decimal_buf): the shortest digits that read back exactly, written
/// in fixed notation while the decimal exponent is in [-4, `max_exp`) (DBL_DIG 15 for float8,
/// FLT_DIG 6 for float4), else as `d.ddde+XX` with a sign and at least two exponent digits.
/// `shortest` is Rust's `{:e}` of the value, which is the shortest exact form (`1.5e-5`). ryu's
/// text, used before, was `1.0`, `100.0`, `1e16` and `0.00001` where PostgreSQL writes `1`, `100`,
/// `1e+16` and `1e-05` (turso fastest-wire, PG 18.6 recorded 2026-10-15).
fn postgres_shortest(shortest: &str, max_exp: i32) -> String {
    let (mantissa, exp) = match shortest.split_once('e') {
        Some((m, e)) => (m, e.parse::<i32>().unwrap_or(0)),
        None => (shortest, 0),
    };
    let (sign, mantissa) = match mantissa.strip_prefix('-') {
        Some(m) => ("-", m),
        None => ("", mantissa),
    };
    if exp < -4 || exp >= max_exp {
        let e_sign = if exp < 0 { '-' } else { '+' };
        return format!("{sign}{mantissa}e{e_sign}{:02}", exp.abs());
    }
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    if exp >= 0 {
        let point = exp as usize + 1;
        if digits.len() <= point {
            format!("{sign}{digits}{}", "0".repeat(point - digits.len()))
        } else {
            format!("{sign}{}.{}", &digits[..point], &digits[point..])
        }
    } else {
        format!("{sign}0.{}{digits}", "0".repeat((-exp - 1) as usize))
    }
}

/// Format with specific number of significant digits
fn format_with_significant_digits(value: f64, sig_digits: usize) -> String {
    if sig_digits == 0 {
        // Round to nearest integer
        return format!("{:.0}", value.round());
    }

    let negative = value < 0.0;
    let abs_value = value.abs();

    // Find the order of magnitude
    let magnitude = abs_value.log10().floor();

    // Calculate decimal places needed
    // For a number like 3.14159, magnitude is 0
    // For 314.159, magnitude is 2
    // For 0.0314159, magnitude is -2
    let decimal_places = if magnitude >= 0.0 {
        // For numbers >= 1, decimal places = sig_digits - (integer_digits)
        let integer_digits = (magnitude as i32) + 1;
        (sig_digits as i32 - integer_digits).max(0) as usize
    } else {
        // For numbers < 1, we need to account for leading zeros after decimal
        let leading_zeros = (-magnitude as i32) - 1;
        sig_digits + leading_zeros as usize
    };

    // Format with calculated precision
    let formatted = format!("{abs_value:.decimal_places$}");

    // Remove trailing zeros and unnecessary decimal point
    let trimmed = trim_trailing_zeros(&formatted);

    if negative {
        format!("-{trimmed}",)
    } else {
        trimmed
    }
}

/// Remove trailing zeros from decimal representation
fn trim_trailing_zeros(s: &str) -> String {
    if !s.contains('.') {
        return s.to_string();
    }

    let trimmed = s.trim_end_matches('0').trim_end_matches('.');
    trimmed.to_string()
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format() {
        let v = std::f64::consts::PI; //3.1415926535897931

        let formatted = &[
            "3",                 // -15
            "3",                 // -14
            "3.1",               // -13
            "3.14",              // -12
            "3.142",             // -11
            "3.1416",            // -10
            "3.14159",           // -9
            "3.141593",          // -8
            "3.1415927",         // -7
            "3.14159265",        // -6
            "3.141592654",       // -5
            "3.1415926536",      // -4
            "3.14159265359",     // -3
            "3.14159265359",     // -2
            "3.1415926535898",   // -1
            "3.14159265358979",  // 0
            "3.141592653589793", // 1
            "3.141592653589793", // 2
            "3.141592653589793", // 3
        ];

        for (i, e) in (-15..=3).enumerate() {
            let extra_float_digit = ExtraFloatDigits(e);
            assert_eq!(extra_float_digit.format_f64(v), formatted[i]);
        }

        assert_eq!(ExtraFloatDigits(0).format_f64(f64::NAN), "NaN");
        // FLAGGED TEST EDIT (fastest-wire, lead ruling 2026-10-15T07:40:59Z; VENDORED.md item 6):
        // upstream asserted "inf" / "-inf"; PostgreSQL 18.6 writes "Infinity" / "-Infinity".
        assert_eq!(ExtraFloatDigits(0).format_f64(f64::INFINITY), "Infinity");
        assert_eq!(ExtraFloatDigits(0).format_f64(-f64::INFINITY), "-Infinity");

        let v = std::f32::consts::PI; //3.1415926535897931

        let formatted = &[
            "3",         // -15
            "3",         // -14
            "3",         // -13
            "3",         // -12
            "3",         // -11
            "3",         // -10
            "3",         // -9
            "3",         // -8
            "3",         // -7
            "3",         // -6
            "3",         // -5
            "3.1",       // -4
            "3.14",      // -3
            "3.142",     // -2
            "3.1416",    // -1
            "3.14159",   // 0
            "3.1415927", // 1
            "3.1415927", // 2
            "3.1415927", // 3
        ];

        for (i, e) in (-15..=3).enumerate() {
            let extra_float_digit = ExtraFloatDigits(e);
            assert_eq!(extra_float_digit.format_f32(v), formatted[i]);
        }
    }
}
