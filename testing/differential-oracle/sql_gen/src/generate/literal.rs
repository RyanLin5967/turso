//! Literal value generation.

use crate::ast::Literal;
use crate::context::Context;
use crate::policy::{LiteralConfig, Policy, StringCharset};
use crate::schema::DataType;

/// Generate a literal value for the given data type.
pub fn generate_literal(ctx: &mut Context, data_type: DataType, policy: &Policy) -> Literal {
    generate_literal_with_config(ctx, data_type, &policy.literal_config)
}

/// Generate a literal value for the given data type using the provided config.
pub fn generate_literal_with_config(
    ctx: &mut Context,
    data_type: DataType,
    config: &LiteralConfig,
) -> Literal {
    // Check for NULL generation
    if ctx.gen_bool_with_prob(config.null_probability) {
        return Literal::Null;
    }

    // Boundary values are drawn HERE rather than inside generate_integer/generate_real,
    // because those two guarantee their result lies inside config.int_min..int_max and
    // test_generate_integer_range / test_generate_real_range assert exactly that. The
    // boundary table has to leave that window -- every value worth testing is outside a
    // sensible ordinary-value range -- so it hooks one level up and leaves the contract
    // intact. A config opts out with boundary_value_probability: 0.0.
    if ctx.gen_bool_with_prob(config.boundary_value_probability) {
        match data_type {
            DataType::Integer => {
                let idx = ctx.gen_range(BOUNDARY_INTEGERS.len());
                return Literal::Integer(BOUNDARY_INTEGERS[idx]);
            }
            DataType::Real => {
                let idx = ctx.gen_range(BOUNDARY_REALS.len());
                return Literal::Real(BOUNDARY_REALS[idx]);
            }
            _ => {}
        }
    }

    match data_type {
        DataType::Integer => generate_integer(ctx, config),
        DataType::Real => generate_real(ctx, config),
        DataType::Text => generate_text(ctx, config),
        DataType::Blob => generate_blob(ctx, config),
        DataType::Null => Literal::Null,
        DataType::IntegerArray | DataType::RealArray | DataType::TextArray => {
            generate_array_literal(ctx, data_type, config)
        }
    }
}

/// Integers where numeric behaviour changes: the i64 and i32 edges, the float-exactness edge
/// at 2^53, byte and word boundaries, and the small values that decide truthiness and
/// division. A table rather than a wider range because repeating a small set is what makes
/// two operands in one statement EQUAL -- the condition typeof(min(1, 1.0)) needs, which a
/// uniform draw essentially never produces.
const BOUNDARY_INTEGERS: &[i64] = &[
    0,
    1,
    -1,
    2,
    -2,
    10,
    127,
    128,
    255,
    256,
    32767,
    32768,
    65535,
    65536,
    2147483647,  // i32::MAX
    -2147483648, // i32::MIN
    2147483648,
    4294967295, // u32::MAX
    4294967296,
    9007199254740992, // 2^53, above which f64 cannot hold every integer
    -9007199254740992,
    9223372036854775807,  // i64::MAX
    -9223372036854775808, // i64::MIN, where negation overflows
];

/// Reals chosen for where formatting, affinity and rounding change behaviour, rather than as
/// a uniform sample of the number line.
const BOUNDARY_REALS: &[f64] = &[
    0.0,
    -0.0,
    1.0,
    -1.0,
    0.5,
    0.1,
    2.0,
    1e-300,
    1e300,
    5e-324,                  // smallest subnormal
    2.2250738585072014e-308, // smallest normal
    1.7976931348623157e308,  // f64::MAX
    9007199254740993.0,      // 2^53 + 1, not representable
    9223372036854775807.0,   // i64::MAX as a real
    1e15,
    1e16, // either side of 15 significant digits
];

/// Generate an integer literal.
pub fn generate_integer(ctx: &mut Context, config: &LiteralConfig) -> Literal {
    let value = ctx.gen_i64_range(config.int_min, config.int_max);
    Literal::Integer(value)
}

/// Generate a real literal.
pub fn generate_real(ctx: &mut Context, config: &LiteralConfig) -> Literal {
    let value = ctx.gen_f64_range(config.real_min, config.real_max);
    Literal::Real(value)
}

/// Generate a text literal.
pub fn generate_text(ctx: &mut Context, config: &LiteralConfig) -> Literal {
    let len = ctx.gen_range_inclusive(config.string_min_len, config.string_max_len);
    let text = generate_string_with_charset(ctx, len, config.string_charset);
    Literal::Text(text)
}

/// Generate a string using the specified charset.
fn generate_string_with_charset(ctx: &mut Context, len: usize, charset: StringCharset) -> String {
    let chars: &[u8] = match charset {
        StringCharset::Alphanumeric => {
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        }
        StringCharset::Alpha => b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
        StringCharset::Numeric => b"0123456789",
        StringCharset::AsciiPrintable => {
            b" !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
        }
        StringCharset::Unicode => {
            // For simplicity, use alphanumeric for now
            // TODO: Add actual unicode support
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        }
    };

    (0..len)
        .map(|_| {
            let idx = ctx.gen_range(chars.len());
            chars[idx] as char
        })
        .collect()
}

/// Generate a blob literal.
///
/// Blob bytes are printable ASCII, so every blob is valid UTF-8. SQLite text
/// is a plain byte string, but Turso's text type is a Rust string and cannot
/// hold invalid UTF-8: casting a blob with a byte like 0x96 to TEXT keeps the
/// byte in SQLite and becomes U+FFFD in Turso. This is a documented Turso
/// limitation (see "Limitations" in COMPAT.md), and it reaches TEXT through
/// many doors — CAST, UPPER, TRIM, REPLACE, || — so keeping generated blobs
/// valid UTF-8 is the one place to make both engines see the same characters.
pub fn generate_blob(ctx: &mut Context, config: &LiteralConfig) -> Literal {
    let len = ctx.gen_range_inclusive(config.blob_min_size, config.blob_max_size);
    let bytes = (0..len)
        .map(|_| ctx.gen_range_inclusive(0x20, 0x7E) as u8)
        .collect();
    Literal::Blob(bytes)
}

/// Generate an array literal as a PG-style text string (e.g. '{1, 2, 3}').
pub fn generate_array_literal(
    ctx: &mut Context,
    data_type: DataType,
    config: &LiteralConfig,
) -> Literal {
    let size = ctx.gen_range_inclusive(config.array_min_size, config.array_max_size);
    let element_type = data_type.array_element_type().unwrap_or(DataType::Integer);

    let mut parts = Vec::with_capacity(size);
    for _ in 0..size {
        match element_type {
            DataType::Integer => {
                let v = ctx.gen_i64_range(config.int_min, config.int_max);
                parts.push(v.to_string());
            }
            DataType::Real => {
                let v = ctx.gen_f64_range(config.real_min, config.real_max);
                parts.push(format!("{v}"));
            }
            DataType::Text => {
                let len = ctx.gen_range_inclusive(config.string_min_len, config.string_max_len);
                let s = generate_string_alphanumeric(ctx, len);
                // Quote text elements in PG array format
                parts.push(format!("\"{s}\""));
            }
            _ => {
                let v = ctx.gen_i64_range(config.int_min, config.int_max);
                parts.push(v.to_string());
            }
        }
    }

    Literal::Text(format!("{{{}}}", parts.join(",")))
}

/// Generate a simple alphanumeric string.
fn generate_string_alphanumeric(ctx: &mut Context, len: usize) -> String {
    let chars: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    (0..len)
        .map(|_| {
            let idx = ctx.gen_range(chars.len());
            chars[idx] as char
        })
        .collect()
}

/// Generate a literal suitable for comparison with a column.
pub fn generate_comparable_literal(
    ctx: &mut Context,
    data_type: DataType,
    config: &LiteralConfig,
) -> Literal {
    match data_type {
        DataType::Integer => generate_integer(ctx, config),
        DataType::Real => generate_real(ctx, config),
        DataType::Text => generate_text(ctx, config),
        DataType::Blob => generate_blob(ctx, config),
        DataType::Null => Literal::Null,
        DataType::IntegerArray | DataType::RealArray | DataType::TextArray => {
            generate_array_literal(ctx, data_type, config)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> LiteralConfig {
        LiteralConfig::default()
    }

    #[test]
    fn test_generate_integer() {
        let mut ctx = Context::new_with_seed(42);
        let config = default_config();
        let lit = generate_integer(&mut ctx, &config);
        assert!(matches!(lit, Literal::Integer(_)));
    }

    #[test]
    fn test_generate_integer_range() {
        let mut ctx = Context::new_with_seed(42);
        let config = LiteralConfig {
            int_min: 0,
            int_max: 10,
            ..Default::default()
        };

        for _ in 0..100 {
            if let Literal::Integer(v) = generate_integer(&mut ctx, &config) {
                assert!((0..=10).contains(&v), "value {v} out of range");
            }
        }
    }

    #[test]
    fn test_generate_real() {
        let mut ctx = Context::new_with_seed(42);
        let config = default_config();
        let lit = generate_real(&mut ctx, &config);
        assert!(matches!(lit, Literal::Real(_)));
    }

    #[test]
    fn test_generate_real_range() {
        let mut ctx = Context::new_with_seed(42);
        let config = LiteralConfig {
            real_min: 0.0,
            real_max: 1.0,
            ..Default::default()
        };

        for _ in 0..100 {
            if let Literal::Real(v) = generate_real(&mut ctx, &config) {
                assert!((0.0..=1.0).contains(&v), "value {v} out of range");
            }
        }
    }

    #[test]
    fn test_generate_text() {
        let mut ctx = Context::new_with_seed(42);
        let config = default_config();
        let lit = generate_text(&mut ctx, &config);
        if let Literal::Text(s) = lit {
            assert!(!s.is_empty());
        } else {
            panic!("Expected Text literal");
        }
    }

    #[test]
    fn test_generate_text_length() {
        let mut ctx = Context::new_with_seed(42);
        let config = LiteralConfig {
            string_min_len: 5,
            string_max_len: 10,
            ..Default::default()
        };

        for _ in 0..100 {
            if let Literal::Text(s) = generate_text(&mut ctx, &config) {
                assert!(
                    s.len() >= 5 && s.len() <= 10,
                    "length {} out of range",
                    s.len()
                );
            }
        }
    }

    #[test]
    fn test_generate_blob() {
        let mut ctx = Context::new_with_seed(42);
        let config = default_config();
        let lit = generate_blob(&mut ctx, &config);
        if let Literal::Blob(b) = lit {
            assert!(!b.is_empty());
        } else {
            panic!("Expected Blob literal");
        }
    }

    #[test]
    fn blobs_are_valid_utf8() {
        // Casting a blob with invalid UTF-8 to TEXT keeps the bytes in SQLite
        // but becomes replacement characters in Turso, so generated blobs must
        // stay valid UTF-8 for the two engines to agree.
        let mut ctx = Context::new_with_seed(7);
        let config = default_config();
        for _ in 0..200 {
            if let Literal::Blob(b) = generate_blob(&mut ctx, &config) {
                assert!(
                    std::str::from_utf8(&b).is_ok(),
                    "generated blob is not valid UTF-8: {b:x?}"
                );
            }
        }
    }

    #[test]
    fn test_generate_blob_size() {
        let mut ctx = Context::new_with_seed(42);
        let config = LiteralConfig {
            blob_min_size: 8,
            blob_max_size: 16,
            ..Default::default()
        };

        for _ in 0..100 {
            if let Literal::Blob(b) = generate_blob(&mut ctx, &config) {
                assert!(
                    b.len() >= 8 && b.len() <= 16,
                    "length {} out of range",
                    b.len()
                );
            }
        }
    }

    #[test]
    fn test_null_probability() {
        let mut ctx = Context::new_with_seed(42);
        let policy = Policy::default().with_null_probability(1.0);

        for _ in 0..10 {
            let lit = generate_literal(&mut ctx, DataType::Integer, &policy);
            assert!(matches!(lit, Literal::Null));
        }
    }

    #[test]
    fn test_string_charset_alpha() {
        let mut ctx = Context::new_with_seed(42);
        let config = LiteralConfig {
            string_charset: StringCharset::Alpha,
            string_min_len: 10,
            string_max_len: 10,
            ..Default::default()
        };

        if let Literal::Text(s) = generate_text(&mut ctx, &config) {
            assert!(s.chars().all(|c| c.is_ascii_alphabetic()));
        }
    }

    #[test]
    fn test_string_charset_numeric() {
        let mut ctx = Context::new_with_seed(42);
        let config = LiteralConfig {
            string_charset: StringCharset::Numeric,
            string_min_len: 10,
            string_max_len: 10,
            ..Default::default()
        };

        if let Literal::Text(s) = generate_text(&mut ctx, &config) {
            assert!(s.chars().all(|c| c.is_ascii_digit()));
        }
    }

    /// The table must be REACHABLE through the dispatcher, or this change does nothing. An
    /// earlier version filtered the table through int_min/int_max, which dropped every value
    /// above a million and made the patch a no-op; this assertion is what caught it.
    ///
    /// Note i64::MIN.abs() panics in debug, which this test also learned the hard way --
    /// hence unsigned_abs.
    #[test]
    fn boundary_values_are_reachable_through_the_dispatcher() {
        let mut ctx = Context::new_with_seed(20260822);
        let cfg = LiteralConfig::default();
        let mut beyond_window = false;
        let mut saw_i64_max = false;
        let mut saw_i64_min = false;
        for _ in 0..4000 {
            if let Literal::Integer(v) =
                generate_literal_with_config(&mut ctx, DataType::Integer, &cfg)
            {
                if v.unsigned_abs() > 1_000_000 {
                    beyond_window = true;
                }
                if v == i64::MAX {
                    saw_i64_max = true;
                }
                if v == i64::MIN {
                    saw_i64_min = true;
                }
            }
        }
        assert!(
            beyond_window,
            "no integer outside +/-1e6 in 4000 draws -- the table is unreachable"
        );
        assert!(saw_i64_max && saw_i64_min, "the i64 edges were never drawn");
    }

    /// Reals too, including the ones a uniform +/-1e6 draw can never produce.
    #[test]
    fn boundary_reals_are_reachable() {
        let mut ctx = Context::new_with_seed(4242);
        let cfg = LiteralConfig::default();
        let mut saw_tiny = false;
        let mut saw_huge = false;
        let mut saw_integral = false;
        for _ in 0..4000 {
            if let Literal::Real(v) = generate_literal_with_config(&mut ctx, DataType::Real, &cfg) {
                if v != 0.0 && v.abs() < 1e-100 {
                    saw_tiny = true;
                }
                if v.abs() > 1e100 {
                    saw_huge = true;
                }
                if v == 1.0 || v == 0.0 {
                    saw_integral = true;
                }
            }
        }
        assert!(saw_tiny, "no subnormal/tiny real drawn");
        assert!(saw_huge, "no very large real drawn");
        assert!(saw_integral, "no small integral real drawn");
    }

    /// Opting out must be honoured exactly, so callers needing small values keep relying on
    /// the window.
    #[test]
    fn opting_out_keeps_every_value_inside_the_window() {
        let mut ctx = Context::new_with_seed(99);
        let small = LiteralConfig::small_integers();
        assert_eq!(small.boundary_value_probability, 0.0);
        for _ in 0..4000 {
            if let Literal::Integer(v) =
                generate_literal_with_config(&mut ctx, DataType::Integer, &small)
            {
                assert!(
                    v >= small.int_min && v <= small.int_max,
                    "{v} escaped the configured window"
                );
            }
        }
    }

    /// The reason for a table rather than a wider range: repeated values let two operands in
    /// one statement be equal, which is what typeof(min(1, 1.0)) needs.
    #[test]
    fn the_same_value_recurs_often_enough_to_pair_up() {
        use std::collections::HashMap;
        let mut ctx = Context::new_with_seed(7);
        let cfg = LiteralConfig::default();
        let mut counts: HashMap<i64, usize> = HashMap::new();
        for _ in 0..4000 {
            if let Literal::Integer(v) =
                generate_literal_with_config(&mut ctx, DataType::Integer, &cfg)
            {
                *counts.entry(v).or_default() += 1;
            }
        }
        assert!(
            counts.values().any(|&c| c > 10),
            "no value recurred, so two operands will never be equal"
        );
    }
}
