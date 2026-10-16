use crate::error::{SqlErrorKind, SqlErrorMessage};
use bigdecimal::BigDecimal;
use num_bigint::{BigInt, Sign};

use crate::alloc::TursoVecExt;
use crate::{LimboError, ValueBlob};

const NUMERIC_BLOB_VERSION: u8 = 0x01;
const FLAG_NEGATIVE: u8 = 0x01;
const MAX_SCALE_MAGNITUDE: i64 = 1_000_000;

/// Serialize a BigDecimal to our portable blob format.
///
/// Format (all multi-byte values little-endian):
/// ```text
/// [version: 1 byte (0x01)]
/// [flags:   1 byte (bit 0 = sign, 0=positive/zero, 1=negative)]
/// [scale:   8 bytes, i64 LE]
/// [num_limbs: 4 bytes, u32 LE]
/// [limb0: 4 bytes u32 LE] [limb1: 4 bytes u32 LE] ... [limbN: 4 bytes u32 LE]
/// ```
fn bigdecimal_to_blob(val: &BigDecimal) -> ValueBlob {
    let (bigint, scale) = val.as_bigint_and_exponent();
    let (sign, magnitude) = bigint.into_parts();

    // magnitude.to_u32_digits() returns limbs in little-endian order
    let limbs = magnitude.to_u32_digits();
    debug_assert!(
        limbs.len() <= u32::MAX as usize,
        "limb count {} exceeds u32::MAX",
        limbs.len()
    );
    let num_limbs = limbs.len() as u32;

    let mut buf = <ValueBlob as TursoVecExt<u8>>::with_capacity(14 + limbs.len() * 4);

    // version
    buf.push(NUMERIC_BLOB_VERSION);

    // flags
    let flags = if sign == Sign::Minus {
        FLAG_NEGATIVE
    } else {
        0
    };
    buf.push(flags);

    // scale (i64 LE)
    buf.extend_from_slice(&scale.to_le_bytes());

    // num_limbs (u32 LE)
    buf.extend_from_slice(&num_limbs.to_le_bytes());

    // limbs (u32 LE each)
    for limb in &limbs {
        buf.extend_from_slice(&limb.to_le_bytes());
    }

    buf
}

/// Deserialize a BigDecimal from our portable blob format: the decoder's raw step, bounded only by the
/// blob's own sanity limits. Outside its tests, only `PgNumeric::from_blob` calls it, which bounds
/// the value it returns.
pub(crate) fn blob_to_bigdecimal(blob: &[u8]) -> crate::Result<BigDecimal> {
    // Minimum size: version(1) + flags(1) + scale(8) + num_limbs(4) = 14
    if blob.len() < 14 {
        return Err(LimboError::Constraint(
            SqlErrorMessage::classified(SqlErrorKind::InvalidTextRepresentation, "invalid numeric blob: too short".to_string()),
        ));
    }

    let version = blob[0];
    if version != NUMERIC_BLOB_VERSION {
        return Err(LimboError::Constraint(format!(
            "unsupported numeric blob version: {version}"
        ).into()));
    }

    let flags = blob[1];
    let sign = if flags & FLAG_NEGATIVE != 0 {
        Sign::Minus
    } else {
        Sign::Plus
    };

    // SAFETY: blob[2..10] is exactly 8 bytes (checked by min-length guard above)
    let scale = i64::from_le_bytes(blob[2..10].try_into().unwrap());

    // Reject absurd scales from corrupted blobs.
    if !(-MAX_SCALE_MAGNITUDE..=MAX_SCALE_MAGNITUDE).contains(&scale) {
        return Err(LimboError::Constraint(SqlErrorMessage::classified(SqlErrorKind::InvalidTextRepresentation, format!(
            "invalid numeric blob: scale {scale} out of range"
        ))));
    }

    // SAFETY: blob[10..14] is exactly 4 bytes (checked by min-length guard above)
    let num_limbs = u32::from_le_bytes(blob[10..14].try_into().unwrap()) as usize;

    // Cap limb count to prevent OOM from crafted blobs. 65536 limbs = 256KB,
    // which supports numbers with ~600,000 decimal digits — far beyond any
    // reasonable precision.
    const MAX_LIMBS: usize = 65536;
    if num_limbs > MAX_LIMBS {
        return Err(LimboError::Constraint(SqlErrorMessage::classified(SqlErrorKind::InvalidTextRepresentation, format!(
            "invalid numeric blob: limb count {num_limbs} exceeds maximum {MAX_LIMBS}"
        ))));
    }

    let expected_len = num_limbs
        .checked_mul(4)
        .and_then(|n| n.checked_add(14))
        .ok_or_else(|| {
            LimboError::Constraint(SqlErrorMessage::classified(
                SqlErrorKind::InvalidTextRepresentation,
                "invalid numeric blob: limb count overflow".to_string(),
            ))
        })?;
    if blob.len() != expected_len {
        return Err(LimboError::Constraint(SqlErrorMessage::classified(SqlErrorKind::InvalidTextRepresentation, format!(
            "invalid numeric blob: expected {expected_len} bytes, got {}",
            blob.len()
        ))));
    }

    let mut limbs = Vec::with_capacity(num_limbs);
    for i in 0..num_limbs {
        let offset = 14 + i * 4;
        let limb = u32::from_le_bytes(blob[offset..offset + 4].try_into().unwrap());
        limbs.push(limb);
    }

    // Handle zero case: BigInt::new with empty limbs and Plus sign = 0
    let sign = if limbs.is_empty() { Sign::NoSign } else { sign };
    let bigint = BigInt::new(sign, limbs);
    Ok(BigDecimal::new(bigint, scale))
}

/// PostgreSQL's division-scale constants (numeric.c): the significant digits a quotient keeps at
/// least, the largest display scale, and the decimal digits in one base-10000 digit.
const PG_MIN_SIG_DIGITS: i64 = 16;
const PG_MAX_DISPLAY_SCALE: i64 = 1000;
const PG_DEC_DIGITS: i64 = 4;

/// PostgreSQL's numeric range (its documented limits: up to 131072 digits before the decimal
/// point and up to 16383 after it).
const PG_MAX_INTEGER_DIGITS: i128 = 131_072;
const PG_MAX_FRACTION_DIGITS: i128 = 16_383;

/// `v` within PostgreSQL's numeric range, or PostgreSQL's refusal, "value overflows numeric
/// format" (engine review 21 MED 10): an operand's scale was bounded only by i64, so one division
/// or rescale could build a power of ten with billions of digits, with no interrupt point, or
/// overflow the scale arithmetic. Checked where a value enters numeric arithmetic from text or a
/// real (`value_to_bigdecimal`) and where numeric's INSERT parses one (`numeric_encode`). The digit
/// count is bounded from the integer's bit length first, so an ordinary value pays no conversion.
///
/// A zero is bounded by its fraction digits alone, as PostgreSQL bounds it (Codex C25 HIGH 3: every
/// zero passed, so `'0e-2000000000' / x` asked the division for 10^1999999000). A zero with a
/// positive exponent is in range; `PgNumeric::new` makes it a plain 0.
fn check_numeric_range(v: &BigDecimal) -> crate::Result<()> {
    let (int, scale) = v.as_bigint_and_exponent();
    if int.sign() == Sign::NoSign {
        // Mutant `numeric_zero_unbounded` (test builds only): every zero passes, as before.
        if i128::from(scale) <= PG_MAX_FRACTION_DIGITS || crate::branch::store::fe_mutant("numeric_zero_unbounded") {
            return Ok(());
        }
        return Err(LimboError::Constraint(SqlErrorMessage::classified(SqlErrorKind::NumericValueOutOfRange, "value overflows numeric format".to_string())));
    }
    let scale = i128::from(scale);
    // At most this many decimal digits: bits * log10(2), rounded up.
    let at_most = i128::from(int.bits()) * 30_103 / 100_000 + 1;
    let fits = |digits: i128| scale <= PG_MAX_FRACTION_DIGITS && digits - scale <= PG_MAX_INTEGER_DIGITS;
    if fits(at_most) || fits(int.magnitude().to_string().len() as i128) {
        return Ok(());
    }
    Err(LimboError::Constraint(SqlErrorMessage::classified(
        SqlErrorKind::NumericValueOutOfRange,
        "value overflows numeric format".to_string(),
    )))
}

/// `a / b` as PostgreSQL computes it (numeric.c `select_div_scale`, then `div_var` rounding at
/// that scale): the result scale keeps at least 16 significant digits and is never below either
/// operand's display scale, and the quotient is rounded half away from zero (lead ruling
/// 2026-10-09 on e52f01422). `b` must not be zero. Both operands must be within PostgreSQL's
/// numeric range (`check_numeric_range`); one that is not is refused here too, so the shift below
/// stays a few hundred thousand digits at most (engine review 21 MED 10: it was unbounded, and the
/// exponent past u32 was silently replaced by u32::MAX).
fn pg_numeric_div(a: &BigDecimal, b: &BigDecimal) -> crate::Result<BigDecimal> {
    // Mutant `numeric_range_unchecked` (test builds only) skips this check too.
    if !crate::branch::store::fe_mutant("numeric_range_unchecked") {
        check_numeric_range(a)?;
        check_numeric_range(b)?;
    }
    let rscale = pg_div_scale(a, b);
    // A zero dividend is zero at the result scale: no power of ten is formed for it, whatever
    // exponent it carries (Codex C25 HIGH 3). Mutant `numeric_zero_unbounded`: divided as any value.
    if a.as_bigint_and_exponent().0.sign() == Sign::NoSign && !crate::branch::store::fe_mutant("numeric_zero_unbounded") {
        return Ok(BigDecimal::new(BigInt::from(0), rscale));
    }
    let (ai, sa) = a.as_bigint_and_exponent();
    let (bi, sb) = b.as_bigint_and_exponent();
    // a / b = (ai / bi) * 10^(sb - sa); the result's integer at `rscale` is that times 10^rscale.
    let overflow = || {
        LimboError::Constraint(SqlErrorMessage::classified(
            SqlErrorKind::NumericValueOutOfRange,
            "value overflows numeric format".to_string(),
        ))
    };
    let shift = rscale
        .checked_add(sb)
        .and_then(|s| s.checked_sub(sa))
        .ok_or_else(overflow)?;
    let pow10 = |k: i64| -> crate::Result<BigInt> {
        Ok(BigInt::from(10).pow(u32::try_from(k).map_err(|_| overflow())?))
    };
    let (num, den) = if shift >= 0 {
        (ai * pow10(shift)?, bi)
    } else {
        (ai, bi * pow10(-shift)?)
    };
    // Truncated toward zero, then rounded half away from zero on the remainder.
    let mut q = &num / &den;
    let r = &num - &q * &den;
    let twice = r.magnitude() + r.magnitude();
    if &twice >= den.magnitude() {
        if (num.sign() == Sign::Minus) != (den.sign() == Sign::Minus) {
            q -= BigInt::from(1);
        } else {
            q += BigInt::from(1);
        }
    }
    Ok(BigDecimal::new(q, rscale))
}

/// numeric.c `select_div_scale`: the quotient's estimated weight from the two operands' weights
/// and first base-10000 digits (equal first digits are taken as `a` < `b`), then 16 significant
/// digits' worth of scale, bounded by the operands' display scales and PostgreSQL's display range.
fn pg_div_scale(a: &BigDecimal, b: &BigDecimal) -> i64 {
    let (weight1, first1) = pg_weight_and_first_digit(a);
    let (weight2, first2) = pg_weight_and_first_digit(b);
    let mut qweight = weight1 - weight2;
    // Mutant `div_scale_first_digit_strict` (test builds only; engine review 21 MED 13): equal
    // first digits taken as a > b.
    let lower = if crate::branch::store::fe_mutant("div_scale_first_digit_strict") {
        first1 < first2
    } else {
        first1 <= first2
    };
    if lower {
        qweight -= 1;
    }
    let display = |v: &BigDecimal| v.as_bigint_and_exponent().1.max(0);
    (PG_MIN_SIG_DIGITS - qweight * PG_DEC_DIGITS)
        .max(display(a))
        .max(display(b))
        .max(0)
        .min(PG_MAX_DISPLAY_SCALE)
}

/// numeric.c's normalized weight of `v` (the base-10000 position of its first nonzero digit) and
/// that digit's value; `(0, 0)` for zero, as `select_div_scale` takes it.
fn pg_weight_and_first_digit(v: &BigDecimal) -> (i64, u32) {
    let (int, scale) = v.as_bigint_and_exponent();
    let digits = int.magnitude().to_string();
    if digits == "0" {
        return (0, 0);
    }
    // The decimal exponent of the most significant digit, and the base-10000 digit holding it,
    // which spans decimal exponents 4 * weight + 3 down to 4 * weight.
    let e10 = digits.len() as i64 - 1 - scale;
    let weight = e10.div_euclid(PG_DEC_DIGITS);
    let lead = usize::try_from(e10 - weight * PG_DEC_DIGITS + 1).unwrap_or(1);
    let first: String = digits.chars().chain(std::iter::repeat('0')).take(lead).collect();
    (weight, first.parse().unwrap_or(0))
}

/// Format a BigDecimal as a string, preserving trailing zeros for the scale.
/// BigDecimal::to_string() may drop trailing zeros, but we want "1.10" to
/// remain "1.10" and "0.00" to remain "0.00" for a value stored with scale=2.
fn format_numeric(bd: &BigDecimal) -> String {
    let (bigint, scale) = bd.as_bigint_and_exponent();
    if scale <= 0 {
        // No decimal places: just print the integer (possibly scaled up)
        if scale == 0 {
            return bigint.to_string();
        }
        // Negative scale means multiply by 10^(-scale).
        // Cap to MAX_SCALE_MAGNITUDE (validated on read) to avoid runaway allocation.
        let neg_scale = scale.unsigned_abs().min(MAX_SCALE_MAGNITUDE as u64) as u32;
        let factor = num_bigint::BigInt::from(10).pow(neg_scale);
        return (bigint * factor).to_string();
    }
    let scale = scale as usize;
    let is_negative = bigint.sign() == Sign::Minus;
    let digits = bigint.magnitude().to_string();
    let digits_len = digits.len();

    let (integer_part, frac_part) = if digits_len > scale {
        let split = digits_len - scale;
        (&digits[..split], digits[split..].to_string())
    } else {
        let zeros = "0".repeat(scale - digits_len);
        ("0", format!("{zeros}{digits}"))
    };

    if is_negative {
        format!("-{integer_part}.{frac_part}")
    } else {
        format!("{integer_part}.{frac_part}")
    }
}

/// A numeric value within PostgreSQL's limits (lead ruling, class (a), DECISIONS 994f7d41e0): at most
/// 16383 fraction digits and 131072 integer digits, a zero with a positive exponent made a plain 0.
/// `new` is its only constructor and every arithmetic result is built through it, so an unbounded
/// value cannot exist past the parser: each one-instance fix before it (the zero's scale, C25 HIGH
/// 3; the product's scale; the operand range, review 21 MED 10; the typmod's precision and scale,
/// review 0e8b1cf18..0a4a127a1 HIGH and MED 1) is now a property of the type. Every path a value
/// takes is one of these methods: `parse` (text, a real's text), `from_i64`, `from_blob` (a stored
/// value), the operators `add`/`sub`/`mul`/`div`, `with_typmod` (numeric's ENCODE and every CAST to
/// numeric(p, s)), `to_blob` and `format`. The expected values of its tests are PG 18.6's (artie
/// raws pg18-numeric-*.txt).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PgNumeric {
    NegInfinity,
    Finite(BigDecimal),
    Infinity,
    NaN,
}

/// PostgreSQL's display-scale cap on a product (numeric.c `mul_var`'s rscale is at most
/// NUMERIC_DSCALE_MAX).
const PG_MAX_DSCALE: i64 = 16_383;

impl PgNumeric {
    /// `v` within PostgreSQL's range, or PostgreSQL's 22003 "value overflows numeric format". Mutant
    /// `numeric_range_unchecked` (test builds only): any value, as before (`check_numeric_range`
    /// reads it); `numeric_zero_unbounded`: a zero keeps its exponent.
    pub fn new(v: BigDecimal) -> crate::Result<Self> {
        if !crate::branch::store::fe_mutant("numeric_range_unchecked") {
            check_numeric_range(&v)?;
        }
        let (int, scale) = v.as_bigint_and_exponent();
        if int.sign() == Sign::NoSign && scale < 0 && !crate::branch::store::fe_mutant("numeric_zero_unbounded") {
            return Ok(Self::Finite(BigDecimal::new(BigInt::from(0), 0)));
        }
        Ok(Self::Finite(v))
    }

    /// Numeric input text, or `None` for text that is not a number (the caller words the refusal).
    /// PostgreSQL's input exponent bound first (review C38 MED 2; numeric.c `set_var_from_str`): an
    /// exponent past INT_MAX / 2 either way, or one no integer holds, is 22003 "value overflows
    /// numeric format", even on a zero (PG 18.6: '0e1073741823' is 0, '0e1073741824' overflows; raw
    /// pg18-numeric-exponent.txt). Mutant `numeric_exponent_unbounded` (test builds only): no bound.
    pub fn parse(text: &str) -> Option<crate::Result<Self>> {
        use std::str::FromStr;
        const PG_MAX_EXPONENT: i128 = (i32::MAX / 2) as i128;
        let trimmed = text.trim_matches(|c: char| c.is_ascii_whitespace());
        match trimmed.to_ascii_lowercase().as_str() {
            "nan" => return Some(Ok(Self::NaN)),
            "inf" | "+inf" | "infinity" | "+infinity" => return Some(Ok(Self::Infinity)),
            "-inf" | "-infinity" => return Some(Ok(Self::NegInfinity)),
            _ => {}
        }
        if let Some(at) = trimmed.find(['e', 'E']) {
            let (mantissa, exp) = (&trimmed[..at], &trimmed[at + 1..]);
            let digits = exp.strip_prefix(['+', '-']).unwrap_or(exp);
            let numeric_shape = !mantissa.is_empty() && !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit());
            if numeric_shape && !crate::branch::store::fe_mutant("numeric_exponent_unbounded") {
                let bounded = exp.parse::<i128>().ok().filter(|e| (-PG_MAX_EXPONENT..=PG_MAX_EXPONENT).contains(e));
                if bounded.is_none() && BigDecimal::from_str(&format!("{mantissa}e0")).is_ok() {
                    return Some(Err(LimboError::Constraint(SqlErrorMessage::classified(SqlErrorKind::NumericValueOutOfRange, "value overflows numeric format"))));
                }
            }
        }
        BigDecimal::from_str(trimmed).ok().map(Self::new)
    }

    pub fn from_i64(i: i64) -> Self {
        Self::Finite(BigDecimal::from(i))
    }

    /// A stored value, refused if its blob holds one outside the range.
    pub fn from_blob(blob: &[u8]) -> crate::Result<Self> {
        match blob {
            [2, 0] => Ok(Self::NegInfinity),
            [2, 1] => Ok(Self::Infinity),
            [2, 2] => Ok(Self::NaN),
            _ => Self::new(blob_to_bigdecimal(blob)?),
        }
    }

    pub fn to_blob(&self) -> ValueBlob {
        match self {
            Self::Finite(v) => bigdecimal_to_blob(v),
            Self::NegInfinity => [2, 0].as_slice().into(),
            Self::Infinity => [2, 1].as_slice().into(),
            Self::NaN => [2, 2].as_slice().into(),
        }
    }

    pub fn format(&self) -> String {
        match self {
            Self::Finite(v) => format_numeric(v),
            Self::NegInfinity => "-Infinity".into(),
            Self::Infinity => "Infinity".into(),
            Self::NaN => "NaN".into(),
        }
    }

    pub fn is_zero(&self) -> bool {
        matches!(self, Self::Finite(v) if v.as_bigint_and_exponent().0.sign() == Sign::NoSign)
    }

    /// The display scale: the scale, at least 0.
    fn dscale(&self) -> i64 {
        match self { Self::Finite(v) => v.as_bigint_and_exponent().1.max(0), _ => 0 }
    }

    pub fn add(&self, other: &Self) -> crate::Result<Self> {
        match (self, other) {
            (Self::Finite(a), Self::Finite(b)) => Self::new(a + b),
            (Self::NaN, _) | (_, Self::NaN)
            | (Self::Infinity, Self::NegInfinity) | (Self::NegInfinity, Self::Infinity) => Ok(Self::NaN),
            (Self::Infinity, _) | (_, Self::Infinity) => Ok(Self::Infinity),
            (Self::NegInfinity, _) | (_, Self::NegInfinity) => Ok(Self::NegInfinity),
        }
    }

    pub fn sub(&self, other: &Self) -> crate::Result<Self> {
        self.add(&other.negate())
    }

    /// PostgreSQL's product: at the sum of the display scales, at most 16383, rounded half away from
    /// zero (a rescale up only appends zeros). Mutant `mul_scale_bigdecimal` (test builds only):
    /// bigdecimal's own product; `product_scale_uncapped`: the sum not capped.
    pub fn mul(&self, other: &Self) -> crate::Result<Self> {
        let (Self::Finite(a), Self::Finite(b)) = (self, other) else {
            if matches!(self, Self::NaN) || matches!(other, Self::NaN) || self.is_zero() || other.is_zero() {
                return Ok(Self::NaN);
            }
            return Ok(if self.is_negative() == other.is_negative() { Self::Infinity } else { Self::NegInfinity });
        };
        let product = a * b;
        if crate::branch::store::fe_mutant("mul_scale_bigdecimal") {
            return Self::new(product);
        }
        let mut scale = self.dscale() + other.dscale();
        if !crate::branch::store::fe_mutant("product_scale_uncapped") {
            scale = scale.min(PG_MAX_DSCALE);
        }
        Self::new(product.with_scale_round(scale, bigdecimal::RoundingMode::HalfUp))
    }

    /// PostgreSQL's quotient (`pg_numeric_div`); `other` must not be zero. Mutant
    /// `div_scale_bigdecimal` (test builds only): bigdecimal's own quotient.
    pub fn div(&self, other: &Self) -> crate::Result<Self> {
        if matches!(self, Self::NaN) || matches!(other, Self::NaN) {
            return Ok(Self::NaN);
        }
        if other.is_zero() {
            return Err(LimboError::InvalidArgument(SqlErrorMessage::classified(SqlErrorKind::DivisionByZero, "division by zero")));
        }
        match (self, other) {
            (Self::Finite(a), Self::Finite(b)) => {
                if crate::branch::store::fe_mutant("div_scale_bigdecimal") { Self::new(a / b) }
                else { Self::new(pg_numeric_div(a, b)?) }
            }
            (Self::Finite(_), _) => Ok(Self::from_i64(0)),
            (_, Self::Finite(_)) => Ok(if self.is_negative() == other.is_negative() { Self::Infinity } else { Self::NegInfinity }),
            _ => Ok(Self::NaN),
        }
    }

    pub fn negate(&self) -> Self {
        match self {
            Self::Finite(value) => Self::Finite(-value),
            Self::Infinity => Self::NegInfinity,
            Self::NegInfinity => Self::Infinity,
            Self::NaN => Self::NaN,
        }
    }

    pub fn is_negative(&self) -> bool {
        match self {
            Self::Finite(value) => value.as_bigint_and_exponent().0.sign() == Sign::Minus,
            Self::NegInfinity => true,
            _ => false,
        }
    }

    pub fn rounded_i64(&self) -> Option<i64> {
        use bigdecimal::ToPrimitive;
        match self {
            Self::Finite(value) => value.with_scale_round(0, bigdecimal::RoundingMode::HalfUp).to_i64(),
            _ => None,
        }
    }

    /// This value in `typmod` (numeric.c `apply_typmod`): rounded half away from zero to its scale,
    /// then refused with PostgreSQL's 22003 "numeric field overflow" if it needs more integer digits
    /// than the precision leaves. The typmod is bounded (`NumericTypmod::new`), so the rescale is a
    /// thousand digits at most. Mutant `typmod_truncates` (test builds only): digits dropped, as
    /// bigdecimal's `with_scale` did before.
    pub fn with_typmod(&self, typmod: NumericTypmod) -> crate::Result<Self> {
        let NumericTypmod { precision, scale } = typmod;
        let Self::Finite(value) = self else {
            return if matches!(self, Self::NaN) { Ok(Self::NaN) }
            else { Err(LimboError::InvalidArgument(SqlErrorMessage::classified(SqlErrorKind::NumericValueOutOfRange, "numeric field overflow"))) };
        };
        let rounded = if crate::branch::store::fe_mutant("typmod_truncates") {
            value.with_scale(scale)
        } else {
            value.with_scale_round(scale, bigdecimal::RoundingMode::HalfUp)
        };
        if !self.is_zero() {
            let (bigint, _) = rounded.as_bigint_and_exponent();
            let digits = if bigint.sign() == Sign::NoSign { 0 } else { bigint.magnitude().to_string().len() as i64 };
            if digits > precision {
                return Err(LimboError::Constraint(SqlErrorMessage::classified(SqlErrorKind::NumericValueOutOfRange, format!(
                    "numeric field overflow: a field with precision {precision}, scale {scale} must round to an absolute value less than 10^{}",
                    precision - scale
                )).with_protocol_text("numeric field overflow")));
            }
        }
        Self::new(rounded)
    }
}

/// numeric(precision, scale) as PostgreSQL bounds it (numeric.c `numerictypmodin`): precision 1 to
/// 1000 and scale -1000 to 1000, refused with its 22023 messages. `new` is the only constructor;
/// CREATE TABLE, ALTER TABLE ADD COLUMN (`check_numeric_typmod`) and numeric's ENCODE build one,
/// so no rescale is ever asked for an unbounded scale. Narrower than PostgreSQL still, as before:
/// a negative scale and a scale above the precision are refused (PostgreSQL 15+ accepts both).
/// Mutant `typmod_range_unchecked` (test builds only): any precision and scale pass the range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NumericTypmod {
    precision: i64,
    scale: i64,
}

impl NumericTypmod {
    pub fn new(precision: i64, scale: i64) -> crate::Result<Self> {
        if !crate::branch::store::fe_mutant("typmod_range_unchecked") {
            if !(1..=1000).contains(&precision) {
                return Err(LimboError::Constraint(format!(
                    "NUMERIC precision {precision} must be between 1 and 1000"
                ).into()));
            }
            if !(-1000..=1000).contains(&scale) {
                return Err(LimboError::Constraint(format!(
                    "NUMERIC scale {scale} must be between -1000 and 1000"
                ).into()));
            }
        }
        if scale < 0 {
            return Err(LimboError::Constraint(format!(
                "numeric scale must be non-negative, got {scale}"
            ).into()));
        }
        if scale > precision {
            return Err(LimboError::Constraint(format!(
                "numeric scale ({scale}) must not exceed precision ({precision})"
            ).into()));
        }
        Ok(Self { precision, scale })
    }
}

/// The old validator's shape, kept for its unit tests: `val` in numeric(precision, scale) through the
/// bounded type.
#[cfg(test)]
fn validate_precision_scale(val: &BigDecimal, precision: i64, scale: i64) -> crate::Result<BigDecimal> {
    let typmod = NumericTypmod::new(precision, scale)?;
    match PgNumeric::new(val.clone())?.with_typmod(typmod)? {
        PgNumeric::Finite(value) => Ok(value),
        _ => unreachable!("finite typmod input stays finite"),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn c50_numeric_special_values_use_the_existing_numeric_representation() {
        use super::PgNumeric;
        let nan = PgNumeric::parse("NaN").expect("PG numeric accepts NaN").unwrap();
        let infinity = PgNumeric::parse("Infinity").expect("PG numeric accepts Infinity").unwrap();
        for (value, text) in [(&nan, "NaN"), (&infinity, "Infinity")] {
            assert_eq!(value.format(), text);
            assert_eq!(PgNumeric::from_blob(&value.to_blob()).unwrap(), *value);
        }
        assert_eq!(nan.div(&PgNumeric::from_i64(0)).unwrap().format(), "NaN");
        assert!(nan > infinity);
    }

    use super::*;
    use bigdecimal::Zero;
    use std::str::FromStr;

    #[test]
    fn test_roundtrip_positive() {
        let val = BigDecimal::from_str("123.45").unwrap();
        let blob = bigdecimal_to_blob(&val);
        let decoded = blob_to_bigdecimal(&blob).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_roundtrip_negative() {
        let val = BigDecimal::from_str("-987.654").unwrap();
        let blob = bigdecimal_to_blob(&val);
        let decoded = blob_to_bigdecimal(&blob).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_roundtrip_zero() {
        let val = BigDecimal::from_str("0").unwrap();
        let blob = bigdecimal_to_blob(&val);
        let decoded = blob_to_bigdecimal(&blob).unwrap();
        assert!(decoded.is_zero());
    }

    #[test]
    fn test_roundtrip_large() {
        // 100+ digit number
        let s = "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890.99";
        let val = BigDecimal::from_str(s).unwrap();
        let blob = bigdecimal_to_blob(&val);
        let decoded = blob_to_bigdecimal(&blob).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_validate_precision_scale_ok() {
        let val = BigDecimal::from_str("123.45").unwrap();
        let result = validate_precision_scale(&val, 5, 2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_precision_scale_overflow() {
        let val = BigDecimal::from_str("12345.67").unwrap();
        let result = validate_precision_scale(&val, 5, 2);
        assert!(result.is_err());
    }

    // ================================================================
    // Property-based fuzz tests (quickcheck)
    // ================================================================

    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;

    /// Newtype for generating arbitrary BigDecimal values via quickcheck.
    #[derive(Debug, Clone)]
    struct ArbDecimal(BigDecimal);

    impl Arbitrary for ArbDecimal {
        fn arbitrary(g: &mut Gen) -> Self {
            let mantissa = i64::arbitrary(g);
            // Scale in [-100, 100] — covers negative scale (large integers),
            // zero scale (plain integers), and positive scale (decimals).
            let scale = (i64::arbitrary(g) % 101).abs();
            let sign_flip = bool::arbitrary(g);
            let s = if sign_flip { -scale } else { scale };
            ArbDecimal(BigDecimal::new(BigInt::from(mantissa), s))
        }
    }

    /// Newtype for generating valid (precision, scale) pairs.
    #[derive(Debug, Clone)]
    struct ArbPrecisionScale {
        precision: i64,
        scale: i64,
    }

    impl Arbitrary for ArbPrecisionScale {
        fn arbitrary(g: &mut Gen) -> Self {
            let precision = (u8::arbitrary(g) % 38) as i64 + 1; // 1..=38
            let scale = (u8::arbitrary(g) as i64) % (precision + 1); // 0..=precision
            ArbPrecisionScale { precision, scale }
        }
    }

    // P1: Roundtrip encode/decode — blob_to_bigdecimal(bigdecimal_to_blob(x)) == x
    #[quickcheck]
    fn prop_blob_roundtrip(arb: ArbDecimal) -> bool {
        let blob = bigdecimal_to_blob(&arb.0);
        let decoded = blob_to_bigdecimal(&blob).unwrap();
        decoded == arb.0
    }

    // P2: format_numeric parse roundtrip — for non-negative scale,
    // parsing the formatted string should produce a numerically equal value.
    #[quickcheck]
    fn prop_format_parse_roundtrip(arb: ArbDecimal) -> bool {
        let (_, scale) = arb.0.as_bigint_and_exponent();
        if scale < 0 {
            return true; // skip negative scale — format_numeric expands it
        }
        let formatted = format_numeric(&arb.0);
        let parsed = BigDecimal::from_str(&formatted).unwrap();
        // Numerically equal (may differ in internal scale representation)
        parsed == arb.0
    }

    // P3: validate_precision_scale idempotence — applying it twice gives same result.
    #[quickcheck]
    fn prop_validate_idempotent(arb: ArbDecimal, ps: ArbPrecisionScale) -> bool {
        let first = validate_precision_scale(&arb.0, ps.precision, ps.scale);
        match first {
            Ok(y) => {
                let second = validate_precision_scale(&y, ps.precision, ps.scale).unwrap();
                second == y
            }
            Err(_) => true, // rejection is fine
        }
    }

    // P4: validate_precision_scale bounds — result fits within declared precision/scale.
    #[quickcheck]
    fn prop_validate_respects_bounds(arb: ArbDecimal, ps: ArbPrecisionScale) -> bool {
        match validate_precision_scale(&arb.0, ps.precision, ps.scale) {
            Ok(y) => {
                let (bigint, result_scale) = y.as_bigint_and_exponent();
                // Scale must match requested scale
                if result_scale != ps.scale {
                    return false;
                }
                // Digit count must be <= precision
                let digits = bigint.magnitude().to_string();
                let digit_count = if digits == "0" {
                    0
                } else {
                    digits.len() as i64
                };
                digit_count <= ps.precision
            }
            Err(_) => true,
        }
    }

    // P5: blob_to_bigdecimal never panics on arbitrary bytes.
    #[quickcheck]
    fn prop_blob_decode_no_panic(data: Vec<u8>) -> bool {
        let _ = blob_to_bigdecimal(&data);
        true // just checking it doesn't panic
    }

    // P6: format_numeric never panics.
    #[quickcheck]
    fn prop_format_no_panic(arb: ArbDecimal) -> bool {
        let _ = format_numeric(&arb.0);
        true
    }

    // P7: Ordering is preserved through encode/decode roundtrip.
    #[quickcheck]
    fn prop_ordering_preserved(a: ArbDecimal, b: ArbDecimal) -> bool {
        let blob_a = bigdecimal_to_blob(&a.0);
        let blob_b = bigdecimal_to_blob(&b.0);
        let decoded_a = blob_to_bigdecimal(&blob_a).unwrap();
        let decoded_b = blob_to_bigdecimal(&blob_b).unwrap();
        // The ordering of decoded values must match the ordering of originals
        a.0.cmp(&b.0) == decoded_a.cmp(&decoded_b)
    }
}
