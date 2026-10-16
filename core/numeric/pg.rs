use crate::error::{SqlErrorKind, SqlErrorMessage};
use crate::{LimboError, Numeric, Result, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgNumericType {
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    Dynamic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgNumericOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Negate,
    Abs,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    Cast { source: PgNumericType },
}

impl PgNumericType {
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name.to_ascii_lowercase().as_str() {
            "smallint" | "int2" | "smallserial" => Self::Int2,
            "integer" | "int" | "int4" | "serial" => Self::Int4,
            "bigint" | "int8" | "bigserial" => Self::Int8,
            "real" | "float4" => Self::Float4,
            "double precision" | "float8" => Self::Float8,
            "numeric" | "decimal" => Self::Numeric,
            _ => return None,
        })
    }

    pub fn from_oid(oid: u32) -> Option<Self> {
        Some(match oid {
            21 => Self::Int2,
            23 => Self::Int4,
            20 => Self::Int8,
            700 => Self::Float4,
            701 => Self::Float8,
            1700 => Self::Numeric,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Int2 => "smallint",
            Self::Int4 => "integer",
            Self::Int8 => "bigint",
            Self::Float4 => "real",
            Self::Float8 => "double precision",
            Self::Numeric => "numeric",
            Self::Dynamic => "numeric",
        }
    }

    pub fn sum_type(self) -> Self {
        match self {
            Self::Int2 | Self::Int4 => Self::Int8,
            Self::Int8 | Self::Numeric => Self::Numeric,
            other => other,
        }
    }

    pub fn is_integer(self) -> bool {
        matches!(self, Self::Int2 | Self::Int4 | Self::Int8)
    }

    pub fn wider(self, rhs: Self) -> Self {
        fn rank(ty: PgNumericType) -> u8 {
            match ty {
                PgNumericType::Dynamic => 0,
                PgNumericType::Int2 => 1,
                PgNumericType::Int4 => 2,
                PgNumericType::Int8 => 3,
                PgNumericType::Numeric => 4,
                PgNumericType::Float4 => 5,
                PgNumericType::Float8 => 6,
            }
        }
        if rank(rhs) > rank(self) {
            rhs
        } else {
            self
        }
    }

    fn checked_integer(self, value: i64) -> Result<i64> {
        let fits = match self {
            Self::Int2 => i16::try_from(value).is_ok(),
            Self::Int4 => i32::try_from(value).is_ok(),
            Self::Int8 => true,
            _ => unreachable!("integer range on a non-integer type"),
        };
        if fits {
            Ok(value)
        } else {
            Err(self.overflow())
        }
    }

    fn overflow(self) -> LimboError {
        range_error(format!("{} out of range", self.name()))
    }
}

pub fn execute(
    op: PgNumericOp,
    ty: PgNumericType,
    lhs: &Value,
    rhs: Option<&Value>,
) -> Result<Value> {
    if matches!(lhs, Value::Null) || rhs.is_some_and(|r| matches!(r, Value::Null)) {
        return Ok(Value::Null);
    }
    if let PgNumericOp::Cast { source } = op {
        return cast(lhs, source, ty);
    }
    if ty == PgNumericType::Dynamic {
        return Err(unresolved_type());
    }
    if ty == PgNumericType::Numeric {
        let a = numeric(lhs)?;
        let b = rhs.map(numeric).transpose()?;
        if matches!(
            op,
            PgNumericOp::Equal
                | PgNumericOp::NotEqual
                | PgNumericOp::Less
                | PgNumericOp::LessEqual
                | PgNumericOp::Greater
                | PgNumericOp::GreaterEqual
        ) {
            let b = b.as_ref().expect("binary argument");
            let yes = match op {
                PgNumericOp::Equal => a == *b,
                PgNumericOp::NotEqual => a != *b,
                PgNumericOp::Less => a < *b,
                PgNumericOp::LessEqual => a <= *b,
                PgNumericOp::Greater => a > *b,
                PgNumericOp::GreaterEqual => a >= *b,
                _ => unreachable!(),
            };
            return Ok(Value::from_i64(i64::from(yes)));
        }
        let result = match op {
            PgNumericOp::Add => a.add(b.as_ref().expect("binary argument"))?,
            PgNumericOp::Subtract => a.sub(b.as_ref().expect("binary argument"))?,
            PgNumericOp::Multiply => a.mul(b.as_ref().expect("binary argument"))?,
            PgNumericOp::Divide => a.div(b.as_ref().expect("binary argument"))?,
            PgNumericOp::Negate => a.negate(),
            PgNumericOp::Abs => {
                if a.is_negative() {
                    a.negate()
                } else {
                    a
                }
            }
            _ => unreachable!("operation already handled"),
        };
        return Ok(Value::build_text(result.format()));
    }
    if ty.is_integer() {
        let a = integer(lhs, ty, PgNumericType::Dynamic)?;
        let b = rhs
            .map(|v| integer(v, ty, PgNumericType::Dynamic))
            .transpose()?;
        let result = match op {
            PgNumericOp::Add => a.checked_add(b.expect("binary argument")),
            PgNumericOp::Subtract => a.checked_sub(b.expect("binary argument")),
            PgNumericOp::Multiply => a.checked_mul(b.expect("binary argument")),
            PgNumericOp::Divide => {
                let b = b.expect("binary argument");
                if b == 0 {
                    return Ok(Value::Null);
                }
                a.checked_div(b)
            }
            PgNumericOp::Negate => a.checked_neg(),
            PgNumericOp::Abs => a.checked_abs(),
            _ => unreachable!("operation already handled"),
        }
        .ok_or_else(|| ty.overflow())?;
        return Ok(Value::from_i64(ty.checked_integer(result)?));
    }
    let a = float(lhs, ty)?;
    let b = rhs.map(|v| float(v, ty)).transpose()?;
    let result = match op {
        PgNumericOp::Add => a + b.expect("binary argument"),
        PgNumericOp::Subtract => a - b.expect("binary argument"),
        PgNumericOp::Multiply => a * b.expect("binary argument"),
        PgNumericOp::Divide => {
            let b = b.expect("binary argument");
            if b == 0.0 {
                return Ok(Value::Null);
            }
            a / b
        }
        PgNumericOp::Negate => -a,
        PgNumericOp::Abs => a.abs(),
        _ => unreachable!("operation already handled"),
    };
    if result.is_infinite() && a.is_finite() && b.is_none_or(f64::is_finite) {
        return Err(range_error("value out of range: overflow"));
    }
    float_value(result, ty, None)
}

pub fn cast(value: &Value, source: PgNumericType, target: PgNumericType) -> Result<Value> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    if target == PgNumericType::Numeric {
        return Ok(Value::build_text(numeric(value)?.format()));
    }
    if target.is_integer() {
        return integer(value, target, source).map(Value::from_i64);
    }
    let value_float = float(value, target)?;
    let rendered = if source == PgNumericType::Numeric {
        Value::build_text(numeric(value)?.format())
    } else {
        value.clone()
    };
    float_value(value_float, target, Some(&rendered))
}

fn integer(value: &Value, target: PgNumericType, source: PgNumericType) -> Result<i64> {
    let i = match value {
        Value::Numeric(Numeric::Integer(i)) => *i,
        Value::Numeric(Numeric::Float(f)) => {
            let f = f64::from(*f).round_ties_even();
            if !f.is_finite() || f < i64::MIN as f64 || f >= -(i64::MIN as f64) {
                return Err(target.overflow());
            }
            f as i64
        }
        Value::Text(t) if source == PgNumericType::Numeric => {
            let numeric = super::decimal::PgNumeric::parse(t.as_str())
                .ok_or_else(|| invalid_text(target, t.as_str()))??;
            numeric_integer(&numeric, target)?
        }
        Value::Text(t) => {
            let text = t.as_str().trim_matches(|c: char| c.is_ascii_whitespace());
            let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return Err(invalid_text(target, t.as_str()));
            }
            let i = text
                .parse::<i64>()
                .ok()
                .filter(|i| target.checked_integer(*i).is_ok());
            return i.ok_or_else(|| {
                range_error(format!(
                    "value \"{}\" is out of range for type {}",
                    t.as_str(),
                    target.name()
                ))
            });
        }
        _ => return Err(invalid_text(target, &value.to_string())),
    };
    target.checked_integer(i)
}

fn float(value: &Value, target: PgNumericType) -> Result<f64> {
    match value {
        Value::Numeric(Numeric::Integer(i)) => Ok(*i as f64),
        Value::Numeric(Numeric::Float(f)) => Ok(f64::from(*f)),
        Value::Text(t) => {
            let text = t.as_str().trim_matches(|c: char| c.is_ascii_whitespace());
            let f = text
                .parse::<f64>()
                .map_err(|_| invalid_text(target, t.as_str()))?;
            let explicit_inf = matches!(
                text.to_ascii_lowercase().as_str(),
                "inf" | "+inf" | "-inf" | "infinity" | "+infinity" | "-infinity"
            );
            if f.is_infinite() && !explicit_inf {
                return Err(range_error(format!(
                    "\"{}\" is out of range for type {}",
                    t.as_str(),
                    target.name()
                )));
            }
            if f == 0.0
                && text
                    .bytes()
                    .take_while(|b| !matches!(*b, b'e' | b'E'))
                    .any(|b| matches!(b, b'1'..=b'9'))
            {
                return Err(range_error(format!(
                    "\"{}\" is out of range for type {}",
                    t.as_str(),
                    target.name()
                )));
            }
            Ok(f)
        }
        _ => Err(invalid_text(target, &value.to_string())),
    }
}

fn float_value(value: f64, target: PgNumericType, input: Option<&Value>) -> Result<Value> {
    if target == PgNumericType::Float4 {
        let narrow = value as f32;
        if (narrow.is_infinite() && value.is_finite()) || (narrow == 0.0 && value != 0.0) {
            return Err(match input {
                Some(input) => range_error(format!("\"{input}\" is out of range for type real")),
                None => range_error(if narrow == 0.0 {
                    "value out of range: underflow"
                } else {
                    "value out of range: overflow"
                }),
            });
        }
        return Ok(Value::from_f64(narrow as f64));
    }
    Ok(Value::from_f64(value))
}

fn range_error(message: impl Into<String>) -> LimboError {
    LimboError::InvalidArgument(SqlErrorMessage::classified(
        SqlErrorKind::NumericValueOutOfRange,
        message.into(),
    ))
}

fn invalid_text(target: PgNumericType, text: &str) -> LimboError {
    LimboError::InvalidArgument(SqlErrorMessage::classified(
        SqlErrorKind::InvalidTextRepresentation,
        format!(
            "invalid input syntax for type {}: \"{text}\"",
            target.name()
        ),
    ))
}

pub fn numeric(value: &Value) -> Result<super::decimal::PgNumeric> {
    use super::decimal::PgNumeric;
    match value {
        Value::Numeric(Numeric::Integer(i)) => Ok(PgNumeric::from_i64(*i)),
        Value::Numeric(Numeric::Float(f)) => {
            PgNumeric::parse(&f.to_string()).expect("float displays numeric")
        }
        Value::Text(text) => PgNumeric::parse(text.as_str())
            .ok_or_else(|| invalid_text(PgNumericType::Numeric, text.as_str()))?,
        Value::Blob(_) => Err(invalid_text(PgNumericType::Numeric, &value.to_string())),
        _ => Err(invalid_text(PgNumericType::Numeric, &value.to_string())),
    }
}

/// Shared PG sum transition for grouped and window aggregation. Numeric special values have
/// separate counts so a moving frame can remove NaN/infinity and recover its finite sum.
/// Payload: [finite sum, non-NULL count, NaN count, +infinity count, -infinity count].
pub fn sum_update(
    ty: PgNumericType,
    arg: &Value,
    state: &mut [Value],
    inverse: bool,
) -> Result<()> {
    use super::decimal::PgNumeric;
    if matches!(arg, Value::Null) {
        return Ok(());
    }
    if ty == PgNumericType::Dynamic {
        return Err(unresolved_type());
    }
    let delta = if inverse { -1 } else { 1 };
    let count = state[1].as_int().expect("PG sum count") + delta;
    state[1] = Value::from_i64(count);
    if ty == PgNumericType::Numeric {
        let value = numeric(arg)?;
        let special = match value {
            PgNumeric::NaN => Some(2),
            PgNumeric::Infinity => Some(3),
            PgNumeric::NegInfinity => Some(4),
            PgNumeric::Finite(_) => None,
        };
        if let Some(slot) = special {
            state[slot] =
                Value::from_i64(state[slot].as_int().expect("PG sum special count") + delta);
        } else {
            let sum = if matches!(state[0], Value::Null) {
                PgNumeric::from_i64(0)
            } else {
                sum_numeric(&state[0])?
            };
            let result = if inverse {
                sum.sub(&value)?
            } else {
                sum.add(&value)?
            };
            state[0] = Value::Blob(result.to_blob());
        }
    } else if count == 0 {
        state[0] = Value::Null;
    } else if matches!(state[0], Value::Null) {
        state[0] = cast(arg, PgNumericType::Dynamic, ty)?;
    } else {
        state[0] = execute(
            if inverse {
                PgNumericOp::Subtract
            } else {
                PgNumericOp::Add
            },
            ty,
            &state[0],
            Some(arg),
        )?;
    }
    Ok(())
}

pub fn sum_final(ty: PgNumericType, state: &[Value]) -> Result<Value> {
    use super::decimal::PgNumeric;
    if state[1].as_int() == Some(0) {
        return Ok(Value::Null);
    }
    if ty != PgNumericType::Numeric {
        return Ok(state[0].clone());
    }
    let has = |slot: usize| state[slot].as_int().is_some_and(|n| n > 0);
    let result = if has(2) || (has(3) && has(4)) {
        PgNumeric::NaN
    } else if has(3) {
        PgNumeric::Infinity
    } else if has(4) {
        PgNumeric::NegInfinity
    } else if matches!(state[0], Value::Null) {
        PgNumeric::from_i64(0)
    } else {
        sum_numeric(&state[0])?
    };
    Ok(Value::build_text(result.format()))
}

fn numeric_integer(value: &super::decimal::PgNumeric, target: PgNumericType) -> Result<i64> {
    use super::decimal::PgNumeric;
    let special = match value {
        PgNumeric::NaN => Some("NaN"),
        PgNumeric::Infinity | PgNumeric::NegInfinity => Some("infinity"),
        _ => None,
    };
    if let Some(special) = special {
        return Err(LimboError::InvalidArgument(SqlErrorMessage::classified(
            SqlErrorKind::FeatureNotSupported,
            format!("cannot convert {special} to {}", target.name()),
        )));
    }
    value.rounded_i64().ok_or_else(|| target.overflow())
}

fn sum_numeric(value: &Value) -> Result<super::decimal::PgNumeric> {
    match value {
        Value::Blob(blob) => super::decimal::PgNumeric::from_blob(blob),
        other => numeric(other),
    }
}

fn unresolved_type() -> LimboError {
    LimboError::InvalidArgument(SqlErrorMessage::classified(
        SqlErrorKind::FeatureNotSupported,
        "could not determine PostgreSQL numeric operand type",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn c50_missing_operand_type_refuses_instead_of_sqlite_fallback() {
        let error = execute(
            PgNumericOp::Add,
            PgNumericType::Dynamic,
            &Value::from_i64(i64::MAX),
            Some(&Value::from_i64(1)),
        )
        .unwrap_err();
        assert_eq!(error.sql_error_kind(), SqlErrorKind::FeatureNotSupported);
        assert_eq!(
            error.to_string(),
            "could not determine PostgreSQL numeric operand type"
        );
        let mut state = vec![
            Value::Null,
            Value::from_i64(0),
            Value::from_i64(0),
            Value::from_i64(0),
            Value::from_i64(0),
        ];
        let error = sum_update(
            PgNumericType::Dynamic,
            &Value::from_i64(1),
            &mut state,
            false,
        )
        .unwrap_err();
        assert_eq!(error.sql_error_kind(), SqlErrorKind::FeatureNotSupported);
    }
}
