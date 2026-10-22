use turso_parser::sql_error::{SqlErrorKind, SqlErrorMessage};
use std::sync::Arc;
use turso_core::schema::{Schema, Table};
use turso_core::{Connection, LimboError, Result, Value};
use turso_parser::ast::RefAct;

const USER_TABLE_OID_START: i64 = 16384;

/// Resolve a PostgreSQL scalar function by name and argument count. Entry
/// point for [`crate::catalog::PostgresDialect::resolve_function`].
pub(crate) fn resolve_scalar(name: &str, arg_count: usize) -> bool {
    let arities: &[i64] = match name {
        "pg_get_userbyid"
        | "pg_table_is_visible"
        | "pg_function_is_visible"
        | "pg_type_is_visible"
        | "pg_encoding_to_char"
        | "pg_get_function_result"
        | "pg_get_function_arguments"
        | "pg_get_statisticsobjdef_columns"
        | "pg_relation_is_publishable"
        | "quote_ident"
        | "quote_literal"
        | "pg_database_size"
        | "current_schemas"
        | "int2send"
        | "int4send"
        | "int8send"
        | "pg_regclass" => &[1],
        "format_type" | "pg_get_constraintdef" | "pg_get_indexdef" | "obj_description" => &[1, 2],
        "pg_get_expr" => &[2, 3],
        "to_char" | "pg_input_is_valid" | "booleq" | "boolne" | "col_description" | "bpchareq"
        | "encode" | "bpcharlt" | "pg_text_int" | "pg_assign_int" => &[2],
        "pg_float_text" | "pg_numeric_mod" => &[2],
        "pg_assign_int_value" | "pg_assign_numeric_value" => &[3],
        "version" | "current_database" | "current_schema" | "pg_backend_pid" => &[0],
        "now" | "transaction_timestamp" | "statement_timestamp" => &[0],
        "make_date" => &[3],
        "make_timestamp" => &[6],
        _ => return false,
    };
    arities.contains(&(arg_count as i64))
}

/// Execute a PostgreSQL scalar function by name. Entry point for
/// [`crate::catalog::PostgresDialect::scalar_function`].
pub(crate) fn exec_scalar(conn: &Connection, name: &str, args: &[Value]) -> Result<Value> {
    let int_arg = |i: usize, default: i64| args.get(i).and_then(|v| v.as_int()).unwrap_or(default);
    let text_arg = |i: usize| match args.get(i) {
        Some(Value::Text(t)) => t.as_str().to_string(),
        _ => String::new(),
    };
    match name {
        "now" | "transaction_timestamp" | "statement_timestamp" => Ok(Value::build_text(
            crate::session::session_time(conn, name == "statement_timestamp"),
        )),
        // PostgreSQL refuses a date past the range (22008) where the SQLite dialect answers NULL.
        "make_date" => turso_core::make_date_or_refuse(args),
        "make_timestamp" => turso_core::make_timestamp_or_refuse(args),
        "pg_get_userbyid" => Ok(exec_pg_get_user_by_id(int_arg(0, 0))),
        "pg_table_is_visible" | "pg_function_is_visible" | "pg_type_is_visible" => {
            Ok(exec_pg_is_visible(int_arg(0, 0)))
        }
        "pg_get_constraintdef" => Ok(exec_pg_get_constraintdef(conn, int_arg(0, 0))),
        "pg_get_indexdef" => Ok(exec_pg_get_indexdef(conn, int_arg(0, 0))),
        "pg_regclass" => exec_pg_regclass(conn, args),
        "pg_encoding_to_char" => Ok(exec_pg_encoding_to_char(int_arg(0, 0))),
        "format_type" => Ok(exec_pg_format_type(int_arg(0, 0), int_arg(1, -1))),
        "encode" => exec_encode(args),
        "int2send" | "int4send" | "int8send" => exec_int_send(name, args.first().unwrap_or(&Value::Null)),
        "to_char" => Ok(exec_to_char(
            args.first().unwrap_or(&Value::Null),
            &text_arg(1),
        )),
        "pg_input_is_valid" => Ok(exec_pg_input_is_valid(
            args.first().unwrap_or(&Value::Null),
            &text_arg(1),
        )),
        // A text column's value cast to an integer type, read as int2in/int4in/int8in read it:
        // 22P02 for anything but an integer, 22003 past the type (wire-rev-1de981755-numeric MED-1).
        // Any other value passes to the cast unchanged.
        // A real or float8 value written as text as float4out or float8out writes it (the
        // shortest text that reads back exactly), with the default extra_float_digits; written by
        // the translator for a cast to a string type or an operand of || (its with_float_texts).
        // Any other value passes as text unchanged.
        "pg_float_text" => Ok(match args.first() {
            Some(Value::Numeric(turso_core::Numeric::Float(f))) => {
                let digits = pgwire::types::format::float_digits::ExtraFloatDigits(1);
                let v = f64::from(*f);
                Value::build_text(if text_arg(1) == "float4" { digits.format_f32(v as f32) } else { digits.format_f64(v) })
            }
            Some(Value::Numeric(turso_core::Numeric::Integer(i))) => Value::build_text(i.to_string()),
            Some(Value::Null) | None => Value::Null,
            Some(other) => Value::build_text(other.to_string()),
        }),
        // PostgreSQL's numeric remainder (numeric_mod): the remainder of the dividend by the
        // divisor truncated toward zero, so its sign is the dividend's, and 22012 for a zero
        // divisor. Written by the translator for a % over a numeric (its with_numeric_mods). Each
        // value is read as the decimal its shortest text spells, which is exact for the numerics
        // of at most 15 significant digits held here (PGDIV-NUMERIC-LITERAL-WIDE refuses wider),
        // and the remainder is computed exactly on those decimals.
        "pg_numeric_mod" => {
            let decimal = |v: &Value| -> Option<String> {
                match v {
                    Value::Numeric(turso_core::Numeric::Integer(i)) => Some(i.to_string()),
                    Value::Numeric(turso_core::Numeric::Float(f)) => Some(format!("{}", f64::from(*f))),
                    Value::Text(t) => Some(t.as_str().trim().to_string()),
                    _ => None,
                }
            };
            let (Some(a), Some(b)) = (args.first(), args.get(1)) else { return Ok(Value::Null) };
            if matches!(a, Value::Null) || matches!(b, Value::Null) {
                return Ok(Value::Null);
            }
            // A value it cannot read as a decimal is refused, never answered as NULL.
            let (Some(x), Some(y)) = (decimal(a), decimal(b)) else {
                return Err(LimboError::Constraint(SqlErrorMessage::classified(SqlErrorKind::FeatureNotSupported, "numeric % over this value is not supported")));
            };
            match numeric_remainder(&x, &y) {
                Some(Ok(r)) => Ok(r.parse::<f64>().map(Value::from_f64).unwrap_or(Value::Null)),
                Some(Err(())) => Err(LimboError::Constraint(SqlErrorMessage::classified(SqlErrorKind::DivisionByZero, "division by zero"))),
                None => Ok(match (x.parse::<f64>(), y.parse::<f64>()) {
                    (Ok(_), Ok(d)) if d == 0.0 => return Err(LimboError::Constraint(SqlErrorMessage::classified(SqlErrorKind::DivisionByZero, "division by zero"))),
                    (Ok(n), Ok(d)) => Value::from_f64(n % d),
                    _ => Value::Null,
                }),
            }
        }
        // An expression's value assigned to an integer column, cast as PostgreSQL's assignment cast
        // casts it: an integer range-checked, a float or decimal text rounded ("numeric" half away
        // from zero, "float" half to even) and range-checked, 22003 '<type> out of range' past the
        // column; a numeric NaN or infinity 0A000, a float one 22003, as PostgreSQL refuses them.
        // Written by the translator (its with_integer_assignments). Any other value passes.
        "pg_assign_int_value" => {
            let name = text_arg(1);
            let numeric = text_arg(2) != "float";
            let limit: i128 = match name.as_str() {
                "smallint" => 1 << 15,
                "integer" => 1 << 31,
                _ => 1 << 63,
            };
            let out_of_range = || LimboError::Constraint(SqlErrorMessage::classified(SqlErrorKind::NumericValueOutOfRange, format!("{name} out of range")));
            let float = |f: f64| -> turso_core::Result<Value> {
                if !f.is_finite() {
                    return Err(if numeric {
                        LimboError::Constraint(SqlErrorMessage::classified(SqlErrorKind::FeatureNotSupported, format!("cannot convert {} to {name}", if f.is_nan() { "NaN" } else { "infinity" })))
                    } else {
                        out_of_range()
                    });
                }
                let rounded = if numeric { f.round() } else { f.round_ties_even() };
                if rounded < -(limit as f64) || rounded >= limit as f64 {
                    return Err(out_of_range());
                }
                Ok(Value::from_i64(rounded as i64))
            };
            match args.first() {
                Some(Value::Numeric(turso_core::Numeric::Integer(i))) => {
                    let v = i128::from(*i);
                    if v < -limit || v >= limit {
                        return Err(out_of_range());
                    }
                    Ok(Value::from_i64(*i))
                }
                Some(Value::Numeric(turso_core::Numeric::Float(f))) => float(f64::from(*f)),
                Some(Value::Text(t)) => match t.as_str().trim().parse::<f64>() {
                    Ok(f) => float(f),
                    Err(_) => Ok(Value::Text(t.clone())),
                },
                Some(other) => Ok(other.clone()),
                None => Ok(Value::Null),
            }
        }
        // An expression's value assigned to a numeric(p, s) column, checked and rounded as
        // PostgreSQL's assignment cast does it: numeric infinity, or a value past the precision once
        // rounded, is 22003 'numeric field overflow'; any other value is rounded half away from zero
        // to the scale, read as the decimal its shortest text spells (a float8's 2.555 is 2.56, as
        // float8_numeric reads it). NaN and a value it does not read pass to the engine. Written by
        // the translator (its with_numeric_assignments).
        "pg_assign_numeric_value" => {
            let precision = args.get(1).and_then(|v| match v {
                Value::Numeric(turso_core::Numeric::Integer(i)) => i32::try_from(*i).ok(),
                _ => None,
            });
            let scale = args.get(2).and_then(|v| match v {
                Value::Numeric(turso_core::Numeric::Integer(i)) => i32::try_from(*i).ok(),
                _ => None,
            });
            let (Some(precision), Some(scale)) = (precision, scale) else { return Ok(args.first().cloned().unwrap_or(Value::Null)) };
            let overflow = || LimboError::Constraint(SqlErrorMessage::classified(SqlErrorKind::NumericValueOutOfRange, "numeric field overflow"));
            let text = match args.first() {
                Some(Value::Numeric(turso_core::Numeric::Integer(i))) => i.to_string(),
                Some(Value::Numeric(turso_core::Numeric::Float(f))) => {
                    let f = f64::from(*f);
                    if f.is_infinite() {
                        return Err(overflow());
                    }
                    if f.is_nan() {
                        return Ok(Value::from_f64(f));
                    }
                    format!("{f}")
                }
                Some(Value::Text(t)) => {
                    let t = t.as_str().trim();
                    let word = t.trim_start_matches(['-', '+']);
                    if word.eq_ignore_ascii_case("infinity") || word.eq_ignore_ascii_case("inf") {
                        return Err(overflow());
                    }
                    t.to_string()
                }
                Some(other) => return Ok(other.clone()),
                None => return Ok(Value::Null),
            };
            if numeric_overflows(&text, precision, scale) == Some(true) {
                return Err(overflow());
            }
            match crate::session::numeric_at_scale(&text, scale) {
                Some(rounded) => Ok(Value::build_text(rounded)),
                None => Ok(args.first().cloned().unwrap_or(Value::Null)),
            }
        }
        "pg_text_int" => match args.first() {
            Some(Value::Text(t)) => match validate_pg_input(t.as_str(), &text_arg(1)) {
                Some((message, kind)) => Err(LimboError::Constraint(SqlErrorMessage::classified(kind, message))),
                None => Ok(match pg_strtoint(t.as_str(), 64) {
                    PgInt::Value(v) => Value::from_i64(v),
                    _ => Value::build_text(t.as_str().to_string()),
                }),
            },
            Some(other) => Ok(other.clone()),
            None => Ok(Value::Null),
        },
        // A numeric constant a DEFAULT gives an integer column, cast at the INSERT that uses it as
        // PostgreSQL casts it there: rounded half away from zero, 22003 '<type> out of range' past
        // the type (recorded, rec39-default-coercion, artie 32951be31a). Written by the translator
        // for a default past its column ([`crate::session`]'s literal_assignments).
        "pg_assign_int" => {
            let name = text_arg(1);
            let bits = match name.as_str() {
                "smallint" => 16,
                "integer" => 32,
                _ => 64,
            };
            let value = match args.first() {
                Some(Value::Text(t)) => round_decimal(t.as_str()),
                Some(other) => round_decimal(&other.to_string()),
                None => None,
            };
            let limit = 1i128 << (bits - 1);
            match value {
                Some(v) if v >= -limit && v < limit => Ok(Value::from_i64(v as i64)),
                _ => Err(LimboError::Constraint(SqlErrorMessage::classified(
                    SqlErrorKind::NumericValueOutOfRange,
                    format!("{name} out of range"),
                ))),
            }
        }
        // character(n)'s `=` and `<` (catalog.rs BPCHAR_TYPE_SQL): trailing blanks are not
        // significant, as PostgreSQL's bpchareq and bpcharlt (wire review 2 item 3). Both sides
        // are trimmed, so an operand the engine did not encode compares the same.
        "bpchareq" | "bpcharlt" => {
            let trimmed = |v: Option<&Value>| match v {
                None | Some(Value::Null) => None,
                Some(Value::Text(t)) => Some(t.as_str().trim_end_matches(' ').to_string()),
                Some(other) => Some(other.to_string().trim_end_matches(' ').to_string()),
            };
            Ok(match (trimmed(args.first()), trimmed(args.get(1))) {
                (Some(a), Some(b)) if name == "bpchareq" => Value::from_i64((a == b) as i64),
                (Some(a), Some(b)) => Value::from_i64((a < b) as i64),
                _ => Value::Null,
            })
        }
        "booleq" => Ok(Value::from_i64((args.first() == args.get(1)) as i64)),
        "boolne" => Ok(Value::from_i64((args.first() != args.get(1)) as i64)),
        "version" => Ok(exec_version()),
        "current_database" => Ok(Value::build_text(crate::catalog::database_name(conn))),
        // pg_catalog presents every user object under the hardcoded "public"
        // namespace, so that is always the current schema.
        "current_schema" => Ok(Value::build_text("public")),
        // The search path as an array literal, with the implicit pg_catalog first when asked.
        "current_schemas" => Ok(Value::build_text(
            match args.first().and_then(|v| v.as_int()) {
                Some(0) => "{public}",
                _ => "{pg_catalog,public}",
            },
        )),
        "pg_backend_pid" => Ok(Value::from_i64(std::process::id() as i64)),
        // The database's files on disk, in bytes: the database file and its WAL. A branch reports
        // the database's. A name is the one database pg_database lists here; any other is
        // PostgreSQL's 3D000 (found by the E5' differential, wire-e5l5-fc85e82fc: every name
        // answered this database's size). An oid is taken as before.
        // NULL answers NULL, and an oid is pg_database's one oid or 42704, as PostgreSQL 18.6
        // answers (wire-rev-uw3 LOW-3; recorded, pg18/pg18-wirefuzz-pg_database_size_of_null_or_an_unknown_oid_answers_as_postgresql_does.out).
        "pg_database_size" => {
            let path = conn.db_file_path();
            match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(Value::Text(t)) => {
                    let name = t.as_str();
                    if name != crate::catalog::database_name(conn) {
                        return Err(LimboError::InvalidArgument(SqlErrorMessage::classified(
                            SqlErrorKind::InvalidCatalogName,
                            format!("database \"{name}\" does not exist"),
                        )));
                    }
                }
                Some(v) => {
                    if v.as_int() != Some(crate::catalog::DATABASE_OID) {
                        // An oid is unsigned: oidin reads -1 as 4294967295 (wire-rev-2a68496d0 L4;
                        // recorded, pg18-wirefuzz-pg_database_size_reads_an_oid_as_postgresql_reads_one.out).
                        let oid = v.as_int().map_or_else(
                            || v.to_string(),
                            |o| if (i64::from(i32::MIN)..0).contains(&o) { (o + (1i64 << 32)).to_string() } else { o.to_string() },
                        );
                        return Err(LimboError::InvalidArgument(SqlErrorMessage::classified(
                            SqlErrorKind::UndefinedObject,
                            format!("database with OID {oid} does not exist"),
                        )));
                    }
                }
                None => {}
            }
            let size = |p: &str| std::fs::metadata(p).map(|m| m.len() as i64).unwrap_or(0);
            Ok(Value::from_i64(size(&path) + size(&format!("{path}-wal"))))
        }
        "quote_ident" => match args.first() {
            Some(Value::Null) | None => Ok(Value::Null),
            _ => Ok(Value::build_text(turso_pg_parser::quote_identifier(
                &text_arg(0),
            ))),
        },
        "quote_literal" => Ok(exec_quote_literal(args.first().unwrap_or(&Value::Null))),
        "pg_get_expr" => exec_pg_get_expr(args),
        // Catalog introspection stubs: accepted for compatibility, no output.
        // obj_description/col_description are NULL because COMMENT ON is not persisted.
        "pg_get_statisticsobjdef_columns"
        | "pg_relation_is_publishable"
        | "pg_get_function_result"
        | "pg_get_function_arguments"
        | "obj_description"
        | "col_description" => Ok(Value::Null),
        _ => Err(LimboError::ParseError(SqlErrorMessage::classified(SqlErrorKind::UndefinedFunction, format!("no such function: {name}")))),
    }
}

fn exec_pg_get_user_by_id(_oid: i64) -> Value {
    Value::build_text("turso")
}

fn exec_pg_is_visible(_oid: i64) -> Value {
    Value::from_i64(1)
}

/// PostgreSQL version advertised by version(). The major version matches the
/// server_version startup parameter pgwire's DefaultServerParameterProvider
/// sends, so the two claims agree.
const SERVER_VERSION: &str = "16.6";

/// Clients sometimes gate connection setup on this string: knex and TypeORM regex
/// parse it as `^PostgreSQL ([\d.]+)`, so it must lead with a numeric version.
fn exec_version() -> Value {
    Value::build_text(format!(
        "PostgreSQL {SERVER_VERSION} (Turso v{})",
        env!("CARGO_PKG_VERSION")
    ))
}

/// PostgreSQL's quote_literal(): wrap a value in single quotes for inclusion
/// in SQL, doubling embedded quotes. Backslashes force the E'' escape-string
/// form so the result reads back identically regardless of
/// standard_conforming_strings.
fn exec_quote_literal(value: &Value) -> Value {
    if matches!(value, Value::Null) {
        return Value::Null;
    }
    let text = match value {
        Value::Text(t) => t.as_str().to_string(),
        other => other.to_string(),
    };
    Value::build_text(crate::catalog::pg_quote_literal(&text))
}

fn exec_pg_encoding_to_char(encoding: i64) -> Value {
    let name = match encoding {
        6 => "UTF8",
        0 => "SQL_ASCII",
        _ => "UTF8",
    };
    Value::build_text(name)
}

fn exec_pg_get_constraintdef(conn: &Connection, oid: i64) -> Value {
    match pg_get_constraintdef(conn, oid) {
        Some(s) => Value::build_text(s),
        None => Value::Null,
    }
}

/// `value::regclass`: the named relation's pg_class oid (recorded, pg18-rev-regclass.out). A
/// number is taken as an oid; text is read as PostgreSQL's regclassin reads it, an identifier
/// list (an unquoted part folded to lower case, a quoted part as written, `public.` the main
/// schema): a missing relation is 42P01 'relation "<name>" does not exist', a malformed name
/// 42602 'invalid name syntax'. NULL stays NULL.
fn exec_pg_regclass(conn: &Connection, args: &[Value]) -> Result<Value> {
    let text = match args.first() {
        None | Some(Value::Null) => return Ok(Value::Null),
        Some(Value::Text(t)) => t.as_str().to_string(),
        Some(other) => match other.as_int() {
            Some(oid) => return Ok(Value::from_i64(oid)),
            None => other.to_string(),
        },
    };
    let invalid = || {
        LimboError::InvalidArgument(SqlErrorMessage::classified(
            SqlErrorKind::InvalidName,
            "invalid name syntax".to_string(),
        ))
    };
    let parts = crate::catalog::identifier_list(&text).ok_or_else(invalid)?;
    // makeRangeVarFromNameList: a third part is a database, which must be this one (0A000
    // otherwise), and a fourth is 42601 (recorded, pg18-rev-uw2-regclass.out).
    let (schema, name) = match parts.as_slice() {
        [name] => (None, name.clone()),
        [schema, name] => (Some(schema.clone()), name.clone()),
        [database, schema, name] => {
            if *database != crate::catalog::database_name(conn) {
                return Err(LimboError::InvalidArgument(SqlErrorMessage::classified(
                    SqlErrorKind::FeatureNotSupported,
                    format!("cross-database references are not implemented: \"{}\"", parts.join(".")),
                )));
            }
            (Some(schema.clone()), name.clone())
        }
        _ => {
            return Err(LimboError::InvalidArgument(SqlErrorMessage::classified(
                SqlErrorKind::SyntaxError,
                format!("improper relation name (too many dotted names): {}", parts.join(".")),
            )))
        }
    };
    // pg_catalog comes first on every search path, as PostgreSQL puts it (wire-rev-uw2 H3:
    // 'pg_class'::regclass was 42P01; PG 18.6 answers 1259); public names only user relations.
    let found = match schema.as_deref() {
        None => crate::catalog::catalog_relation_oid(&name)
            .or_else(|| crate::catalog::relation_oid(&conn.current_schema(), &name)),
        Some("pg_catalog") => crate::catalog::catalog_relation_oid(&name),
        Some("public") => crate::catalog::relation_oid(&conn.current_schema(), &name),
        Some(_) => None,
    };
    found.map(Value::from_i64).ok_or_else(|| {
        LimboError::InvalidArgument(SqlErrorMessage::classified(
            SqlErrorKind::UndefinedTable,
            format!("relation \"{}\" does not exist", parts.join(".")),
        ))
    })
}

fn exec_pg_get_indexdef(conn: &Connection, oid: i64) -> Value {
    match pg_get_indexdef(conn, oid) {
        Some(s) => Value::build_text(s),
        None => Value::Null,
    }
}

fn exec_pg_format_type(type_oid: i64, typemod: i64) -> Value {
    let type_name = match type_oid {
        16 => "boolean".to_string(),
        17 => "bytea".to_string(),
        18 => "\"char\"".to_string(),
        19 => "name".to_string(),
        20 => "bigint".to_string(),
        21 => "smallint".to_string(),
        23 => "integer".to_string(),
        25 => "text".to_string(),
        26 => "oid".to_string(),
        114 => "json".to_string(),
        700 => "real".to_string(),
        701 => "double precision".to_string(),
        1000 => "boolean[]".to_string(),
        1007 => "integer[]".to_string(),
        1009 => "text[]".to_string(),
        1022 => "double precision[]".to_string(),
        1042 => {
            if typemod > 4 {
                format!("character({})", typemod - 4)
            } else {
                "character".to_string()
            }
        }
        1043 => {
            if typemod > 4 {
                format!("character varying({})", typemod - 4)
            } else {
                "character varying".to_string()
            }
        }
        1082 => "date".to_string(),
        1083 => "time without time zone".to_string(),
        1114 => "timestamp without time zone".to_string(),
        1184 => "timestamp with time zone".to_string(),
        1186 => "interval".to_string(),
        1700 => {
            if typemod > 4 {
                let precision = ((typemod - 4) >> 16) & 0xffff;
                let scale = (typemod - 4) & 0xffff;
                format!("numeric({precision},{scale})")
            } else {
                "numeric".to_string()
            }
        }
        2205 => "regclass".to_string(),
        2206 => "regtype".to_string(),
        2278 => "void".to_string(),
        2950 => "uuid".to_string(),
        3802 => "jsonb".to_string(),
        _ => "unknown".to_string(),
    };
    Value::build_text(type_name)
}

/// Simplified to_char: formats a number with the given format pattern.
/// Supports basic PG numeric format patterns (9, 0, S, MI, FM, D, G, PR, TH, L).
fn exec_to_char(value: &Value, format: &str) -> Value {
    let num = match value {
        Value::Null => return Value::Null,
        Value::Numeric(_) => value.as_float(),
        Value::Text(t) => match t.as_str().parse::<f64>() {
            Ok(f) => f,
            Err(_) => return Value::Null,
        },
        _ => return Value::Null,
    };

    let result = pg_to_char_numeric(num, format);
    Value::build_text(result)
}

/// pg_input_is_valid(text, type) → boolean
/// Returns true if the text is valid input for the given type.
fn exec_pg_input_is_valid(input: &Value, type_name: &str) -> Value {
    let s = match input {
        Value::Text(t) => t.as_str().to_string(),
        Value::Null => return Value::Null,
        v => v.to_string(),
    };
    let valid = validate_pg_input(&s, type_name).is_none();
    Value::from_i64(if valid { 1 } else { 0 })
}

fn exec_pg_get_expr(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Text(expression)) => {
            if args[1..].iter().any(|arg| matches!(arg, Value::Null)) {
                return Ok(Value::Null);
            }

            // TursoPG stores adbin and conbin as rendered SQL, so the relation OID and pretty flag do not change the output.
            Ok(Value::Text(expression.clone()))
        }
        Some(Value::Null) => Ok(Value::Null),
        Some(_) | None => Err(LimboError::ConversionError(
            "Expected text value".to_string(),
        )),
    }
}

/// PostgreSQL's encode(bytea, format): `hex` (two lower-case digits a byte), `base64` (lines of 76
/// characters, as encode.c breaks them) or `escape` (a backslash doubled, a zero byte or one with
/// the high bit set as `\nnn` in octal, every other byte itself); NULL for a NULL argument (the
/// function is strict); any other format 22023 "unrecognized encoding". A text argument is read as
/// bytea input reads it: `\x` followed by hex digits, else its bytes. It was "no such function"
/// (42883), so a client reading a bytea column as hex could not (found by the cloud run of
/// bind_reads_each_parameter_in_its_format, 2026-10-10).
/// PostgreSQL's int2send, int4send and int8send: the integer's big-endian two's complement in 2,
/// 4 or 8 bytes, which is also PostgreSQL 18's cast of that integer type to bytea (strings.out,
/// "conversions between bytea and integer types"). A value outside the width is PostgreSQL's
/// 22003, as its int2 or int4 input refuses it.
fn exec_int_send(name: &str, value: &Value) -> Result<Value> {
    let v = match value {
        Value::Null => return Ok(Value::Null),
        other => other.as_int().ok_or_else(|| {
            LimboError::InvalidArgument(format!("{name} takes an integer").into())
        })?,
    };
    let out_of_range = |ty: &str| {
        LimboError::Constraint(SqlErrorMessage::classified(
            SqlErrorKind::NumericValueOutOfRange,
            format!("{ty} out of range"),
        ))
    };
    let bytes = match name {
        "int2send" => i16::try_from(v).map_err(|_| out_of_range("smallint"))?.to_be_bytes().to_vec(),
        "int4send" => i32::try_from(v).map_err(|_| out_of_range("integer"))?.to_be_bytes().to_vec(),
        _ => v.to_be_bytes().to_vec(),
    };
    Ok(Value::Blob(bytes))
}

fn exec_encode(args: &[Value]) -> Result<Value> {
    let (Some(data), Some(format)) = (args.first(), args.get(1)) else {
        return Err(LimboError::InvalidArgument(
            "encode takes two arguments".to_string().into(),
        ));
    };
    let format = match format {
        Value::Null => return Ok(Value::Null),
        Value::Text(t) => t.as_str().to_ascii_lowercase(),
        other => other.to_string().to_ascii_lowercase(),
    };
    let hex_input;
    let bytes: &[u8] = match data {
        Value::Null => return Ok(Value::Null),
        Value::Blob(_) => data.to_blob().unwrap_or_default(),
        Value::Text(t) => match t.as_str().strip_prefix("\\x") {
            Some(digits) => {
                hex_input = (0..digits.len() / 2)
                    .map(|i| u8::from_str_radix(&digits[2 * i..2 * i + 2], 16))
                    .collect::<std::result::Result<Vec<u8>, _>>()
                    .ok()
                    .filter(|_| digits.len() % 2 == 0)
                    .ok_or_else(|| {
                        LimboError::Constraint(SqlErrorMessage::classified(SqlErrorKind::InvalidTextRepresentation, "invalid input syntax for type bytea".to_string()))
                    })?;
                &hex_input
            }
            None => t.as_str().as_bytes(),
        },
        other => {
            hex_input = other.to_string().into_bytes();
            &hex_input
        }
    };
    let encoded = match format.as_str() {
        "hex" => bytes.iter().map(|b| format!("{b:02x}")).collect::<String>(),
        "escape" => {
            let mut out = String::with_capacity(bytes.len());
            for &b in bytes {
                match b {
                    b'\\' => out.push_str("\\\\"),
                    0 | 0x80..=0xff => out.push_str(&format!("\\{b:03o}")),
                    _ => out.push(b as char),
                }
            }
            out
        }
        "base64" => {
            const ALPHABET: &[u8; 64] =
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
            let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4 + bytes.len() / 57);
            let mut line = 0;
            for chunk in bytes.chunks(3) {
                if line == 76 {
                    out.push('\n');
                    line = 0;
                }
                let n = (u32::from(chunk[0]) << 16)
                    | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
                    | u32::from(*chunk.get(2).unwrap_or(&0));
                for i in 0..4 {
                    if i <= chunk.len() {
                        out.push(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize] as char);
                    } else {
                        out.push('=');
                    }
                }
                line += 4;
            }
            out
        }
        other => {
            return Err(LimboError::InvalidArgument(SqlErrorMessage::classified(SqlErrorKind::InvalidParameterValue, format!(
                "unrecognized encoding: \"{other}\""
            ))))
        }
    };
    Ok(Value::build_text(encoded))
}

fn user_tables_sorted(schema: &Schema) -> Vec<(&String, &Arc<Table>)> {
    let mut tables: Vec<_> = schema
        .tables
        .iter()
        .filter(|(name, table)| {
            if name.starts_with("sqlite_")
                || name.starts_with("pg_")
                || name.starts_with("pragma_")
                || name.starts_with("json_")
            {
                return false;
            }
            matches!(table.as_ref(), Table::BTree(_))
        })
        .collect();
    tables.sort_by_key(|(name, _)| *name);
    tables
}

fn ref_act_to_char(act: &RefAct) -> &'static str {
    match act {
        RefAct::NoAction => "a",
        RefAct::Restrict => "r",
        RefAct::Cascade => "c",
        RefAct::SetNull => "n",
        RefAct::SetDefault => "d",
    }
}

fn ref_act_to_sql(code: &str) -> &'static str {
    match code {
        "r" => "RESTRICT",
        "c" => "CASCADE",
        "n" => "SET NULL",
        "d" => "SET DEFAULT",
        _ => "NO ACTION",
    }
}

fn pg_get_constraintdef(conn: &Connection, target_oid: i64) -> Option<String> {
    let schema = conn.current_schema();
    let tables = user_tables_sorted(&schema);
    let num_tables = tables.len() as i64;

    let mut next_index_oid = USER_TABLE_OID_START + num_tables;
    for (table_name, _) in &tables {
        for idx in schema.get_indices(table_name) {
            if !idx.ephemeral {
                next_index_oid += 1;
            }
        }
    }

    let mut constraint_oid = next_index_oid;

    for (_, table) in &tables {
        let btree = match table.as_ref() {
            Table::BTree(bt) => bt,
            _ => continue,
        };

        let has_pk_in_unique_sets = btree.unique_sets.iter().any(|us| us.is_primary_key);
        if !has_pk_in_unique_sets && !btree.primary_key_columns.is_empty() {
            if constraint_oid == target_oid {
                let cols: Vec<String> = btree
                    .primary_key_columns
                    .iter()
                    .map(|(name, _)| crate::catalog::pg_ident(name))
                    .collect();
                return Some(format!("PRIMARY KEY ({})", cols.join(", ")));
            }
            constraint_oid += 1;
        }

        for us in &btree.unique_sets {
            if constraint_oid == target_oid {
                let col_names: Vec<String> =
                    us.columns.iter().map(|c| crate::catalog::pg_ident(&c.name)).collect();
                let kw = if us.is_primary_key {
                    "PRIMARY KEY"
                } else {
                    "UNIQUE"
                };
                return Some(format!("{kw} ({})", col_names.join(", ")));
            }
            constraint_oid += 1;
        }

        for fk in &btree.foreign_keys {
            if constraint_oid == target_oid {
                let names = |cols: &[String]| {
                    cols.iter().map(|c| crate::catalog::pg_ident(c)).collect::<Vec<_>>().join(", ")
                };
                let (child_cols, parent_cols) = (names(&fk.child_columns), names(&fk.parent_columns));
                let mut def = format!(
                    "FOREIGN KEY ({child_cols}) REFERENCES {}({parent_cols})",
                    crate::catalog::pg_ident(&fk.parent_table)
                );
                let on_update = ref_act_to_char(&fk.on_update);
                let on_delete = ref_act_to_char(&fk.on_delete);
                if on_update != "a" {
                    def.push_str(&format!(" ON UPDATE {}", ref_act_to_sql(on_update)));
                }
                if on_delete != "a" {
                    def.push_str(&format!(" ON DELETE {}", ref_act_to_sql(on_delete)));
                }
                return Some(def);
            }
            constraint_oid += 1;
        }

        for chk in &btree.check_constraints {
            if constraint_oid == target_oid {
                return Some(format!("CHECK ({})", chk.expr));
            }
            constraint_oid += 1;
        }
    }

    None
}

fn pg_get_indexdef(conn: &Connection, target_oid: i64) -> Option<String> {
    let schema = conn.current_schema();
    let tables = user_tables_sorted(&schema);
    let num_tables = tables.len() as i64;

    let mut index_oid = USER_TABLE_OID_START + num_tables;
    for (table_name, _) in &tables {
        for idx in schema.get_indices(table_name) {
            if idx.ephemeral {
                continue;
            }
            if index_oid == target_oid {
                let cols: Vec<String> = idx
                    .columns
                    .iter()
                    .map(|col| {
                        if let Some(expr) = &col.expr {
                            expr.to_string()
                        } else {
                            crate::catalog::pg_ident(&col.name)
                        }
                    })
                    .collect();
                let mut def =
                    crate::catalog::pg_index_definition(idx.unique, &idx.name, table_name, &cols);
                if let Some(where_clause) = &idx.where_clause {
                    def.push_str(&format!(" WHERE {where_clause}"));
                }
                return Some(def);
            }
            index_oid += 1;
        }
    }

    None
}

/// Validate input for a PostgreSQL type, returning error info if invalid.
///
/// Returns `None` for valid input, or `Some((message, sql_error_code))` for invalid input.
/// Used by both `pg_input_error_info` (table-valued) and `pg_input_is_valid` (scalar).


/// Whether numeric `text` (a constant's spelling, or numeric input: blanks, a sign, 'NaN',
/// 'Infinity') is past numeric(precision, scale): rounded half away from zero to `scale` places,
/// it reaches 10^(precision - scale), a negative scale or one past the precision included, as
/// apply_typmod judges it (recorded, rec31-constant-numeric-cast, artie 8ceb58f30a). None for a
/// spelling it does not read (a 0x prefix, an underscore), which the caller leaves alone.
/// The remainder of decimal `x` by decimal `y`, each a plain decimal's text (an optional sign,
/// digits and one point), computed exactly as scaled integers: Some(Ok(text)), Some(Err(())) for a
/// zero divisor, None for a spelling it does not read or past an i128 ([`pg_numeric_mod`]'s float
/// fallback then answers).
fn numeric_remainder(x: &str, y: &str) -> Option<Result<String, ()>> {
    let read = |t: &str| -> Option<(i128, u32)> {
        let (negative, body) = match t.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, t.strip_prefix('+').unwrap_or(t)),
        };
        let (whole, fraction) = body.split_once('.').unwrap_or((body, ""));
        if (whole.is_empty() && fraction.is_empty()) || !whole.bytes().chain(fraction.bytes()).all(|b| b.is_ascii_digit()) {
            return None;
        }
        let digits = format!("{whole}{fraction}");
        let magnitude: i128 = if digits.is_empty() { 0 } else { digits.parse().ok()? };
        Some((if negative { -magnitude } else { magnitude }, fraction.len() as u32))
    };
    let ((a, sa), (b, sb)) = (read(x)?, read(y)?);
    if b == 0 {
        return Some(Err(()));
    }
    let scale = sa.max(sb);
    let a = a.checked_mul(10i128.checked_pow(scale - sa)?)?;
    let b = b.checked_mul(10i128.checked_pow(scale - sb)?)?;
    let r = a % b;
    let unit = 10i128.checked_pow(scale)?;
    let sign = if r < 0 { "-" } else { "" };
    let (q, f) = (r.abs() / unit, r.abs() % unit);
    Some(Ok(if scale == 0 { format!("{sign}{q}") } else { format!("{sign}{q}.{f:0width$}", width = scale as usize) }))
}

pub(crate) fn numeric_overflows(text: &str, precision: i32, scale: i32) -> Option<bool> {
    let t = text.trim();
    let unsigned = t.strip_prefix(['-', '+']).unwrap_or(t);
    if unsigned.eq_ignore_ascii_case("nan") {
        return Some(false);
    }
    if unsigned.eq_ignore_ascii_case("infinity") || unsigned.eq_ignore_ascii_case("inf") {
        return Some(true);
    }
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(p) => (&unsigned[..p], unsigned[p + 1..].parse::<i64>().ok()?),
        None => (unsigned, 0),
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
        return Some(false);
    }
    // The value times 10^scale is digits x 10^shift; its rounded integer's digit count decides.
    let shift = exponent.saturating_sub(fraction.len() as i64).saturating_add(i64::from(scale));
    let n = digits.len() as i64;
    let rounded = if shift >= 0 {
        n.saturating_add(shift)
    } else {
        let kept = n + shift;
        if kept < 0 {
            0
        } else if kept == 0 {
            i64::from(digits.as_bytes()[0] >= b'5')
        } else {
            let up = digits.as_bytes()[kept as usize] >= b'5';
            if up && digits[..kept as usize].bytes().all(|b| b == b'9') { kept + 1 } else { kept }
        }
    };
    Some(rounded > i64::from(precision))
}

/// A constant's value rounded half away from zero to `scale` places, then to an integer, as a cast
/// to numeric(p, s) then the assignment to an integer column round it (1.45 at scale 1 is 1.5,
/// then 2; recorded, rec32-cast-constant-assign, artie c2fe0580f5). None past 30 places or for a
/// spelling [`round_decimal`] does not read.
pub(crate) fn round_decimal_at(text: &str, scale: i32) -> Option<i128> {
    if !(-30..=30).contains(&scale) {
        return None;
    }
    let t = text.trim();
    let (mantissa, exponent) = match t.find(['e', 'E']) {
        Some(p) => (&t[..p], t[p + 1..].parse::<i64>().ok()?),
        None => (t, 0),
    };
    // The value times 10^scale, rounded: then back by 10^scale, rounded again.
    let scaled = round_decimal(&format!("{mantissa}e{}", exponent + i64::from(scale)))?;
    if scaled.abs() == i128::MAX {
        // Past 30 digits at this scale: judged only when the integer part alone is past every
        // integer type.
        return round_decimal(text).filter(|v| v.abs() >= 1i128 << 63).map(|_| i128::MAX);
    }
    if scale <= 0 {
        return scaled.checked_mul(10i128.pow(scale.unsigned_abs())).or(Some(i128::MAX));
    }
    let unit = 10i128.pow(scale as u32);
    let (q, r) = (scaled / unit, scaled % unit);
    Some(if r.abs() * 2 >= unit { q + r.signum() } else { q })
}

/// int2in/int4in/int8in's refusal of `input`, if any, with PostgreSQL's message.
fn pg_int_input_error(input: &str, bits: u32, name: &str) -> Option<(String, SqlErrorKind)> {
    match pg_strtoint(input, bits) {
        PgInt::Value(_) => None,
        PgInt::Invalid => Some((format!("invalid input syntax for type {name}: \"{input}\""), SqlErrorKind::InvalidTextRepresentation)),
        PgInt::OutOfRange => Some((format!("value \"{input}\" is out of range for type {name}"), SqlErrorKind::NumericValueOutOfRange)),
    }
}

pub(crate) use turso_pg_parser::pg_input::{pg_strtoint, round_decimal, PgInt};

pub(crate) fn validate_pg_input(input: &str, type_name: &str) -> Option<(String, SqlErrorKind)> {
    let trimmed = input.trim();

    // Extract base type and optional length modifier, e.g. "varchar(4)" → ("varchar", Some(4))
    let (base_type, type_mod) = match type_name.find('(') {
        Some(pos) => {
            let base = type_name[..pos].trim();
            let mod_str = type_name[pos + 1..].trim_end_matches(')').trim();
            let modifier = mod_str.parse::<usize>().ok();
            (base.to_lowercase(), modifier)
        }
        None => (type_name.to_lowercase(), None),
    };

    match base_type.as_str() {
        "bool" | "boolean" => {
            let lower = trimmed.to_lowercase();
            let valid = matches!(
                lower.as_str(),
                "t" | "true" | "y" | "yes" | "on" | "1" | "f" | "false" | "n" | "no" | "off" | "0"
            );
            if valid {
                None
            } else {
                Some((
                    format!("invalid input syntax for type boolean: \"{input}\""),
                    SqlErrorKind::InvalidTextRepresentation,
                ))
            }
        }
        "int2" | "smallint" => pg_int_input_error(input, 16, "smallint"),
        "int4" | "integer" | "int" => pg_int_input_error(input, 32, "integer"),
        "int8" | "bigint" => pg_int_input_error(input, 64, "bigint"),
        "float4" | "real" => match trimmed.parse::<f32>() {
            Ok(v) if v.is_infinite() => Some((
                format!("value \"{input}\" is out of range for type real"),
                SqlErrorKind::NumericValueOutOfRange,
            )),
            Ok(_) => None,
            Err(_) => Some((
                format!("invalid input syntax for type real: \"{input}\""),
                SqlErrorKind::InvalidTextRepresentation,
            )),
        },
        "float8" | "double precision" => match trimmed.parse::<f64>() {
            Ok(v) if v.is_infinite() => Some((
                format!("value \"{input}\" is out of range for type double precision"),
                SqlErrorKind::NumericValueOutOfRange,
            )),
            Ok(_) => None,
            Err(_) => Some((
                format!("invalid input syntax for type double precision: \"{input}\""),
                SqlErrorKind::InvalidTextRepresentation,
            )),
        },
        "numeric" | "decimal" => match trimmed.parse::<f64>() {
            Ok(v) if v.is_nan() || v.is_infinite() => Some((
                format!("invalid input syntax for type numeric: \"{input}\""),
                SqlErrorKind::InvalidTextRepresentation,
            )),
            Ok(_) => None,
            Err(_) => Some((
                format!("invalid input syntax for type numeric: \"{input}\""),
                SqlErrorKind::InvalidTextRepresentation,
            )),
        },
        "text" => None,
        "varchar" | "character varying" => {
            if let Some(max_len) = type_mod {
                if trimmed.chars().count() > max_len {
                    return Some((
                        format!("value too long for type character varying({max_len})"),
                        SqlErrorKind::StringDataRightTruncation,
                    ));
                }
            }
            None
        }
        "char" | "character" => {
            if let Some(max_len) = type_mod {
                if trimmed.chars().count() > max_len {
                    return Some((
                        format!("value too long for type character({max_len})"),
                        SqlErrorKind::StringDataRightTruncation,
                    ));
                }
            }
            None
        }
        "uuid" => {
            let hex: String = trimmed.chars().filter(|c| *c != '-').collect();
            if hex.len() != 32 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
                Some((
                    format!("invalid input syntax for type uuid: \"{input}\""),
                    SqlErrorKind::InvalidTextRepresentation,
                ))
            } else {
                None
            }
        }
        "date" => {
            // Accept YYYY-MM-DD
            let parts: Vec<&str> = trimmed.split('-').collect();
            if parts.len() == 3
                && parts[0].parse::<i32>().is_ok()
                && parts[1].parse::<u32>().is_ok_and(|m| (1..=12).contains(&m))
                && parts[2].parse::<u32>().is_ok_and(|d| (1..=31).contains(&d))
            {
                None
            } else {
                Some((
                    format!("invalid input syntax for type date: \"{input}\""),
                    SqlErrorKind::InvalidDatetimeFormat,
                ))
            }
        }
        "timestamp" | "timestamp without time zone" => {
            // Accept YYYY-MM-DD HH:MM:SS[.fff]
            if parse_timestamp_prefix(trimmed).is_some() {
                None
            } else {
                Some((
                    format!("invalid input syntax for type timestamp: \"{input}\""),
                    SqlErrorKind::InvalidDatetimeFormat,
                ))
            }
        }
        "timestamptz" | "timestamp with time zone" => {
            // Accept YYYY-MM-DD HH:MM:SS[.fff][+/-HH[:MM]]
            let (base, _tz) = match trimmed.rfind('+') {
                Some(pos) if pos > 10 => (&trimmed[..pos], Some(&trimmed[pos..])),
                _ => match trimmed.rfind('-') {
                    Some(pos) if pos > 10 => (&trimmed[..pos], Some(&trimmed[pos..])),
                    _ => (trimmed, None),
                },
            };
            if parse_timestamp_prefix(base).is_some() {
                None
            } else {
                Some((
                    format!("invalid input syntax for type timestamp with time zone: \"{input}\""),
                    SqlErrorKind::InvalidDatetimeFormat,
                ))
            }
        }
        "time" | "time without time zone" => {
            // Accept HH:MM:SS[.fff]
            let time_part = trimmed.split('.').next().unwrap_or(trimmed);
            let parts: Vec<&str> = time_part.split(':').collect();
            if parts.len() >= 2
                && parts.len() <= 3
                && parts[0].parse::<u32>().is_ok_and(|h| (0..=23).contains(&h))
                && parts[1].parse::<u32>().is_ok_and(|m| (0..=59).contains(&m))
                && (parts.len() < 3 || parts[2].parse::<u32>().is_ok_and(|s| (0..=59).contains(&s)))
            {
                None
            } else {
                Some((
                    format!("invalid input syntax for type time: \"{input}\""),
                    SqlErrorKind::InvalidDatetimeFormat,
                ))
            }
        }
        "json" | "jsonb" => {
            if is_valid_json(trimmed) {
                None
            } else {
                let type_label = if base_type == "jsonb" {
                    "jsonb"
                } else {
                    "json"
                };
                Some((
                    format!("invalid input syntax for type {type_label}: \"{input}\""),
                    SqlErrorKind::InvalidTextRepresentation,
                ))
            }
        }
        "bytea" => {
            // Accept \x hex format
            if let Some(hex) = trimmed.strip_prefix("\\x") {
                if hex.len() % 2 == 0 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
                    None
                } else {
                    Some((
                        format!("invalid input syntax for type bytea: \"{input}\""),
                        SqlErrorKind::InvalidTextRepresentation,
                    ))
                }
            } else {
                // Plain text is valid bytea input (escape format)
                None
            }
        }
        "inet" | "cidr" => {
            // Accept IP address with optional /prefix
            let addr_part = trimmed.split('/').next().unwrap_or(trimmed);
            if addr_part.parse::<std::net::IpAddr>().is_ok() {
                // If there's a prefix, validate it
                if let Some(prefix_str) = trimmed.split('/').nth(1) {
                    if prefix_str.parse::<u8>().is_err() {
                        return Some((
                            format!("invalid input syntax for type {base_type}: \"{input}\""),
                            SqlErrorKind::InvalidTextRepresentation,
                        ));
                    }
                }
                None
            } else {
                Some((
                    format!("invalid input syntax for type {base_type}: \"{input}\""),
                    SqlErrorKind::InvalidTextRepresentation,
                ))
            }
        }
        "macaddr" => {
            let parts: Vec<&str> = trimmed.split(':').collect();
            if parts.len() == 6
                && parts
                    .iter()
                    .all(|p| p.len() == 2 && p.chars().all(|c| c.is_ascii_hexdigit()))
            {
                None
            } else {
                Some((
                    format!("invalid input syntax for type macaddr: \"{input}\""),
                    SqlErrorKind::InvalidTextRepresentation,
                ))
            }
        }
        "oid" => {
            if trimmed.parse::<u32>().is_ok() {
                None
            } else {
                Some((
                    format!("invalid input syntax for type oid: \"{input}\""),
                    SqlErrorKind::InvalidTextRepresentation,
                ))
            }
        }
        _ => Some((
            format!("type \"{type_name}\" does not exist"),
            SqlErrorKind::UndefinedObject,
        )),
    }
}

/// Parse a YYYY-MM-DD HH:MM:SS[.fff] prefix, returning Some(()) if valid.
fn parse_timestamp_prefix(s: &str) -> Option<()> {
    let parts: Vec<&str> = s.splitn(2, [' ', 'T']).collect();
    if parts.len() != 2 {
        return None;
    }
    // Validate date part
    let date_parts: Vec<&str> = parts[0].split('-').collect();
    if date_parts.len() != 3
        || date_parts[0].parse::<i32>().is_err()
        || !date_parts[1]
            .parse::<u32>()
            .is_ok_and(|m| (1..=12).contains(&m))
        || !date_parts[2]
            .parse::<u32>()
            .is_ok_and(|d| (1..=31).contains(&d))
    {
        return None;
    }
    // Validate time part (strip fractional seconds)
    let time_str = parts[1].split('.').next().unwrap_or(parts[1]);
    let time_parts: Vec<&str> = time_str.split(':').collect();
    if time_parts.len() < 2
        || time_parts.len() > 3
        || !time_parts[0]
            .parse::<u32>()
            .is_ok_and(|h| (0..=23).contains(&h))
        || !time_parts[1]
            .parse::<u32>()
            .is_ok_and(|m| (0..=59).contains(&m))
    {
        return None;
    }
    if time_parts.len() == 3
        && !time_parts[2]
            .parse::<u32>()
            .is_ok_and(|s| (0..=59).contains(&s))
    {
        return None;
    }
    Some(())
}

/// Minimal JSON validation without requiring serde_json.
fn is_valid_json(s: &str) -> bool {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return false;
    }
    // Quick structural check: must start with {, [, ", digit, true, false, or null
    let first = trimmed.as_bytes()[0];
    match first {
        b'{' => trimmed.ends_with('}') && validate_json_braces(trimmed),
        b'[' => trimmed.ends_with(']') && validate_json_braces(trimmed),
        b'"' => trimmed.len() >= 2 && trimmed.ends_with('"'),
        b't' => trimmed == "true",
        b'f' => trimmed == "false",
        b'n' => trimmed == "null",
        b'0'..=b'9' | b'-' => trimmed.parse::<f64>().is_ok(),
        _ => false,
    }
}

/// Check that braces/brackets are balanced in a JSON string.
fn validate_json_braces(s: &str) -> bool {
    let mut stack = Vec::new();
    let mut in_string = false;
    let mut escape = false;

    for ch in s.chars() {
        if escape {
            escape = false;
            continue;
        }
        if ch == '\\' && in_string {
            escape = true;
            continue;
        }
        if ch == '"' {
            in_string = !in_string;
            continue;
        }
        if in_string {
            continue;
        }
        match ch {
            '{' => stack.push('}'),
            '[' => stack.push(']'),
            '}' | ']' => {
                if stack.pop() != Some(ch) {
                    return false;
                }
            }
            _ => {}
        }
    }
    stack.is_empty() && !in_string
}

/// Format a number using PG's to_char numeric format patterns.
fn pg_to_char_numeric(num: f64, format: &str) -> String {
    let is_negative = num < 0.0;
    let abs_num = num.abs();

    // Parse format string for flags
    let upper_fmt = format.to_uppercase();
    let fm = upper_fmt.contains("FM"); // fill mode (suppress padding)
    let has_pr = upper_fmt.contains("PR"); // angle brackets for negative
    let has_s = upper_fmt.contains('S'); // sign
    let has_mi = upper_fmt.starts_with("MI") || upper_fmt.ends_with("MI");

    // Count digit positions
    let mut integer_digits = 0;
    let mut decimal_digits = 0;
    let mut leading_zeros = 0;
    let mut seen_dot = false;

    for ch in upper_fmt.chars() {
        match ch {
            '9' => {
                if seen_dot {
                    decimal_digits += 1;
                } else {
                    integer_digits += 1;
                }
            }
            '0' => {
                if seen_dot {
                    decimal_digits += 1;
                } else {
                    integer_digits += 1;
                    leading_zeros += 1;
                }
            }
            'D' | '.' => seen_dot = true,
            _ => {}
        }
    }

    if integer_digits == 0 && decimal_digits == 0 {
        return format!("{num}");
    }

    // Format the number
    let formatted = if decimal_digits > 0 {
        let prec = decimal_digits;
        format!("{abs_num:.prec$}")
    } else {
        let int_val = abs_num as i64;
        format!("{int_val}")
    };

    // Split into integer and decimal parts
    let parts: Vec<&str> = formatted.split('.').collect();
    let int_part = parts[0];
    let dec_part = if parts.len() > 1 { parts[1] } else { "" };

    // Pad integer part
    let padded_int = if !fm {
        let width = integer_digits.max(int_part.len());
        if leading_zeros > 0 {
            format!("{int_part:0>width$}")
        } else {
            format!("{int_part:>width$}")
        }
    } else {
        int_part.to_string()
    };

    // Build result
    let mut result = if decimal_digits > 0 {
        format!("{padded_int}.{dec_part}")
    } else {
        padded_int
    };

    // Add sign
    if has_pr {
        result = if is_negative {
            format!("<{result}>")
        } else {
            format!(" {result} ")
        };
    } else if has_s {
        let sign_pos = upper_fmt.find('S').unwrap_or(0);
        let sign = if is_negative { "-" } else { "+" };
        if sign_pos == 0 {
            result = format!("{sign}{result}");
        } else {
            result = format!("{result}{sign}");
        }
    } else if has_mi {
        if is_negative {
            result = format!("{result}-");
        } else {
            result = format!("{result} ");
        }
    } else if is_negative {
        result = format!("-{result}");
    } else {
        result = format!(" {result}");
    }

    result
}
