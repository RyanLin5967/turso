use super::*;

/// Map an AST operator to the string representation used in custom type operator definitions.
pub(super) fn operator_to_str(op: &ast::Operator) -> Option<&'static str> {
    match op {
        ast::Operator::Add => Some("+"),
        ast::Operator::Subtract => Some("-"),
        ast::Operator::Multiply => Some("*"),
        ast::Operator::Divide => Some("/"),
        ast::Operator::Modulus => Some("%"),
        ast::Operator::Less => Some("<"),
        ast::Operator::LessEquals => Some("<="),
        ast::Operator::Greater => Some(">"),
        ast::Operator::GreaterEquals => Some(">="),
        ast::Operator::Equals => Some("="),
        ast::Operator::NotEquals => Some("!="),
        _ => None,
    }
}

/// Emit bytecode for a resolved custom type operator call.
/// Handles argument swapping and result negation.
///
/// The operands reach the operator as they are: a custom-type column is read decoded (its
/// user-facing value, `emit_user_facing_column_value`), and the other operand is a value of the
/// type's input, which the type's operator functions take as given (numeric's take integer, real,
/// text and blob). It is not encoded with the column's parameters, which would truncate or refuse
/// it as if it were stored (engine review 14 HIGH 1: 6b (a) did so to every bound parameter, in
/// arithmetic too, and sent a NULL one into an ENCODE that raises). Mutant
/// `operand_keeps_column_typmod` (test builds only): encoded with the column's parameters, as
/// literals were before.
pub(super) fn emit_custom_type_operator(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    e1: &ast::Expr,
    e2: &ast::Expr,
    resolved: &ResolvedOperator,
    resolver: &Resolver,
) -> Result<usize> {
    let func = resolver
        .resolve_function(&resolved.func_name, 2)?
        .ok_or_else(|| {
            LimboError::InternalError(format!("function not found: {}", resolved.func_name))
        })?;
    let (first, second) = if resolved.swap_args {
        (e2, e1)
    } else {
        (e1, e2)
    };

    // The mutant's encoding uses separate registers for the function call arguments:
    // translate_expr may place literals in preamble registers (constant optimization), and
    // encoding in-place would clobber that register, breaking subsequent loop iterations.
    let encoded = resolved
        .encode_info
        .as_ref()
        .filter(|_| crate::branch::store::fe_mutant("operand_keeps_column_typmod"));
    let func_start = if let Some(encode_info) = encoded {
        if let Some(encode_expr) = encode_info.type_def.encode() {
            // Translate operands into temporary registers first.
            let tmp1 = program.alloc_register();
            let tmp2 = program.alloc_register();
            translate_expr(program, referenced_tables, first, tmp1, resolver)?;
            translate_expr(program, referenced_tables, second, tmp2, resolver)?;

            // Determine which tmp holds the literal and which holds the column.
            let (lit_tmp, col_tmp) = match encode_info.which {
                EncodeArg::First if resolved.swap_args => (tmp2, tmp1),
                EncodeArg::First => (tmp1, tmp2),
                EncodeArg::Second if resolved.swap_args => (tmp1, tmp2),
                EncodeArg::Second => (tmp2, tmp1),
            };

            // Allocate fresh contiguous registers for the function call.
            let func_args = program.alloc_registers(2);
            // The literal goes in the same position it occupied in arg layout.
            let (lit_dst, col_dst) = match encode_info.which {
                EncodeArg::First if resolved.swap_args => (func_args + 1, func_args),
                EncodeArg::First => (func_args, func_args + 1),
                EncodeArg::Second if resolved.swap_args => (func_args, func_args + 1),
                EncodeArg::Second => (func_args + 1, func_args),
            };

            // Copy column value as-is.
            program.emit_insn(Insn::Copy {
                src_reg: col_tmp,
                dst_reg: col_dst,
                extra_amount: 0,
            });
            // Encode the operand into the fresh function arg slot.
            emit_type_expr(
                program,
                encode_expr,
                lit_tmp,
                lit_dst,
                &encode_info.column,
                &encode_info.type_def,
                resolver,
            )?;
            func_args
        } else {
            // Type has no encode expression; translate directly into arg slots.
            let arg_reg = program.alloc_registers(2);
            translate_expr(program, referenced_tables, first, arg_reg, resolver)?;
            translate_expr(program, referenced_tables, second, arg_reg + 1, resolver)?;
            arg_reg
        }
    } else {
        // The operands as they are, in the call's argument slots.
        let arg_reg = program.alloc_registers(2);
        translate_expr(program, referenced_tables, first, arg_reg, resolver)?;
        translate_expr(program, referenced_tables, second, arg_reg + 1, resolver)?;
        arg_reg
    };

    let result_reg = program.alloc_register();
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: func_start,
        dest: result_reg,
        func: FuncCtx { func, arg_count: 2 },
    });
    if resolved.negate {
        program.emit_insn(Insn::Not {
            reg: result_reg,
            dest: result_reg,
        });
    }
    Ok(result_reg)
}

/// Info about a column with a custom type, extracted from an expression.
pub(super) struct ExprCustomTypeInfo {
    type_name: String,
    column: Column,
    type_def: Arc<TypeDef>,
}

/// If the expression is a column reference to a custom type, return the type info.
pub(super) fn expr_custom_type_info(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Option<ExprCustomTypeInfo> {
    if let ast::Expr::Column {
        table: table_ref_id,
        column,
        ..
    } = expr
    {
        let tables = referenced_tables?;
        let (_, table) = tables.find_table_by_internal_id(*table_ref_id)?;
        let col = table.get_column_at(*column)?;
        let type_name = &col.ty_str;
        let type_def = resolver
            .schema()
            .get_type_def(type_name, table.is_strict())?;
        return Some(ExprCustomTypeInfo {
            type_name: type_name.to_lowercase(),
            column: col.clone(),
            type_def: Arc::clone(type_def),
        });
    }
    None
}

/// Get the effective type name of a literal expression.
pub(super) fn literal_type_name(expr: &ast::Expr) -> Option<&'static str> {
    match expr {
        ast::Expr::Literal(lit) => match lit {
            ast::Literal::Numeric(s) => {
                if s.contains('.') || s.contains('e') || s.contains('E') {
                    Some("real")
                } else {
                    Some("integer")
                }
            }
            ast::Literal::String(_) => Some("text"),
            ast::Literal::Blob(_) => Some("blob"),
            ast::Literal::True | ast::Literal::False => Some("integer"),
            _ => None,
        },
        _ => None,
    }
}

/// Check if a literal type is compatible with a custom type's value input type.
/// "any" matches everything; otherwise exact match (case-insensitive).
pub(super) fn literal_compatible_with_value_type(
    literal_type: &str,
    value_input_type: &str,
) -> bool {
    value_input_type.eq_ignore_ascii_case("any")
        || literal_type.eq_ignore_ascii_case(value_input_type)
}

/// Which operand of a binary expression is the one that is not the custom type column.
pub(super) enum EncodeArg {
    /// The first argument (e1 is the operand, e2 is the custom type column)
    First,
    /// The second argument (e1 is the custom type column, e2 is the operand)
    Second,
}

/// The column an operand meets, and which operand it is (only mutant
/// `operand_keeps_column_typmod` encodes the operand with it).
pub(super) struct OperatorEncodeInfo {
    column: Column,
    type_def: Arc<TypeDef>,
    which: EncodeArg,
}

/// Result of resolving a custom type operator. May be a direct match or derived
/// from `<` and `=` operators (e.g. `>` is derived as swap_args + `<`).
pub(super) struct ResolvedOperator {
    func_name: String,
    swap_args: bool,
    negate: bool,
    /// When one operand is not a custom type column: which, and the column it meets.
    encode_info: Option<OperatorEncodeInfo>,
}

/// Find a custom type operator function for a binary expression.
///
/// Operators fire when:
/// 1. Both operands are columns of the same custom type, OR
/// 2. One operand is a custom type column and the other a constant operand
///    (`operand_compatible`): a literal whose type is compatible with the custom
///    type's `value` input type, a bound parameter, or another constant expression.
///
/// Both arguments reach the function as user-facing values: the column decoded, the
/// operand as given (`emit_custom_type_operator`).
pub(super) fn find_custom_type_operator(
    e1: &ast::Expr,
    e2: &ast::Expr,
    op: &ast::Operator,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Option<ResolvedOperator> {
    let op_str = operator_to_str(op)?;
    let lhs_info = expr_custom_type_info(e1, referenced_tables, resolver);
    let rhs_info = expr_custom_type_info(e2, referenced_tables, resolver);

    // Try to find a direct or derived operator match on a type definition.
    let find_in_type_def = |type_def: &TypeDef| -> Option<(String, bool, bool)> {
        // Direct match: just check op symbol (no right_type constraint)
        for op_def in type_def.operators() {
            if op_def.op == op_str {
                // Naked operator (func_name = None): fall through to standard comparison
                let func_name = op_def.func_name.as_ref()?;
                return Some((func_name.clone(), false, false));
            }
        }

        // Derive missing operators from < and =
        let find_op = |sym: &str| -> Option<String> {
            type_def
                .operators()
                .iter()
                .find(|o| o.op == sym)
                .and_then(|o| o.func_name.clone())
        };

        match *op {
            // a > b  →  lt(b, a)
            ast::Operator::Greater => find_op("<").map(|f| (f, true, false)),
            // a >= b  →  NOT lt(a, b)
            ast::Operator::GreaterEquals => find_op("<").map(|f| (f, false, true)),
            // a <= b  →  NOT lt(b, a)
            ast::Operator::LessEquals => find_op("<").map(|f| (f, true, true)),
            // a != b  →  NOT eq(a, b)
            ast::Operator::NotEquals => find_op("=").map(|f| (f, false, true)),
            _ => None,
        }
    };

    // Case 1: Both operands are custom type columns of the SAME type.
    if let (Some(ref lhs), Some(ref rhs)) = (&lhs_info, &rhs_info) {
        if lhs.type_name == rhs.type_name {
            if let Some((func_name, swap_args, negate)) = find_in_type_def(&lhs.type_def) {
                return Some(ResolvedOperator {
                    func_name,
                    swap_args,
                    negate,
                    encode_info: None,
                });
            }
        }
        // Different custom types: fall through to standard operator.
        return None;
    }

    // Case 2: LHS is custom type, RHS is a constant operand.
    if let Some(ref lhs) = lhs_info {
        if operand_compatible(e2, lhs.type_def.value_input_type(), resolver) {
            if let Some((func_name, swap_args, negate)) = find_in_type_def(&lhs.type_def) {
                return Some(ResolvedOperator {
                    func_name,
                    swap_args,
                    negate,
                    encode_info: Some(OperatorEncodeInfo {
                        column: lhs.column.clone(),
                        type_def: lhs.type_def.clone(),
                        which: EncodeArg::Second,
                    }),
                });
            }
        }
    }

    // Case 3: RHS is custom type, LHS is a constant operand (reversed).
    if let Some(ref rhs) = rhs_info {
        if operand_compatible(e1, rhs.type_def.value_input_type(), resolver) {
            if let Some((func_name, swap_args, negate)) = find_in_type_def(&rhs.type_def) {
                return Some(ResolvedOperator {
                    func_name,
                    swap_args,
                    negate,
                    encode_info: Some(OperatorEncodeInfo {
                        column: rhs.column.clone(),
                        type_def: rhs.type_def.clone(),
                        which: EncodeArg::First,
                    }),
                });
            }
        }
    }

    None
}

/// The function a custom type's '=' calls: `find_custom_type_operator`'s direct match, the type's
/// first '=' operator (None when that one is naked, or when there is none).
pub(crate) fn type_eq_function(type_def: &TypeDef) -> Option<&str> {
    type_def
        .operators()
        .iter()
        .find(|op_def| op_def.op == "=")
        .and_then(|op_def| op_def.func_name.as_deref())
}

/// The '=' function that checks an index seek on a column of this type (engine review 16 HIGH 2):
/// for a type with a function '=', the seek key's ENCODE runs in a catch region and must round-trip
/// under that function, or the seek is empty (`main_loop/seek.rs`), and the equality's WHERE term
/// stays unconsumed, so the type's operator re-checks every row the seek returns
/// (`optimizer::mark_seek_constraints_consumed`): a seek answers as a scan does. None for a type
/// whose '=' is naked or absent: its seek keys by the encoding alone, as before. Mutant
/// `seek_key_encodes_raising` (test builds only): None for every type, so the key is ENCODEd in
/// place, raising and losing precision, and the term is consumed.
pub(crate) fn seek_key_eq_function(type_def: &TypeDef) -> Option<&str> {
    let eq = type_eq_function(type_def);
    if eq.is_none() || crate::branch::store::fe_mutant("seek_key_encodes_raising") {
        return None;
    }
    eq
}

/// Whether `expr` is passed to an operator of a custom type whose `value` input type is
/// `value_input_type`: a literal of a compatible type, or any other constant operand (a bound
/// parameter, a negated or cast literal; `Optimizable::is_constant`), whose value's type is known
/// only when it runs (fastest-wire, wire review 2 item 3: a parameter got the plain comparison, so
/// `code = $1` from every extended-protocol client missed what the literal finds; engine review 14
/// HIGH 1: chosen as constant, not from a list). Mutant `param_skips_type_operator` (test builds
/// only): a parameter is not one, as before.
fn operand_compatible(expr: &ast::Expr, value_input_type: &str, resolver: &Resolver) -> bool {
    if let ast::Expr::Literal(_) = expr {
        return literal_type_name(expr)
            .is_some_and(|t| literal_compatible_with_value_type(t, value_input_type));
    }
    if matches!(expr, ast::Expr::Variable(_))
        && crate::branch::store::fe_mutant("param_skips_type_operator")
    {
        return false;
    }
    expr.is_constant(resolver)
}

/// Evaluate an expression-index expression in a DML context (INSERT/UPDATE/UPSERT).
///
/// Shared logic: decode custom-type column registers into temps (so the
/// expression sees user-facing values), build a `SelfTableContext::ForDML`,
/// and translate the expression.
///
/// The caller must:
/// 1. Clone the expression from `idx_col.expr`
/// 2. Build the initial `column_regs` mapping (before decode)
///
/// The expression is resolved via `resolve_gencol_expr_columns` and custom-type
/// columns are decoded in-place in `column_regs`.
pub(crate) fn emit_dml_expr_index_value(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    mut expr: ast::Expr,
    columns: &[Column],
    column_regs: &mut [usize],
    table: &Arc<BTreeTable>,
    dest_reg: usize,
) -> Result<()> {
    crate::schema::resolve_gencol_expr_columns(&mut expr, columns)?;

    let is_strict = table.is_strict;
    for (i, col) in columns.iter().enumerate() {
        if col.is_rowid_alias() {
            continue;
        }
        if let Some(type_def) = resolver.schema().get_type_def(&col.ty_str, is_strict) {
            if type_def.decode().is_some() {
                let src_reg = column_regs[i];
                let tmp = program.alloc_register();
                emit_user_facing_column_value(program, src_reg, tmp, col, is_strict, resolver)?;
                column_regs[i] = tmp;
            }
        }
    }

    let pairs = columns.iter().zip(column_regs.iter().copied());
    let ctx = SelfTableContext::ForDML {
        dml_ctx: DmlColumnContext::from_column_reg_mapping(pairs),
        table: Arc::clone(table),
    };
    resolver.with_self_table_context(program, Some(&ctx), |program, _| {
        translate_expr(program, None, &expr, dest_reg, resolver)?;
        Ok(())
    })?;
    Ok(())
}

/// Emit bytecode that transforms a stored column value into its user-facing
/// representation.
///
/// For regular columns this is a simple copy (or no-op when source == dest).
/// For custom type columns with a DECODE function the decode expression is
/// applied, converting the internal storage form back to the value the user
/// expects to see.
///
/// Every code path that surfaces a stored column value to the user — SELECT,
/// RETURNING, trigger OLD/NEW — should go through this function so decode
/// logic lives in one place.
pub(crate) fn emit_user_facing_column_value(
    program: &mut ProgramBuilder,
    source_reg: usize,
    dest_reg: usize,
    column: &Column,
    is_strict: bool,
    resolver: &Resolver,
) -> Result<()> {
    if source_reg != dest_reg {
        program.emit_insn(Insn::Copy {
            src_reg: source_reg,
            dst_reg: dest_reg,
            extra_amount: 0,
        });
    }
    // Array columns: pass through raw record blob. ArrayDecode is emitted
    // at display time (ResultRow) so that functions/subscripts see raw blobs.
    if column.is_array() {
        return Ok(());
    }
    if let Ok(Some(resolved)) = resolver.schema().resolve_type(&column.ty_str, is_strict) {
        let skip_label = program.allocate_label();
        program.emit_insn(Insn::IsNull {
            reg: dest_reg,
            target_pc: skip_label,
        });

        // Apply decode in reverse order (parent/ancestor first, then child)
        for td in resolved.chain.iter().rev() {
            if let Some(decode_expr) = td.decode() {
                emit_type_expr(
                    program,
                    decode_expr,
                    dest_reg,
                    dest_reg,
                    column,
                    td,
                    resolver,
                )?;
            }
        }

        program.preassign_label_to_next_insn(skip_label);
    }
    Ok(())
}

/// Emit domain constraint checks for CAST(expr AS domain).
/// Validates NOT NULL and CHECK constraints from the domain type chain.
pub(super) fn emit_domain_cast_constraints(
    program: &mut ProgramBuilder,
    chain: &[crate::sync::Arc<TypeDef>],
    reg: usize,
    resolver: &Resolver,
) -> Result<()> {
    use crate::error::{SQLITE_CONSTRAINT_CHECK, SQLITE_CONSTRAINT_NOTNULL};

    let any_not_null = chain.iter().any(|td| td.not_null);

    if any_not_null {
        program.emit_insn(Insn::HaltIfNull {
            target_reg: reg,
            err_code: SQLITE_CONSTRAINT_NOTNULL,
            description: format!(
                "domain {} does not allow null values",
                chain.first().map(|td| td.name.as_str()).unwrap_or("?")
            ),
        });
    }

    for td in chain {
        for (i, dc) in td.domain_checks.iter().enumerate() {
            let constraint_name = dc
                .name
                .clone()
                .unwrap_or_else(|| format!("{}_{}", td.name, i));

            // Bind `value` → reg, translate check expr, verify truthy
            program
                .id_register_overrides
                .insert("value".to_string(), reg);

            let expr_result_reg = program.alloc_register();
            translate_expr(program, None, &dc.check, expr_result_reg, resolver)?;

            program.id_register_overrides.remove("value");

            let passed_label = program.allocate_label();

            // NULL result passes CHECK constraints (SQLite semantics)
            program.emit_insn(Insn::IsNull {
                reg: expr_result_reg,
                target_pc: passed_label,
            });

            program.emit_insn(Insn::If {
                reg: expr_result_reg,
                target_pc: passed_label,
                jump_if_null: false,
            });

            program.emit_insn(Insn::Halt {
                err_code: SQLITE_CONSTRAINT_CHECK,
                description: format!(
                    "value for domain {} violates check constraint \"{}\"",
                    td.name, constraint_name
                ),
                on_error: None,
                description_reg: None,
            });

            program.preassign_label_to_next_insn(passed_label);
        }
    }
    Ok(())
}

/// Emit bytecode for a custom type encode/decode expression.
/// Sets up `value` to reference `value_reg`, and type parameter overrides
/// from `column.ty_params` matched against `type_def.params`.
/// The expression result is written to `dest_reg`.
pub(crate) fn emit_type_expr(
    program: &mut ProgramBuilder,
    expr: &ast::Expr,
    value_reg: usize,
    dest_reg: usize,
    column: &Column,
    type_def: &TypeDef,
    resolver: &Resolver,
) -> Result<usize> {
    // Set up value override
    program
        .id_register_overrides
        .insert("value".to_string(), value_reg);

    // Set up type parameter overrides. Capture the result so we can
    // clean up overrides even if param translation fails.
    let param_result: Result<()> = (|| {
        // Skip `value` param (already handled above); match remaining params
        // against the user-provided ty_params by position.
        let user_params: Vec<_> = type_def.user_params().collect();
        for (i, param) in user_params.iter().enumerate() {
            if let Some(param_expr) = column.ty_params.get(i) {
                let reg = program.alloc_register();
                translate_expr(program, None, param_expr, reg, resolver)?;
                program
                    .id_register_overrides
                    .insert(param.name.clone(), reg);
            }
        }
        Ok(())
    })();

    // Translate body expression only if param setup succeeded
    let result = param_result.and_then(|()| {
        // Translate the expression, disabling constant optimization since
        // the `value` placeholder refers to a register that changes per row.
        translate_expr_no_constant_opt(
            program,
            None,
            expr,
            dest_reg,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )
    });

    // Always clean up overrides, even on error
    program.id_register_overrides.clear();

    result
}

/// Decode custom type columns for AFTER trigger NEW registers.
///
/// For each column with a custom type decode expression, copies the encoded register
/// to a new register and emits the decode expression. NULL values are skipped.
/// Returns a Vec of registers: one per column (decoded or original) plus the rowid at the end.
pub(crate) fn emit_trigger_decode_registers(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    columns: &[Column],
    source_regs: &dyn Fn(usize) -> usize,
    rowid_reg: usize,
    is_strict: bool,
) -> Result<Vec<usize>> {
    columns
        .iter()
        .enumerate()
        .map(|(i, col)| -> Result<usize> {
            let type_def = resolver.schema().get_type_def(&col.ty_str, is_strict);
            if let Some(type_def) = type_def {
                if let Some(decode_expr) = type_def.decode() {
                    let src = source_regs(i);
                    let decoded_reg = program.alloc_register();
                    program.emit_insn(Insn::Copy {
                        src_reg: src,
                        dst_reg: decoded_reg,
                        extra_amount: 0,
                    });
                    let skip_label = program.allocate_label();
                    program.emit_insn(Insn::IsNull {
                        reg: decoded_reg,
                        target_pc: skip_label,
                    });
                    emit_type_expr(
                        program,
                        decode_expr,
                        decoded_reg,
                        decoded_reg,
                        col,
                        type_def,
                        resolver,
                    )?;
                    program.preassign_label_to_next_insn(skip_label);
                    return Ok(decoded_reg);
                }
            }
            Ok(source_regs(i))
        })
        .chain(std::iter::once(Ok(rowid_reg)))
        .collect::<Result<Vec<usize>>>()
}

#[cfg(test)]
mod tests {
    use crate::{Database, DatabaseOpts, MemoryIO, OpenFlags, SqliteDialect, Value, IO};
    use std::sync::Arc;

    fn open() -> Arc<crate::Connection> {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            ":memory:",
            OpenFlags::Create,
            DatabaseOpts::new().with_custom_types(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        db.connect().unwrap()
    }

    fn count(conn: &Arc<crate::Connection>, sql: &str, param: Option<Value>) -> crate::Result<i64> {
        let mut stmt = conn.prepare(sql)?;
        if let Some(value) = param {
            stmt.bind_at(1.try_into().unwrap(), value)?;
        }
        let rows = stmt.run_collect_rows()?;
        Ok(rows[0][0].as_int().expect("a count"))
    }

    /// fastest-wire (wire review 2 item 3, 6b (a)): a bound parameter compared with a custom-type
    /// column got the plain comparison, not the type's operator, so a numeric column (stored
    /// encoded) never equalled `?1` bound to the very value a literal finds. A parameter is treated
    /// as a literal of the type's value input type: encoded and passed to the operator. Mutant
    /// `param_skips_type_operator`.
    #[test]
    fn a_parameter_compared_with_a_custom_type_column_uses_its_operator() {
        let conn = open();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 1.5)").unwrap();
        assert_eq!(
            count(&conn, "SELECT count(*) FROM t WHERE x = 1.5", None).unwrap(),
            1,
            "premise: the literal finds the row"
        );
        assert_eq!(
            count(
                &conn,
                "SELECT count(*) FROM t WHERE x = ?1",
                Some(Value::from_f64(1.5))
            )
            .unwrap(),
            1,
            "a bound parameter did not find the row the same literal finds"
        );
    }

    /// fastest-wire (6b (b)): a comparison operand was encoded with the column's own parameters,
    /// so a value longer than a length-checked type's length raised 'value too long' where
    /// PostgreSQL compares (a comparison operand is coerced to the type with no typmod) and answers
    /// false. The operand now reaches the type's operator as given, never encoded (engine review 14
    /// HIGH 1). The type's '=' is `instr`, which finds what a plain '=' does not ('b' in 'abc'), so
    /// the test sees that a literal and a parameter reach it. Mutant `operand_keeps_column_typmod`.
    #[test]
    fn an_over_length_comparison_operand_compares_instead_of_raising() {
        let conn = open();
        conn.execute(
            "CREATE TYPE tag(value text, maxlen integer) BASE text ENCODE CASE WHEN maxlen IS NULL \
             THEN value WHEN length(value) <= maxlen THEN value ELSE RAISE(ABORT, 'value too long \
             for type tag') END DECODE value OPERATOR '=' instr",
        )
        .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v tag(3)) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'abc')").unwrap();
        assert!(
            conn.execute("INSERT INTO t VALUES (2, 'abcdef')").is_err(),
            "premise: the column's own length check refuses an over-length value"
        );
        assert_eq!(
            count(&conn, "SELECT count(*) FROM t WHERE v = 'abc'", None).unwrap(),
            1,
            "premise: equal finds it"
        );
        assert_eq!(
            count(&conn, "SELECT count(*) FROM t WHERE v = 'b'", None).unwrap(),
            1,
            "premise: a literal reaches the type's '=' (instr), where a plain '=' finds nothing"
        );
        let inner = count(
            &conn,
            "SELECT count(*) FROM t WHERE v = ?1",
            Some(Value::build_text("b")),
        );
        assert!(
            matches!(inner, Ok(1)),
            "premise: a parameter reaches the type's '=': {inner:?}"
        );
        let literal = count(&conn, "SELECT count(*) FROM t WHERE v = 'abcdef'", None);
        assert!(
            matches!(literal, Ok(0)),
            "an over-length literal raised or matched: {literal:?}"
        );
        let param = count(
            &conn,
            "SELECT count(*) FROM t WHERE v = ?1",
            Some(Value::build_text("abcdef")),
        );
        assert!(
            matches!(param, Ok(0)),
            "an over-length parameter raised or matched: {param:?}"
        );
    }

    fn exec(conn: &Arc<crate::Connection>, sql: &str, params: &[Value]) -> crate::Result<()> {
        let mut stmt = conn.prepare(sql)?;
        for (i, value) in params.iter().enumerate() {
            stmt.bind_at((i + 1).try_into().unwrap(), value.clone())?;
        }
        stmt.run_ignore_rows()
    }

    /// Engine review 14 HIGH 1: 6b (a) encoded every bound operand of a custom type's operator
    /// with the COLUMN's parameters, so on numeric(10, 2) a parameter was truncated to two places
    /// and refused past ten digits, in arithmetic as well as comparisons: `x * ?1` with 1.075
    /// stored 10.70, `x / ?1` with 0.001 divided by zero, `x = ?1` with 1.509 matched 1.50, and
    /// `x < ?1` with 1e9 raised. The operator takes its operand as given (the column reaches it
    /// decoded). Mutant `operand_keeps_column_typmod`.
    #[test]
    fn a_numeric_operand_keeps_its_own_precision() {
        let conn = open();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10.00), (2, 12.34), (3, 1.50)")
            .unwrap();
        let at = |id: i64, literal: &str| {
            count(
                &conn,
                &format!("SELECT count(*) FROM t WHERE id = {id} AND x = {literal}"),
                None,
            )
        };
        assert!(matches!(at(3, "1.50"), Ok(1)), "premise: a literal finds row 3");
        let mul = exec(&conn, "UPDATE t SET x = x * ?1 WHERE id = 1", &[Value::from_f64(1.075)]);
        assert!(
            mul.is_ok() && matches!(at(1, "10.75"), Ok(1)),
            "x * ?1 with 1.075 did not store 10.75: {mul:?}"
        );
        let div = exec(&conn, "UPDATE t SET x = x / ?1 WHERE id = 2", &[Value::from_f64(0.001)]);
        assert!(
            div.is_ok() && matches!(at(2, "12340"), Ok(1)),
            "x / ?1 with 0.001 did not store 12340: {div:?}"
        );
        for (sql, param, want) in [
            ("x = ?1", 1.509, 0),
            ("x >= ?1", 1.501, 0),
            ("x < ?1", 1e9, 1),
        ] {
            let got = count(
                &conn,
                &format!("SELECT count(*) FROM t WHERE id = 3 AND {sql}"),
                Some(Value::from_f64(param)),
            );
            assert!(
                matches!(got, Ok(n) if n == want),
                "{sql} with {param} on 1.50 gave {got:?}, not {want}"
            );
        }
    }

    /// The `EXPLAIN QUERY PLAN` lines of `sql`, for a premise on the plan the planner chose.
    fn plan(conn: &Arc<crate::Connection>, sql: &str) -> Vec<String> {
        conn.prepare(format!("EXPLAIN QUERY PLAN {sql}"))
            .and_then(|mut stmt| stmt.run_collect_rows())
            .expect("premise: the plan is explained")
            .iter()
            .map(|row| {
                row.iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect()
    }

    /// Whether `sql` searches `table` through `index`.
    fn seeks(conn: &Arc<crate::Connection>, sql: &str, table: &str, index: &str) -> bool {
        plan(conn, sql).iter().any(|line| {
            line.contains(&format!("SEARCH {table} USING")) && line.contains(&format!("INDEX {index}"))
        })
    }

    /// Whether `sql` scans `table`.
    fn scans(conn: &Arc<crate::Connection>, sql: &str, table: &str) -> bool {
        plan(conn, sql)
            .iter()
            .any(|line| line.contains(&format!("SCAN {table}")))
    }

    /// Each `(predicate, parameter, rows)` answers `rows` through a seek of `index` and through a
    /// scan (NOT INDEXED) of `table`, each plan asserted as a premise.
    fn answers_by_both_plans(
        conn: &Arc<crate::Connection>,
        table: &str,
        index: &str,
        cases: &[(&str, Option<Value>, i64)],
    ) {
        for (pred, param, want) in cases {
            let seek = format!("SELECT count(*) FROM {table} WHERE {pred}");
            let scan = format!("SELECT count(*) FROM {table} NOT INDEXED WHERE {pred}");
            assert!(
                seeks(conn, &seek, table, index),
                "premise: {seek} seeks index {index}: {:?}",
                plan(conn, &seek)
            );
            assert!(
                scans(conn, &scan, table),
                "premise: {scan} scans: {:?}",
                plan(conn, &scan)
            );
            let by_scan = count(conn, &scan, param.clone());
            assert!(
                matches!(by_scan, Ok(n) if n == *want),
                "premise: the scan answers {want} for {pred} ({param:?}): {by_scan:?}"
            );
            let by_seek = count(conn, &seek, param.clone());
            assert!(
                matches!(by_seek, Ok(n) if n == *want),
                "the seek of {index} answered {by_seek:?} for {pred} ({param:?}), the scan {want}"
            );
        }
    }

    /// Engine review 16 HIGH 2 (review 14 MED 6): the seek path ENCODEs an equality's key with the
    /// column's parameters and consumes the term, so the type's operator never re-checks a row the
    /// seek returns, while a scan passes the operand to the operator as given (2fa04254c). On
    /// numeric(10, 2) with an index on x, `x = 1.501` matched 1.50 through the seek (the key was cut
    /// to the scale), and `x = ?1` bound to 1e9 raised "numeric value out of range"; a scan answers
    /// 0 for both, which is PostgreSQL's answer. The index arm of
    /// `a_numeric_operand_keeps_its_own_precision`. Mutant `seek_key_encodes_raising`.
    #[test]
    fn an_indexed_numeric_equality_answers_as_a_scan_does() {
        let conn = open();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT")
            .unwrap();
        conn.execute("CREATE INDEX tx ON t(x)").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10.00), (2, 12.34), (3, 1.50)")
            .unwrap();
        answers_by_both_plans(
            &conn,
            "t",
            "tx",
            &[
                ("x = 1.50", None, 1),
                ("x = ?1", Some(Value::from_f64(1.5)), 1),
                ("x = 1.501", None, 0),
                ("x = ?1", Some(Value::from_f64(1.509)), 0),
                ("x = 1e9", None, 0),
                ("x = ?1", Some(Value::from_f64(1e9)), 0),
            ],
        );
    }

    /// Register the type `sql` creates as a built-in type, as the wire registers its bpchar.
    fn register_built_in(conn: &Arc<crate::Connection>, sql: &str) {
        let mut parser = turso_parser::parser::Parser::new(sql.as_bytes());
        let Ok(Some(turso_parser::ast::Cmd::Stmt(turso_parser::ast::Stmt::CreateType {
            type_name, body, ..
        }))) = parser.next_cmd()
        else {
            panic!("premise: the type parses");
        };
        let def = crate::schema::TypeDef::from_create_type(&type_name, &body, true, sql.to_string())
            .unwrap();
        conn.with_schema_mut(|schema| {
            schema
                .type_registry
                .insert(type_name.to_lowercase(), Arc::new(def))
        })
        .unwrap();
    }

    /// Engine review 16 HIGH 2 (review 14 MED 6): a length-checked type with a function '=' and a
    /// UNIQUE index on the column: the seek ENCODEs an over-length key with the column's length and
    /// raises 'value too long', where a scan compares it, false, and answers 0 rows (the wire's
    /// `code = $1` shape). The UNIQUE v arm of
    /// `an_over_length_comparison_operand_compares_instead_of_raising` (a user type, t) and of
    /// `a_built_in_length_checked_type_compares_an_over_length_operand` (registered built-in, u).
    /// Mutant `seek_key_encodes_raising`.
    #[test]
    fn an_indexed_length_checked_type_compares_an_over_length_operand() {
        let conn = open();
        conn.execute(
            "CREATE TYPE tag(value text, maxlen integer) BASE text ENCODE CASE WHEN maxlen IS NULL \
             THEN value WHEN length(value) <= maxlen THEN value ELSE RAISE(ABORT, 'value too long \
             for type tag') END DECODE value OPERATOR '=' instr",
        )
        .unwrap();
        register_built_in(
            &conn,
            "CREATE TYPE bpc(value text, maxlen integer) BASE text ENCODE CASE WHEN length(value) \
             <= maxlen THEN value ELSE RAISE(ABORT, 'value too long for type bpc') END DECODE \
             value OPERATOR '=' instr",
        );
        for (table, ty, index) in [("t", "tag(3)", "tv"), ("u", "bpc(3)", "uv")] {
            conn.execute(format!(
                "CREATE TABLE {table}(id INTEGER PRIMARY KEY, v {ty}) STRICT"
            ))
            .unwrap();
            conn.execute(format!("CREATE UNIQUE INDEX {index} ON {table}(v)"))
                .unwrap();
            conn.execute(format!("INSERT INTO {table} VALUES (1, 'abc')"))
                .unwrap();
            assert!(
                conn.execute(format!("INSERT INTO {table} VALUES (2, 'abcdef')"))
                    .is_err(),
                "premise: {ty}'s own length check refuses an over-length value"
            );
            answers_by_both_plans(
                &conn,
                table,
                index,
                &[
                    ("v = 'abc'", None, 1),
                    ("v = ?1", Some(Value::build_text("abc")), 1),
                    ("v = 'abcdef'", None, 0),
                    ("v = ?1", Some(Value::build_text("abcdef")), 0),
                ],
            );
        }
    }

    /// Engine review 14 HIGH 2: 6b (b) bound every user type's parameters to NULL when encoding a
    /// comparison operand, on a contract nothing states: the documented length-check ENCODE then
    /// raised on every comparison, a user copy of numeric raised "precision must be an integer",
    /// and a substr-shaped ENCODE turned the operand into NULL and matched nothing. A user type
    /// keeps its parameters' meaning; its operator's operand is not encoded at all (HIGH 1).
    #[test]
    fn a_user_types_operator_does_not_unbind_its_parameters() {
        let conn = open();
        conn.execute(
            "CREATE TYPE tag2(value text, maxlen integer) BASE text ENCODE CASE WHEN \
             length(value) <= maxlen THEN value ELSE RAISE(ABORT, 'value too long for type tag2') \
             END DECODE value OPERATOR '=' instr",
        )
        .unwrap();
        conn.execute(
            "CREATE TYPE money(value any, precision integer, scale integer) BASE blob ENCODE \
             numeric_encode(value, precision, scale) DECODE numeric_decode(value) OPERATOR '+' \
             numeric_add OPERATOR '<' numeric_lt OPERATOR '=' numeric_eq",
        )
        .unwrap();
        conn.execute(
            "CREATE TYPE short(value text, len integer) BASE text ENCODE substr(value, 1, len) \
             DECODE value OPERATOR '=' instr",
        )
        .unwrap();
        conn.execute(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v tag2(3), m money(10, 2), s short(3)) STRICT",
        )
        .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'abc', 1.5, 'abcdef')")
            .unwrap();
        let tag = count(&conn, "SELECT count(*) FROM t WHERE v = 'abc'", None);
        assert!(matches!(tag, Ok(1)), "a length-checked user type's comparison gave {tag:?}");
        let eq = count(&conn, "SELECT count(*) FROM t WHERE m = 1.5", None);
        assert!(matches!(eq, Ok(1)), "a user numeric's m = 1.5 gave {eq:?}");
        let lt = count(&conn, "SELECT count(*) FROM t WHERE m < ?1", Some(Value::from_f64(2.0)));
        assert!(matches!(lt, Ok(1)), "a user numeric's m < ?1 gave {lt:?}");
        let add = conn.prepare("SELECT m + 1 FROM t").and_then(|mut s| s.run_collect_rows());
        assert!(add.is_ok(), "a user numeric's m + 1 raised: {add:?}");
        let short = count(&conn, "SELECT count(*) FROM t WHERE s = 'abc'", None);
        assert!(matches!(short, Ok(1)), "a substr-encoded user type's comparison gave {short:?}");
    }

    /// Engine review 14 HIGH 3: a type registered as built-in (the wire's bpchar: a length check
    /// and a function '=') never took 6b (b)'s unconstrained path, and 6b (a) sent a bound
    /// parameter through its length-checking ENCODE too: an over-length parameter raised 'value
    /// too long' where PostgreSQL, and this code before 6b, answer no rows. Both an over-length
    /// literal and parameter compare, false.
    #[test]
    fn a_built_in_length_checked_type_compares_an_over_length_operand() {
        let conn = open();
        let sql = "CREATE TYPE bpc(value text, maxlen integer) BASE text ENCODE CASE WHEN \
                   length(value) <= maxlen THEN value ELSE RAISE(ABORT, 'value too long for type \
                   bpc') END DECODE value OPERATOR '=' instr";
        let mut parser = turso_parser::parser::Parser::new(sql.as_bytes());
        let Ok(Some(turso_parser::ast::Cmd::Stmt(turso_parser::ast::Stmt::CreateType {
            type_name, body, ..
        }))) = parser.next_cmd()
        else {
            panic!("premise: the type parses");
        };
        let def = crate::schema::TypeDef::from_create_type(&type_name, &body, true, sql.to_string())
            .unwrap();
        conn.with_schema_mut(|schema| {
            schema
                .type_registry
                .insert(type_name.to_lowercase(), Arc::new(def))
        })
        .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v bpc(3)) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'abc')").unwrap();
        assert!(
            conn.execute("INSERT INTO t VALUES (2, 'abcdef')").is_err(),
            "premise: the built-in type's own length check refuses an over-length value"
        );
        let literal = count(&conn, "SELECT count(*) FROM t WHERE v = 'abcdef'", None);
        assert!(matches!(literal, Ok(0)), "an over-length literal gave {literal:?}");
        let param = count(
            &conn,
            "SELECT count(*) FROM t WHERE v = ?1",
            Some(Value::build_text("abcdef")),
        );
        assert!(matches!(param, Ok(0)), "an over-length parameter gave {param:?}");
    }

    /// Engine review 14 HIGH 4: NULL bypasses ENCODE and DECODE (create-type.mdx), as the INSERT
    /// and seek paths keep it, but 6b (a) sent a NULL parameter into the operator path's ENCODE
    /// unguarded: any ENCODE ending in `ELSE RAISE` raised on `col < ?1` bound NULL, where the
    /// comparison is NULL and selects no row. Kept as a regression guard.
    #[test]
    fn a_null_parameter_never_reaches_a_types_encode() {
        let conn = open();
        conn.execute(
            "CREATE TYPE pint(value integer) BASE integer ENCODE CASE WHEN value > 0 THEN value \
             ELSE RAISE(ABORT, 'pint must be positive') END DECODE value OPERATOR '<' numeric_lt",
        )
        .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v pint) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 5)").unwrap();
        assert!(
            matches!(count(&conn, "SELECT count(*) FROM t WHERE v < 7", None), Ok(1)),
            "premise: the type's operator compares"
        );
        for sql in ["v < ?1", "v >= ?1"] {
            let got = count(
                &conn,
                &format!("SELECT count(*) FROM t WHERE {sql}"),
                Some(Value::Null),
            );
            assert!(matches!(got, Ok(0)), "{sql} bound NULL gave {got:?}");
        }
    }

    /// Engine review 16 MED 10: since 2fa04254c an operand qualifies for a custom type's operator
    /// if it is a compatible literal or ANY constant, so only a bare literal is type-checked:
    /// `v = ('abc')`, `v = CAST('abc' AS TEXT)` and `v = 'ab' || 'c'` reach numeric_eq on an
    /// integer-valued type and raise, where `v = 'abc'` (a text literal, not the type's value
    /// input) takes the plain comparison and answers no row; and `w = 'ABC' COLLATE NOCASE` reaches
    /// the type's operator, which drops the collation. An operand is checked through its
    /// parentheses, its sign and a CAST's target type; a COLLATE operand takes the plain
    /// comparison, which honours it. Mutant `operand_any_constant`.
    #[test]
    fn an_operand_qualifies_for_a_types_operator_by_its_own_type() {
        let conn = open();
        conn.execute(
            "CREATE TYPE pint(value integer) BASE integer ENCODE value DECODE value OPERATOR '=' \
             numeric_eq",
        )
        .unwrap();
        conn.execute(
            "CREATE TYPE tag(value text, maxlen integer) BASE text ENCODE CASE WHEN maxlen IS NULL \
             THEN value WHEN length(value) <= maxlen THEN value ELSE RAISE(ABORT, 'value too long \
             for type tag') END DECODE value OPERATOR '=' instr",
        )
        .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v pint, w tag(3)) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 5, 'abc')").unwrap();
        let plain = count(&conn, "SELECT count(*) FROM t WHERE v = 'abc'", None);
        assert!(
            matches!(plain, Ok(0)),
            "premise: a text literal takes the plain comparison: {plain:?}"
        );
        for operand in ["('abc')", "(('abc'))", "CAST('abc' AS TEXT)", "'ab' || 'c'"] {
            let got = count(
                &conn,
                &format!("SELECT count(*) FROM t WHERE v = {operand}"),
                None,
            );
            assert!(
                matches!(got, Ok(0)),
                "v = {operand} answered {got:?}, where v = 'abc' answers Ok(0)"
            );
        }
        assert!(
            matches!(count(&conn, "SELECT count(*) FROM t WHERE w = 'b'", None), Ok(1)),
            "premise: a text literal reaches tag's '=' (instr), which finds 'b' in 'abc'"
        );
        let collated = count(
            &conn,
            "SELECT count(*) FROM t WHERE w = 'ABC' COLLATE NOCASE",
            None,
        );
        assert!(
            matches!(collated, Ok(1)),
            "w = 'ABC' COLLATE NOCASE answered {collated:?}: the collation was dropped"
        );
    }

    /// Engine review 16 MED 10's other half: the type check must not shut out an operand of the
    /// type's value input type that is not a bare literal. A signed literal (`v = -1`, `v = (+7)`)
    /// and a CAST to the value input type reach the type's operator; here '=' is `max`, which a
    /// plain comparison is not (5 = -1 is false; max(5, -1) is 5, true). And numeric's operator
    /// still compares `x = -1.5` with -1.50.
    #[test]
    fn a_signed_or_cast_operand_still_reaches_a_types_operator() {
        let conn = open();
        conn.execute(
            "CREATE TYPE pmax(value integer) BASE integer ENCODE value DECODE value OPERATOR '=' \
             max",
        )
        .unwrap();
        conn.execute(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v pmax, x numeric(10, 2)) STRICT",
        )
        .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 5, -1.50)").unwrap();
        assert!(
            matches!(count(&conn, "SELECT count(*) FROM t WHERE v = 1", None), Ok(1)),
            "premise: a bare literal reaches pmax's '=' (max(5, 1) is true)"
        );
        for operand in ["-1", "(-1)", "(+7)", "- 3", "CAST(7 AS INTEGER)"] {
            let got = count(
                &conn,
                &format!("SELECT count(*) FROM t WHERE v = {operand}"),
                None,
            );
            assert!(
                matches!(got, Ok(1)),
                "v = {operand} answered {got:?}: it did not reach pmax's operator"
            );
        }
        for (pred, want) in [("x = -1.5", 1), ("x = -1.509", 0), ("x = (-1.50)", 1)] {
            let got = count(&conn, &format!("SELECT count(*) FROM t WHERE {pred}"), None);
            assert!(
                matches!(got, Ok(n) if n == want),
                "{pred} on -1.50 answered {got:?}, not {want}"
            );
        }
    }
}
