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
        translate_custom_type_operand(program, referenced_tables, first, arg_reg, resolver, &func)?;
        translate_custom_type_operand(
            program,
            referenced_tables,
            second,
            arg_reg + 1,
            resolver,
            &func,
        )?;
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

fn translate_custom_type_operand(
    program: &mut ProgramBuilder,
    tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target: usize,
    resolver: &Resolver,
    func: &Func,
) -> Result<()> {
    if matches!(
        func,
        Func::Scalar(
            ScalarFunc::NumericAdd
                | ScalarFunc::NumericSub
                | ScalarFunc::NumericMul
                | ScalarFunc::NumericDiv
        )
    ) {
        // Numeric's operators already parse exact text. Converting a decimal literal to
        // SQLite REAL first loses its display scale and possibly significant digits.
        let mut literal = expr;
        let mut negative = false;
        loop {
            match literal {
                ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => literal = &exprs[0],
                ast::Expr::Unary(ast::UnaryOperator::Positive, inner) => literal = inner,
                ast::Expr::Unary(ast::UnaryOperator::Negative, inner) => {
                    negative = !negative;
                    literal = inner;
                }
                ast::Expr::Literal(ast::Literal::Numeric(text)) => {
                    // Keep SQLite's signed hexadecimal-integer interpretation.
                    let unsigned = text.trim_start_matches(['+', '-']);
                    if unsigned.starts_with("0x") || unsigned.starts_with("0X") {
                        break;
                    }
                    let mut value = unsigned.replace('_', "");
                    if negative != text.starts_with('-') {
                        value.insert(0, '-');
                    }
                    program.emit_insn(Insn::String8 { value, dest: target });
                    program.mark_last_insn_constant();
                    return Ok(());
                }
                _ => break,
            }
        }
    }
    translate_expr(program, tables, expr, target, resolver)?;
    Ok(())
}

/// `emit_custom_type_operator` for operands already in registers: `lhs_reg` holds `e1` and
/// `rhs_reg` holds `e2` of the pair `find_custom_type_operator` resolved, each as the operator
/// takes it (a custom-type column decoded, the other operand as given). Used by an IN list, which
/// translates its left side once and each element once (engine review 14 LOW 14). Returns the
/// result's register.
pub(super) fn emit_custom_type_operator_on_regs(
    program: &mut ProgramBuilder,
    lhs_reg: usize,
    rhs_reg: usize,
    resolved: &ResolvedOperator,
    resolver: &Resolver,
) -> Result<usize> {
    let func = resolver
        .resolve_function(&resolved.func_name, 2)?
        .ok_or_else(|| {
            LimboError::InternalError(format!("function not found: {}", resolved.func_name))
        })?;
    let (first, second) = if resolved.swap_args {
        (rhs_reg, lhs_reg)
    } else {
        (lhs_reg, rhs_reg)
    };
    let args = program.alloc_registers(2);
    program.emit_insn(Insn::Copy {
        src_reg: first,
        dst_reg: args,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Copy {
        src_reg: second,
        dst_reg: args + 1,
        extra_amount: 0,
    });
    let result_reg = program.alloc_register();
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: args,
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

/// The column an operand meets, for mutant `operand_keeps_column_typmod` alone: built in test
/// builds only (engine review 16 #22: a `Column` clone per prepare that production never read).
fn encode_info(
    column: &Column,
    type_def: &Arc<TypeDef>,
    which: EncodeArg,
) -> Option<OperatorEncodeInfo> {
    cfg!(test).then(|| OperatorEncodeInfo {
        column: column.clone(),
        type_def: type_def.clone(),
        which,
    })
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
/// 2. One operand is a custom type column and the other a compatible operand
///    (`operand_compatible`): a literal (through parentheses and a sign) or a CAST
///    whose type is compatible with the custom type's `value` input type, or a bound
///    parameter. Any other operand gets the standard comparison.
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
                    encode_info: encode_info(&lhs.column, &lhs.type_def, EncodeArg::Second),
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
                    encode_info: encode_info(&rhs.column, &rhs.type_def, EncodeArg::First),
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
/// The functions a BUILT-IN type's '=' may call and still agree with equality of the type's
/// encodings, which an index compares (engine review 20 item 8): numeric_eq (numeric_encode is
/// canonical at the column's scale) and the wire's bpchareq (its ENCODE drops the trailing blanks
/// bpchareq ignores). An allowlist: a function not named here, or named by a user type whose ENCODE
/// we did not read, is taken to disagree.
const EQ_AGREES_WITH_ENCODING: [&str; 2] = ["numeric_eq", "bpchareq"];

/// Whether an equality on a column of `type_def` may be answered from an index (engine review 20
/// item 8): the type's '=' agrees with equality of the encodings, so the seek misses no row the
/// '=' would find (the WHERE term's re-check can only remove rows). A type with no function '='
/// compares with the plain comparison, which is that equality. Mutant `eq_index_ignores_coarse_eq`
/// (test builds only): every type may, as before.
pub(crate) fn eq_agrees_with_encoding(type_def: &TypeDef) -> bool {
    if crate::branch::store::fe_mutant("eq_index_ignores_coarse_eq") {
        return true;
    }
    match type_eq_function(type_def) {
        None => true,
        Some(func) => type_def.is_builtin && EQ_AGREES_WITH_ENCODING.iter().any(|f| func.eq_ignore_ascii_case(f)),
    }
}

/// Whether an equality between a column of the custom type `type_name` (`type_def`) and the operand
/// `key` compares through the type's operator (`find_custom_type_operator`): `key` is a column of
/// the same type, or an operand the type takes (`operand_compatible`). Otherwise the equality takes
/// the plain comparison, which never passes `key` to the type's '=' (a TEXT join column against
/// numeric, `a_text_join_against_an_indexed_numeric_column_answers_as_a_scan_does`).
pub(crate) fn key_takes_type_operator(
    key: &ast::Expr,
    type_name: &str,
    type_def: &TypeDef,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> bool {
    match expr_custom_type_info(key, referenced_tables, resolver) {
        Some(info) => info.type_name.eq_ignore_ascii_case(type_name),
        None => operand_compatible(key, type_def.value_input_type(), resolver),
    }
}

pub(crate) fn seek_key_eq_function(type_def: &TypeDef) -> Option<&str> {
    let eq = type_eq_function(type_def);
    if eq.is_none() || crate::branch::store::fe_mutant("seek_key_encodes_raising") {
        return None;
    }
    eq
}

/// Whether `expr` is passed to an operator of a custom type whose `value` input type is
/// `value_input_type`: an operand whose type is known before it runs and is compatible
/// (`typed_operand_compatible`: a literal, through its parentheses and sign, or a CAST to a
/// compatible type), or a bound parameter, whose value's type is known only when it runs
/// (fastest-wire, wire review 2 item 3: a parameter got the plain comparison, so `code = $1` from
/// every extended-protocol client missed what the literal finds). A type whose input is `any`
/// (numeric) takes any constant but a COLLATE one: there is no type to check. Anything else, a
/// function call, a concatenation, a COLLATE operand, takes the plain comparison, as an
/// incompatible literal does (engine review 16 MED 10: 2fa04254c passed every constant
/// unchecked, so `v = ('abc')` raised in
/// the operator where `v = 'abc'` compared plainly, and a COLLATE was dropped). Mutant
/// `param_skips_type_operator` (test builds only): a parameter is not one, as before 6b. Mutant
/// `operand_any_constant` (test builds only): any constant is one, as 2fa04254c had it.
fn operand_compatible(expr: &ast::Expr, value_input_type: &str, resolver: &Resolver) -> bool {
    if matches!(expr, ast::Expr::Variable(_)) {
        return !crate::branch::store::fe_mutant("param_skips_type_operator");
    }
    typed_operand_compatible(expr, value_input_type)
        || (value_input_type.eq_ignore_ascii_case("any")
            && !matches!(expr, ast::Expr::Literal(_) | ast::Expr::Collate(..))
            && expr.is_constant(resolver)
            && !crate::branch::store::fe_mutant("any_input_takes_no_constant"))
        || (crate::branch::store::fe_mutant("operand_any_constant")
            && !matches!(expr, ast::Expr::Literal(_))
            && expr.is_constant(resolver))
}

/// Whether an operand whose type is known before it runs is of a type compatible with a custom
/// type's `value` input type: a literal (`literal_type_name`), the same through parentheses
/// around one expression, a sign on a numeric literal, or a CAST, by its target type
/// (`cast_type_compatible`). False for everything else (engine review 16 MED 10).
fn typed_operand_compatible(expr: &ast::Expr, value_input_type: &str) -> bool {
    match expr {
        ast::Expr::Literal(_) => literal_type_name(expr)
            .is_some_and(|t| literal_compatible_with_value_type(t, value_input_type)),
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            typed_operand_compatible(&exprs[0], value_input_type)
        }
        ast::Expr::Unary(ast::UnaryOperator::Negative | ast::UnaryOperator::Positive, inner) => {
            is_signed_numeric_literal(inner) && typed_operand_compatible(inner, value_input_type)
        }
        ast::Expr::Cast {
            type_name: Some(type_name),
            ..
        } => cast_type_compatible(&type_name.name, value_input_type),
        _ => false,
    }
}

/// A numeric literal, possibly parenthesized or signed: what a sign keeps the literal's type for
/// (`-'abc'` is not text, so a sign on anything else qualifies nothing).
fn is_signed_numeric_literal(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Literal(ast::Literal::Numeric(_)) => true,
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => is_signed_numeric_literal(&exprs[0]),
        ast::Expr::Unary(ast::UnaryOperator::Negative | ast::UnaryOperator::Positive, inner) => {
            is_signed_numeric_literal(inner)
        }
        _ => false,
    }
}

/// Whether a CAST to `cast_type` yields a value of a custom type's `value` input type: the input
/// is `any`, or names the CAST's type, or names the class of the CAST's affinity (INT -> integer,
/// VARCHAR -> text, DOUBLE -> real, BLOB -> blob). A NUMERIC CAST has no single class.
fn cast_type_compatible(cast_type: &str, value_input_type: &str) -> bool {
    if value_input_type.eq_ignore_ascii_case("any") || cast_type.eq_ignore_ascii_case(value_input_type)
    {
        return true;
    }
    let class = match crate::vdbe::affinity::Affinity::affinity(cast_type) {
        crate::vdbe::affinity::Affinity::Integer => "integer",
        crate::vdbe::affinity::Affinity::Text => "text",
        crate::vdbe::affinity::Affinity::Real => "real",
        crate::vdbe::affinity::Affinity::Blob => "blob",
        _ => return false,
    };
    class.eq_ignore_ascii_case(value_input_type)
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
    use crate::error::SQLITE_CONSTRAINT_NOTNULL;

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
        for (dc, constraint_name) in td.domain_checks.iter().zip(crate::schema::domain_check_names(td)) {

            // Bind `value` → reg, translate check expr, verify truthy
            program.id_register_overrides.insert("value", reg);

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
                err_code: crate::error::SQLITE_CONSTRAINT_CHECK_DOMAIN,
                description: crate::schema::domain_check_failure(&td.name, &constraint_name),
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
    // The existing numeric representation also serves unconstrained NUMERIC. Its
    // one-argument encoder applies PostgreSQL's global bounds without a typmod.
    let mut unbounded_numeric;
    let expr = if type_def.is_builtin && type_def.name.eq_ignore_ascii_case("numeric") && column.ty_params.is_empty() {
        if let ast::Expr::FunctionCall { name, .. } = expr {
            if name.as_str().eq_ignore_ascii_case("numeric_encode") {
                unbounded_numeric = expr.clone();
                if let ast::Expr::FunctionCall { args, .. } = &mut unbounded_numeric { args.truncate(1); }
                &unbounded_numeric
            } else { expr }
        } else { expr }
    } else { expr };
    // Set up value override
    program.id_register_overrides.insert("value", value_reg);

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
                program.id_register_overrides.insert(&param.name, reg);
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
    /// as a literal of the type's value input type: passed to the operator as given (2fa04254c;
    /// engine review 16 #20). The `maxlen IS NULL` arm of the ENCODE below is not vestigial (engine
    /// review 20 LOW 28): no comparison operand is encoded, but an index seek's key is, with the
    /// column's typmod (1d8bf442e), and the column's own values are. Mutant
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

    /// Engine review 16 #21, rewritten for engine review 20 HIGH 2 (FLAGGED, review-directed): a
    /// blob operand reaches numeric's operators as given, and they read a blob as the type's
    /// internal encoding, so `x = <blob>` compared the column with a value no INSERT could have
    /// stored (INSERT refuses every blob). It must be refused as that INSERT is, literal and bound.
    /// The first version used X'00', which `blob_to_bigdecimal` refuses as too short (under 14
    /// bytes), so it passed at its own sha and could not discriminate; this one uses a well-formed
    /// 18-byte encoding of 10.00 (version 1, scale 2, one limb of 1000), which the decoder accepts,
    /// so `x = <it>` matches row 1 unless the operand is refused. X'00' stays only as a premise.
    #[test]
    fn a_blob_operand_of_numeric_is_refused_as_its_insert_is() {
        const TEN: [u8; 18] = [1, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0xE8, 3, 0, 0];
        assert!(
            crate::numeric::decimal::blob_to_bigdecimal(&TEN).is_ok(),
            "premise: the blob is a well-formed numeric encoding"
        );
        assert!(
            crate::numeric::decimal::blob_to_bigdecimal(&[0]).is_err(),
            "premise: X'00' is refused by the decoder itself"
        );
        let conn = open();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10.00)").unwrap();
        let insert = conn.execute("INSERT INTO t VALUES (2, X'0100020000000000000001000000E8030000')");
        assert!(insert.is_err(), "premise: numeric refuses to store a blob, even its own encoding");
        for (sql, param) in [
            ("x = X'0100020000000000000001000000E8030000'", None),
            ("x = ?1", Some(Value::from_slice(&TEN).unwrap())),
        ] {
            let got = count(&conn, &format!("SELECT count(*) FROM t WHERE {sql}"), param);
            assert!(
                got.is_err(),
                "CLAIM: {sql} with a blob operand gave {got:?}, where an INSERT of that blob is refused"
            );
        }
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

    /// Each `(predicate, parameter, rows)` answers `rows` through the default plan, which does NOT
    /// seek `index` (engine review 20 item 8: the type's '=' does not agree with equality of the
    /// encodings, so the index is refused for the term), and through a scan (NOT INDEXED) of
    /// `table`, each plan asserted as a premise.
    fn answers_unindexed_by_both_plans(
        conn: &Arc<crate::Connection>,
        table: &str,
        index: &str,
        cases: &[(&str, Option<Value>, i64)],
    ) {
        for (pred, param, want) in cases {
            let default = format!("SELECT count(*) FROM {table} WHERE {pred}");
            let scan = format!("SELECT count(*) FROM {table} NOT INDEXED WHERE {pred}");
            assert!(
                !seeks(conn, &default, table, index),
                "premise: {default} does not seek index {index} (a coarser '='): {:?}",
                plan(conn, &default)
            );
            let by_scan = count(conn, &scan, param.clone());
            assert!(
                matches!(by_scan, Ok(n) if n == *want),
                "premise: the scan answers {want} for {pred} ({param:?}): {by_scan:?}"
            );
            let by_default = count(conn, &default, param.clone());
            assert!(
                matches!(by_default, Ok(n) if n == *want),
                "the default plan answered {by_default:?} for {pred} ({param:?}), the scan {want}"
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

    /// Engine review 20 item 6: the seek's catch region (1d8bf442e) caught every refusal of the
    /// key's ENCODE and answered an empty seek, so with an index `x = 'abc'` on numeric answered 0
    /// where NOT INDEXED, like PostgreSQL, raises (invalid input), and so did a well-formed numeric
    /// blob operand, which the type's operator refuses (94a5dc65c). A refused key is compared with
    /// itself under the type's '=' outside the catch region: what raises there raises, as the scan
    /// does. Both plans asserted as premises. Mutant `seek_refusal_swallowed`.
    #[test]
    fn an_indexed_seek_raises_where_the_scan_raises() {
        let conn = open();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT")
            .unwrap();
        conn.execute("CREATE INDEX tx ON t(x)").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10.00), (2, 12.34)")
            .unwrap();
        for pred in ["x = 'abc'", "x = X'0100020000000000000001000000E8030000'"] {
            let seek = format!("SELECT count(*) FROM t WHERE {pred}");
            let scan = format!("SELECT count(*) FROM t NOT INDEXED WHERE {pred}");
            assert!(seeks(&conn, &seek, "t", "tx"), "premise: {seek} seeks tx: {:?}", plan(&conn, &seek));
            assert!(scans(&conn, &scan, "t"), "premise: {scan} scans: {:?}", plan(&conn, &scan));
            let by_scan = count(&conn, &scan, None);
            assert!(by_scan.is_err(), "premise: the scan raises for {pred}: {by_scan:?}");
            let by_seek = count(&conn, &seek, None);
            assert!(
                by_seek.is_err(),
                "CLAIM: the seek answered {by_seek:?} for {pred}, where the scan raises {by_scan:?}"
            );
        }
    }

    /// The crash batch, item 8 (lead, 2026-10-15; the wire's 4 reds at artie 512c7c17bb wait on it):
    /// FLOAT4 and FLOAT8 are built-in types of a STRICT table, base real with REAL affinity, as
    /// boolean and uuid are; `CREATE TABLE fw(r FLOAT4) STRICT` was refused "unknown datatype".
    #[test]
    fn float4_and_float8_are_builtin_types_of_a_strict_table() {
        let conn = open();
        conn.execute("CREATE TABLE fw(r FLOAT4, d float8) STRICT").unwrap();
        conn.execute("INSERT INTO fw VALUES (1.5, 2.25)").unwrap();
        conn.execute("INSERT INTO fw VALUES (3, 4)").unwrap();
        let rows = conn
            .prepare("SELECT r, d, typeof(r), typeof(d) FROM fw ORDER BY r")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Value::from_f64(1.5), Value::from_f64(2.25), Value::build_text("real"), Value::build_text("real")],
                vec![Value::from_f64(3.0), Value::from_f64(4.0), Value::build_text("real"), Value::build_text("real")],
            ]
        );
    }

    /// C32 MED (a Claude review, 2026-10-15): FLOAT4 and FLOAT8 are BASE-only custom types (no
    /// ENCODE), and a CAST to one emitted nothing, so CAST('1.5' AS float8) stayed text. A CAST to a
    /// BASE-only type casts by its primitive base's affinity. Expected values from sqlite3 3.51.0
    /// (/usr/bin/sqlite3, 2025-06-12): `SELECT typeof(CAST('1.5' AS float8)), CAST('1.5' AS float8),
    /// typeof(CAST('1.5' AS float4)), CAST('1.5' AS float4), typeof(CAST(2 AS float8)),
    /// CAST(2 AS float8)` answered `real|1.5|real|1.5|real|2.0`.
    #[test]
    fn a_cast_to_a_base_only_type_casts_by_its_base() {
        let conn = open();
        let rows = conn
            .prepare(
                "SELECT typeof(CAST('1.5' AS float8)), CAST('1.5' AS float8), typeof(CAST('1.5' AS float4)), \
                 CAST('1.5' AS float4), typeof(CAST(2 AS float8)), CAST(2 AS float8)",
            )
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(
            rows,
            vec![vec![
                Value::build_text("real"),
                Value::from_f64(1.5),
                Value::build_text("real"),
                Value::from_f64(1.5),
                Value::build_text("real"),
                Value::from_f64(2.0),
            ]]
        );
    }

    /// Engine review 20 item 9: 1d8bf442e's catch region covered only types with a function '=', so
    /// a built-in with a RAISE in its ENCODE, a bare '<' and no '=' (varchar(n), smallint, the
    /// date/time types) still raised through an indexed equality: `v = 'abcdef'` on varchar(3)
    /// raised 'value too long', `s = 40000` on smallint 'out of range', where a scan, and
    /// PostgreSQL, answer 0 rows. Both through `answers_by_both_plans`. Mutant
    /// `seek_key_encodes_raising_plain`.
    #[test]
    fn an_indexed_equality_on_a_length_or_range_checked_built_in_answers_as_a_scan_does() {
        let conn = open();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v varchar(3), s smallint) STRICT")
            .unwrap();
        conn.execute("CREATE INDEX tv ON t(v)").unwrap();
        conn.execute("CREATE INDEX ts ON t(s)").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'abc', 7)").unwrap();
        answers_by_both_plans(
            &conn,
            "t",
            "tv",
            &[
                ("v = 'abc'", None, 1),
                ("v = 'abcdef'", None, 0),
                ("v = ?1", Some(Value::build_text("abcdef")), 0),
            ],
        );
        answers_by_both_plans(
            &conn,
            "t",
            "ts",
            &[
                ("s = 7", None, 1),
                ("s = 40000", None, 0),
                ("s = ?1", Some(Value::from_i64(40000)), 0),
            ],
        );
    }

    /// Engine review 20 LOW 27 (b): 150a70db0's arm, a type whose value input is `any` (numeric)
    /// takes any non-literal constant operand but a COLLATE one, had no test: `x = abs(-1.5)` and
    /// `x = 1 + 1` on numeric(10, 2) find 1.50 and 2.00, through the type's '=', as a scan.
    /// Mutant `any_input_takes_no_constant` (the arm off).
    #[test]
    fn a_numeric_column_compares_a_constant_expression_through_its_operator() {
        let conn = open();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 1.5), (2, 2), (3, 7)").unwrap();
        for (pred, want) in [("x = abs(-1.5)", 1), ("x = 1 + 1", 1), ("x = abs(-7) + 0", 1)] {
            let got = count(&conn, &format!("SELECT count(*) FROM t WHERE {pred}"), None);
            assert!(matches!(got, Ok(n) if n == want), "CLAIM: {pred} answered {got:?}, not {want}");
        }
    }

    /// Engine review 20 LOW 27 (a): the optimizer half of 1d8bf442e (an equality on a column whose
    /// type has a function '=' leaves its WHERE term to re-check each row the seek returns) had no
    /// discriminating mutant. A TEXT join column against an indexed numeric column takes the plain
    /// comparison in the WHERE, which the seek's encoded key does not decide: the indexed join
    /// answers as the NOT INDEXED one. Mutant `seek_term_consumed`.
    #[test]
    fn a_text_join_against_an_indexed_numeric_column_answers_as_a_scan_does() {
        let conn = open();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT")
            .unwrap();
        conn.execute("CREATE INDEX tx ON t(x)").unwrap();
        conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, s TEXT)").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 1.5), (2, 2)").unwrap();
        conn.execute("INSERT INTO u VALUES (1, '1.5'), (2, '1.50'), (3, '2'), (4, 'x')").unwrap();
        let joined = count(&conn, "SELECT count(*) FROM u JOIN t ON t.x = u.s", None);
        let scanned = count(&conn, "SELECT count(*) FROM u JOIN t NOT INDEXED ON t.x = u.s", None);
        assert!(scanned.is_ok(), "premise: the scan answers: {scanned:?}");
        assert_eq!(
            joined.ok(),
            scanned.ok(),
            "CLAIM: the indexed join answered differently from the scan ({:?})",
            plan(&conn, "SELECT count(*) FROM u JOIN t ON t.x = u.s")
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
    /// Mutant `seek_key_encodes_raising`. FLAGGED TEST EDIT (engine review 20 HIGH 3, as review 14
    /// MED 6 prescribed): both types gained a bare `OPERATOR '<'`, without which no index can be
    /// created on them (index.rs refuses it), so the test panicked at its CREATE INDEX at its
    /// parent and at its fix alike; the CREATE INDEX is now an asserted premise.
    ///
    /// FLAGGED TEST EDIT (engine review 20 item 8, a premise the fix changes; for the lead's
    /// ruling): both types' '=' is instr, which does not agree with equality of the encodings, so
    /// since aeb76c261 the index is refused for these equalities and the premise "the default plan
    /// seeks the index" cannot hold. The arms assert the opposite premise and the same answers
    /// (`answers_unindexed_by_both_plans`). The claim (an over-length operand compares, false, and
    /// raises nothing) is unchanged; the seek-path RAISE half it exercised is now reachable in core
    /// only through a type whose '=' is allowlisted (none of core's built-ins RAISEs in its ENCODE;
    /// the wire's bpchar does, postgres/ tests).
    #[test]
    fn an_indexed_length_checked_type_compares_an_over_length_operand() {
        let conn = open();
        conn.execute(
            "CREATE TYPE tag(value text, maxlen integer) BASE text ENCODE CASE WHEN maxlen IS NULL \
             THEN value WHEN length(value) <= maxlen THEN value ELSE RAISE(ABORT, 'value too long \
             for type tag') END DECODE value OPERATOR '<' OPERATOR '=' instr",
        )
        .unwrap();
        register_built_in(
            &conn,
            "CREATE TYPE bpc(value text, maxlen integer) BASE text ENCODE CASE WHEN length(value) \
             <= maxlen THEN value ELSE RAISE(ABORT, 'value too long for type bpc') END DECODE \
             value OPERATOR '<' OPERATOR '=' instr",
        );
        for (table, ty, index) in [("t", "tag(3)", "tv"), ("u", "bpc(3)", "uv")] {
            conn.execute(format!(
                "CREATE TABLE {table}(id INTEGER PRIMARY KEY, v {ty}) STRICT"
            ))
            .unwrap();
            let indexed = conn.execute(format!("CREATE UNIQUE INDEX {index} ON {table}(v)"));
            assert!(
                indexed.is_ok(),
                "premise: {ty} takes an index (a bare OPERATOR '<' orders it): {indexed:?}"
            );
            conn.execute(format!("INSERT INTO {table} VALUES (1, 'abc')"))
                .unwrap();
            assert!(
                conn.execute(format!("INSERT INTO {table} VALUES (2, 'abcdef')"))
                    .is_err(),
                "premise: {ty}'s own length check refuses an over-length value"
            );
            answers_unindexed_by_both_plans(
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

    /// Engine review 14 LOW 14: an IN list never consulted a custom type's operator (condition.rs
    /// `translate_in_list` compares each element with a plain Eq), so `w IN ('b')` and
    /// `x IN (?1)` answered differently from `w = 'b'` and `x = ?1`, for literals and parameters
    /// alike. An IN list answers as the OR of its elements' `=`: on tag (whose '=' is instr)
    /// `w IN ('b')` finds 'abc' as `w = 'b'` does, NOT IN inverts it, and numeric's IN agrees
    /// with its '='. Mutant `in_list_skips_type_operator`.
    #[test]
    fn an_in_list_compares_with_the_types_operator() {
        let conn = open();
        conn.execute(
            "CREATE TYPE tag(value text, maxlen integer) BASE text ENCODE CASE WHEN maxlen IS NULL \
             THEN value WHEN length(value) <= maxlen THEN value ELSE RAISE(ABORT, 'value too long \
             for type tag') END DECODE value OPERATOR '=' instr",
        )
        .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2), w tag(3)) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 1.5, 'abc')").unwrap();
        for (eq, in_list, param) in [
            ("w = 'b'", "w IN ('b')", None),
            ("w = 'b'", "w IN ('zz', 'b')", None),
            ("w = ?1", "w IN (?1)", Some(Value::build_text("b"))),
            ("NOT (w = 'b')", "w NOT IN ('b')", None),
            ("x = ?1", "x IN (?1)", Some(Value::from_f64(1.5))),
            ("x = 1.5", "x IN (7, 1.5)", None),
            ("x = 1.501", "x IN (1.501)", None),
        ] {
            let by_eq = count(
                &conn,
                &format!("SELECT count(*) FROM t WHERE {eq}"),
                param.clone(),
            );
            let by_in = count(
                &conn,
                &format!("SELECT count(*) FROM t WHERE {in_list}"),
                param.clone(),
            );
            assert!(by_eq.is_ok(), "premise: {eq} answers: {by_eq:?}");
            assert!(
                matches!((&by_in, &by_eq), (Ok(a), Ok(b)) if a == b),
                "{in_list} answered {by_in:?}, where {eq} answers {by_eq:?}"
            );
        }
        assert!(
            matches!(count(&conn, "SELECT count(*) FROM t WHERE w = 'b'", None), Ok(1)),
            "premise: tag's '=' (instr) finds 'b' in 'abc'"
        );
    }

    /// Engine review 20 item 7: after fa3f83cdc an IN list compares through the type's '=', but
    /// with an index on the column the planner still chose an IN seek, which compares the raw
    /// elements with the ENCODEd index keys: numeric's `x IN (7, 1.5)` found nothing where `x =
    /// 1.5` finds the row, and tag's `w IN ('zz', 'b')` compared bytes where its '=' is instr. With
    /// both columns indexed (tag given a bare `OPERATOR '<'`, without which no index is allowed),
    /// each IN list answers as its elements' `=` do. Mutant `in_seek_on_custom_type`.
    #[test]
    fn an_indexed_in_list_answers_as_the_types_eq_does() {
        let conn = open();
        conn.execute(
            "CREATE TYPE tag(value text, maxlen integer) BASE text ENCODE CASE WHEN maxlen IS NULL \
             THEN value WHEN length(value) <= maxlen THEN value ELSE RAISE(ABORT, 'value too long \
             for type tag') END DECODE value OPERATOR '<' OPERATOR '=' instr",
        )
        .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2), w tag(3)) STRICT")
            .unwrap();
        conn.execute("CREATE INDEX tx ON t(x)")
            .unwrap_or_else(|e| panic!("premise: numeric's column takes an index: {e}"));
        conn.execute("CREATE INDEX tw ON t(w)")
            .unwrap_or_else(|e| panic!("premise: tag's column takes an index: {e}"));
        conn.execute("INSERT INTO t VALUES (1, 1.5, 'abc')").unwrap();
        for (eq, in_list, param) in [
            ("x = 1.5", "x IN (7, 1.5)", None),
            ("x = ?1", "x IN (?1)", Some(Value::from_f64(1.5))),
            ("w = 'b'", "w IN ('zz', 'b')", None),
        ] {
            let by_eq = count(&conn, &format!("SELECT count(*) FROM t WHERE {eq}"), param.clone());
            let by_in = count(&conn, &format!("SELECT count(*) FROM t WHERE {in_list}"), param.clone());
            assert!(matches!(by_eq, Ok(1)), "premise: {eq} finds the row: {by_eq:?}");
            assert!(
                matches!(by_in, Ok(1)),
                "CLAIM: {in_list} answered {by_in:?} with the column indexed, where {eq} finds the row ({:?})",
                plan(&conn, &format!("SELECT count(*) FROM t WHERE {in_list}"))
            );
        }
    }

    /// Engine review 20 item 8: an equality may use an index only where the type's '=' agrees with
    /// equality of the encodings, which the index compares. tag's '=' is instr, coarser than byte
    /// equality: with tag's column indexed, `w = 'b'` sought the key 'b', found nothing and the
    /// re-check could only remove rows, while the scan (and the '=') finds 'abc'. The indexed
    /// equality answers as the scan does. Mutant `eq_index_ignores_coarse_eq`.
    #[test]
    fn an_indexed_equality_under_a_coarser_eq_answers_as_the_scan_does() {
        let conn = open();
        conn.execute(
            "CREATE TYPE tag(value text, maxlen integer) BASE text ENCODE CASE WHEN maxlen IS NULL \
             THEN value WHEN length(value) <= maxlen THEN value ELSE RAISE(ABORT, 'value too long \
             for type tag') END DECODE value OPERATOR '<' OPERATOR '=' instr",
        )
        .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, w tag(3)) STRICT").unwrap();
        conn.execute("CREATE INDEX tw ON t(w)")
            .unwrap_or_else(|e| panic!("premise: tag's column takes an index: {e}"));
        conn.execute("INSERT INTO t VALUES (1, 'abc')").unwrap();
        for (pred, param) in [("w = 'b'", None), ("w = ?1", Some(Value::build_text("b")))] {
            let by_scan = count(&conn, &format!("SELECT count(*) FROM t NOT INDEXED WHERE {pred}"), param.clone());
            assert!(matches!(by_scan, Ok(1)), "premise: the scan finds 'abc' for {pred} (instr): {by_scan:?}");
            let sql = format!("SELECT count(*) FROM t WHERE {pred}");
            let by_plan = count(&conn, &sql, param.clone());
            assert!(
                matches!(by_plan, Ok(1)),
                "CLAIM: {pred} answered {by_plan:?} with the column indexed, where the scan finds the row ({:?})",
                plan(&conn, &sql)
            );
        }
    }

    /// Engine review 16 MED 9: with the operand passed to numeric's operators as given
    /// (2fa04254c), arithmetic with a literal takes the literal's own scale: 10.00 * 3 is 30.00
    /// (PostgreSQL's scale, the sum of the operands'), not 30.0000 from the literal encoded as
    /// 3.00. The UPDATE arms of `a_numeric_operand_keeps_its_own_precision` store into
    /// numeric(10, 2), which hides the result's scale; this reads it in the SELECT list. Division
    /// is `numeric_division_takes_postgresqls_result_scale`'s (its first row is this table's
    /// 100.00 / 10). Mutant `operand_keeps_column_typmod`.
    ///
    /// FLAGGED TEST EDIT, ruled ACCEPTED (engine review 21 MED 12 option (a), fastest DECISIONS
    /// 2026-10-15T04:28:01Z): the `x / 10` row is deleted. Since 4d31d483c it panicked on the
    /// loop's third row, so `x - 2` and `x * ?1` never ran, and it failed with or without the
    /// mutant, proving no kill.
    #[test]
    fn numeric_arithmetic_in_the_select_list_keeps_the_operands_scale() {
        let conn = open();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10), (2, 100), (3, -5)")
            .unwrap();
        for (sql, param, want) in [
            ("SELECT x * 3 FROM t WHERE id = 1", None, "30.00"),
            ("SELECT x * 3 FROM t WHERE id = 3", None, "-15.00"),
            ("SELECT x - 2 FROM t WHERE id = 2", None, "98.00"),
            (
                "SELECT x * ?1 FROM t WHERE id = 1",
                Some(Value::from_f64(1.075)),
                "10.75000",
            ),
        ] {
            let got = conn.prepare(sql).and_then(|mut stmt| {
                if let Some(value) = param {
                    stmt.bind_at(1.try_into().unwrap(), value)?;
                }
                stmt.run_collect_rows()
            });
            assert!(
                matches!(&got, Ok(rows) if rows.len() == 1 && rows[0][0].to_text() == Some(want)),
                "{sql} gave {got:?}, not {want}"
            );
        }
    }

    /// Lead ruling 2026-10-09 on e52f01422 (a FIX item): numeric division takes PostgreSQL's
    /// result scale (`select_div_scale` in numeric.c: at least NUMERIC_MIN_SIG_DIGITS = 16
    /// significant digits, never below either operand's display scale, rounded half away from
    /// zero), not bigdecimal's. The expectations are DERIVED from PostgreSQL's select_div_scale by
    /// reading its source, not from our output (100.00 / 10: the first base-10000 digits give a
    /// quotient weight of 0, so 16 decimal places; 1.00 / 3: 1 <= 3 lowers the weight to -1, so
    /// 20). CONFIRMED by PostgreSQL 18.6 for the same queries, verbatim (artie raw
    /// lanes/engine/raw/rpgdiv-pg18.6.txt, 2026-10-15): every expectation below is its output.
    /// Mutant `div_scale_bigdecimal`.
    #[test]
    fn numeric_division_takes_postgresqls_result_scale() {
        let conn = open();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 100), (2, 30), (3, 1), (4, -2)")
            .unwrap();
        for (sql, want) in [
            ("SELECT x / 10 FROM t WHERE id = 1", "10.0000000000000000"),
            ("SELECT x / 10 FROM t WHERE id = 2", "3.0000000000000000"),
            ("SELECT x / 3 FROM t WHERE id = 3", "0.33333333333333333333"),
            ("SELECT x / 3 FROM t WHERE id = 4", "-0.66666666666666666667"),
        ] {
            let got = conn.prepare(sql).and_then(|mut stmt| stmt.run_collect_rows());
            assert!(
                matches!(&got, Ok(rows) if rows.len() == 1 && rows[0][0].to_text() == Some(want)),
                "CLAIM: {sql} gave {got:?}, not PostgreSQL's {want}"
            );
        }
    }

    /// Engine review 21 MED 13's rows that no in-tree regression file covered, from PostgreSQL 18.6
    /// verbatim (artie raw lanes/engine/raw/rpgdiv-pg18.6.txt): an operand below 1 (the weight's
    /// div_euclid), an exact quotient padded to the result scale, an operand of display scale 1, and
    /// one of display scale above 16 (the result takes it). A display scale above 1000 has no row:
    /// PostgreSQL refuses numeric(1200, 1100) ("precision 1200 must be between 1 and 1000"), so its
    /// arm needs another construction. Mutant `div_scale_bigdecimal`.
    #[test]
    fn numeric_division_matches_postgresql_18() {
        let conn = open();
        // Each column holds the PostgreSQL query's dividend at its display scale (`0.5::numeric(10,
        // 2)`, `1::numeric`, `2.5::numeric(10,1)`, `1.00000000000000000001::numeric`).
        conn.execute(
            "CREATE TABLE d(id INTEGER PRIMARY KEY, a numeric(10, 2), b numeric(10, 0), c numeric(10, 1), \
             e numeric(30, 20)) STRICT",
        )
        .unwrap();
        conn.execute("INSERT INTO d VALUES (1, '0.5', '1', '2.5', '1.00000000000000000001')")
            .unwrap();
        for (sql, want) in [
            ("SELECT a / 3 FROM d", "0.16666666666666666667"),
            ("SELECT b / 8 FROM d", "0.12500000000000000000"),
            ("SELECT c / 2 FROM d", "1.25000000000000000000"),
            ("SELECT e / 3 FROM d", "0.33333333333333333334"),
        ] {
            let got = conn.prepare(sql).and_then(|mut stmt| stmt.run_collect_rows());
            assert!(
                matches!(&got, Ok(rows) if rows.len() == 1 && rows[0][0].to_text() == Some(want)),
                "CLAIM: {sql} gave {got:?}, not PostgreSQL 18's {want}"
            );
        }
    }

    /// Engine review 21 MED 13: the division red's four rows never had equal first digits, a zero
    /// dividend, a zero divisor or a quotient past 10^17, so several wrong ports passed it. These
    /// rows' expectations are PostgreSQL's own output, copied verbatim from the in-tree regression
    /// file postgres/conformance/upstream/numeric.out (the quot column at lines 737-754, and the
    /// queries at 2781, 2829 and 2841; division by zero at 791), with the dividend's display scale
    /// carried by the column's scale and the divisor given as text, as the literals are there. Not
    /// covered by any row in that file, so owed to the PostgreSQL 18 run (rpgdiv): an operand below
    /// 1 (the weight's div_euclid), an exact rounding tie, an operand display scale above 16 and
    /// one above 1000. Mutant `div_scale_first_digit_strict` (test builds only).
    #[test]
    fn numeric_division_matches_postgresqls_regression_output() {
        let conn = open();
        conn.execute(
            "CREATE TABLE d(id INTEGER PRIMARY KEY, i numeric(25, 0), f numeric(25, 1)) STRICT",
        )
        .unwrap();
        conn.execute(
            // The 21- and 20-digit dividends as text: as integer literals they would be read as reals.
            "INSERT INTO d VALUES (1, 0, 4.2), (2, 1, 70.0), (3, -1, 4.2), \
             (4, '999999999999999999999', 4.2), (5, '12345678901234567890', 4.2)",
        )
        .unwrap();
        for (sql, want) in [
            ("SELECT i / '1' FROM d WHERE id = 1", Some("0.00000000000000000000")),
            ("SELECT i / '1' FROM d WHERE id = 2", Some("1.00000000000000000000")),
            ("SELECT i / '1' FROM d WHERE id = 3", Some("-1.00000000000000000000")),
            ("SELECT f / '1' FROM d WHERE id = 1", Some("4.2000000000000000")),
            ("SELECT f / '-1' FROM d WHERE id = 1", Some("-4.2000000000000000")),
            ("SELECT i / '4.2' FROM d WHERE id = 2", Some("0.23809523809523809524")),
            ("SELECT i / '4.2' FROM d WHERE id = 3", Some("-0.23809523809523809524")),
            ("SELECT f / '4.2' FROM d WHERE id = 1", Some("1.00000000000000000000")),
            ("SELECT f / '70' FROM d WHERE id = 2", Some("1.00000000000000000000")),
            ("SELECT i / '1000000000000000000000' FROM d WHERE id = 4", Some("1.00000000000000000000")),
            ("SELECT i / '123' FROM d WHERE id = 5", Some("100371373180768845")),
            ("SELECT i / '0' FROM d WHERE id = 1", None),
        ] {
            let got = conn.prepare(sql).and_then(|mut stmt| stmt.run_collect_rows());
            match want {
                Some(want) => assert!(
                    matches!(&got, Ok(rows) if rows.len() == 1 && rows[0][0].to_text() == Some(want)),
                    "CLAIM: {sql} gave {got:?}, not PostgreSQL's {want}"
                ),
                None => assert!(
                    matches!(&got, Err(e) if e.to_string().contains("division by zero")),
                    "CLAIM: {sql} gave {got:?}, not PostgreSQL's \"division by zero\""
                ),
            }
        }
    }

    /// Engine review 21 LOW 24, a REGISTERED DIVERGENCE (fastest DECISIONS), red until fixed: a
    /// real literal reaches numeric's operators as a REAL (SQLite's literal), and its decimal comes
    /// from `f64::to_string`, which drops the literal's trailing zeros, so `1.50` takes display scale
    /// 1, not PostgreSQL's 2. Multiplication shows it: PostgreSQL's numeric_mul gives the product
    /// the sum of the operands' display scales (numeric.c, read, not run: rscale = var1.dscale +
    /// var2.dscale), so numeric(10, 2) 10.00 * 1.50 is 15.0000, where this engine answers 15.000.
    /// The text literal '1.50' is the control. LOUD-OWED: both rows from PostgreSQL 18 (rpgdiv).
    #[test]
    fn a_real_literal_operand_keeps_its_display_scale() {
        let conn = open();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        for (sql, want) in [
            ("SELECT x * '1.50' FROM t WHERE id = 1", "15.0000"),
            ("SELECT x * 1.50 FROM t WHERE id = 1", "15.0000"),
        ] {
            let got = conn.prepare(sql).and_then(|mut stmt| stmt.run_collect_rows());
            assert!(
                matches!(&got, Ok(rows) if rows.len() == 1 && rows[0][0].to_text() == Some(want)),
                "{sql} gave {got:?}, not PostgreSQL's {want}"
            );
        }
    }

    // PG18.6: postgres/cli/tests/c50-pg18-literal-scale.txt. The ordinary
    // SQLite and hexadecimal controls keep their existing numeric value semantics.
    #[test]
    fn c50_custom_numeric_literals_preserve_recorded_scale() {
        let conn = open();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        for (expr, want) in [
            ("1.50 * x", "15.0000"),
            ("x * -(1.50)", "-15.0000"),
            ("(-(-1.50)) * x", "15.0000"),
            ("x * +(1.50)", "15.0000"),
            ("x * 1.500e0", "15.00000"),
            ("x * 1_0.50", "105.0000"),
            ("x * 9007199254740993", "90071992547409930.00"),
            ("x * 0.000", "0.00000"),
            ("x + 1.5000", "11.5000"),
            ("x - 1.5000", "8.5000"),
            ("x / 1.50", "6.6666666666666667"),
            ("x * 0xFFFFFFFFFFFFFFFF", "-10.00"),
        ] {
            let sql = format!("SELECT {expr} FROM t WHERE id = 1");
            let rows = conn.prepare(&sql).unwrap().run_collect_rows().unwrap();
            assert_eq!(rows.len(), 1, "{sql}");
            assert_eq!(rows[0][0].to_text(), Some(want), "{sql}");
        }
        let rows = conn
            .prepare("SELECT typeof(1.50), typeof(1.50 * 2), 1.50 * 2")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows[0][0].to_text(), Some("real"));
        assert_eq!(rows[0][1].to_text(), Some("real"));
        assert_eq!(rows[0][2], Value::from_f64(3.0));
    }

    /// Engine review 21 MED 10: a numeric operand's scale was bounded only by i64, so
    /// `x / '1e-2000000000'` built 10^(2*10^9) inside one division, with no interrupt point, and
    /// `'1e9223372036854775807' / x` overflowed i64 (a panic in a debug build, a wrapped value in a
    /// release one). An operand outside PostgreSQL's numeric range is refused as PostgreSQL
    /// refuses it, "value overflows numeric format", within 10 s: a literal, a bound parameter,
    /// either side. Each runs on a thread of its own: a run that never ends is the red.
    #[test]
    fn a_numeric_operand_outside_postgresqls_range_is_refused_at_once() {
        for (sql, param) in [
            ("SELECT x / '1e-2000000000' FROM t", None),
            ("SELECT x / ?1 FROM t", Some("1e-5000000000")),
            ("SELECT '1e9223372036854775807' / x FROM t", None),
        ] {
            let (tx, rx) = std::sync::mpsc::channel();
            let (owned, param) = (sql.to_string(), param.map(str::to_string));
            std::thread::spawn(move || {
                let got = (|| -> crate::Result<()> {
                    let conn = open();
                    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT")?;
                    conn.execute("INSERT INTO t VALUES (1, 100)")?;
                    let mut stmt = conn.prepare(&owned)?;
                    if let Some(p) = param {
                        stmt.bind_at(1.try_into().unwrap(), Value::build_text(p))?;
                    }
                    stmt.run_collect_rows().map(|_| ())
                })();
                let _ = tx.send(got.map_err(|e| e.to_string()));
            });
            let got = rx.recv_timeout(std::time::Duration::from_secs(10));
            assert!(
                matches!(&got, Ok(Err(m)) if m.contains("value overflows numeric format")),
                "CLAIM: {sql} gave {got:?} (a timeout: it never finished), not PostgreSQL's refusal"
            );
        }
    }

    /// The domain CHECK identifier class (lead order, from the ultrareview fix session): a domain CHECK
    /// written with `VALUE` (any case but lower) was SILENTLY not enforced, because its `value` was
    /// bound under the exact key "value" and looked up by the identifier as written. PostgreSQL folds
    /// the identifier: VALUE, value, Value and "value" all enforce (PG 18.6, artie raw
    /// frontier/fastest/lanes/engine/raw/pg18-domain-check-case.txt 6e02bd34ec): CAST(-5 AS d) is
    /// refused (23514), CAST(5 AS d) is 5, and an INSERT of -5 into a column of the domain is refused.
    /// The same map binds a type's parameters in its ENCODE: a parameter declared `Scale` and used
    /// as `scale` must bind too (identifiers are case-insensitive here).
    #[test]
    fn a_domain_check_binds_value_in_any_case() {
        let mut bad = Vec::new();
        for spelling in ["VALUE", "value", "Value", "\"value\""] {
            let conn = open();
            let ddl = format!("CREATE DOMAIN d AS integer CHECK ({spelling} > 0)");
            if let Err(e) = conn.execute(&ddl) {
                bad.push(format!("{ddl} refused: {e}"));
                continue;
            }
            let neg = conn.prepare("SELECT CAST(-5 AS d)").and_then(|mut s| s.run_collect_rows());
            if !matches!(&neg, Err(e) if e.to_string().contains("violates check constraint")) {
                bad.push(format!("{spelling}: CAST(-5 AS d) gave {neg:?}, not PostgreSQL's 23514"));
            }
            let pos = conn.prepare("SELECT CAST(5 AS d)").and_then(|mut s| s.run_collect_rows());
            if !matches!(&pos, Ok(rows) if rows.len() == 1) {
                bad.push(format!("{spelling}: CAST(5 AS d) gave {pos:?}, not PostgreSQL's 5"));
            }
            let _ = conn.execute("CREATE TABLE dt(id INTEGER PRIMARY KEY, x d) STRICT");
            // Refused, as PostgreSQL refuses it. Its message here is the table CHECK's ("CHECK
            // constraint failed: d_0", not PG's "violates check constraint"): a separate message
            // divergence, reported, not this class.
            let ins = conn.execute("INSERT INTO dt VALUES (1, -5)");
            if ins.is_ok() {
                bad.push(format!("{spelling}: INSERT of -5 was accepted, where PostgreSQL refuses it (23514)"));
            }
        }
        let conn = open();
        let ddl = "CREATE TYPE pct(value any, Scale integer) BASE integer ENCODE value * scale DECODE value";
        match conn.execute(ddl) {
            Err(e) => bad.push(format!("{ddl} refused: {e}")),
            Ok(()) => {
                let got = conn
                    .execute("CREATE TABLE pt(id INTEGER PRIMARY KEY, x pct(10)) STRICT")
                    .and_then(|()| conn.execute("INSERT INTO pt VALUES (1, 3)"))
                    .and_then(|()| conn.prepare("SELECT CAST(x AS TEXT) FROM pt").and_then(|mut s| s.run_collect_rows()));
                if !matches!(&got, Ok(rows) if rows.len() == 1 && rows[0][0].to_text() == Some("30")) {
                    bad.push(format!("a parameter declared Scale and used as scale: {got:?}, not 30"));
                }
            }
        }
        assert!(bad.is_empty(), "CLAIM: an identifier bound by a custom type or domain matched only in its exact case: {bad:?}");
    }

    /// The lead's correction to `a_domain_check_binds_value_in_any_case`: PostgreSQL's exact claim for a
    /// failed domain CHECK, registered as a red of its own. PG 18.6 (artie raw frontier/fastest/lanes/
    /// engine/raw/pg18-domain-check-message.txt 0673f158a3) fails a CAST, an INSERT and an UPDATE alike
    /// with SQLSTATE 23514 and `value for domain <d> violates check constraint "<name>"`, the name being
    /// the constraint's own, or `<d>_check`, `<d>_check1`, ... in declaration order. Here: the message
    /// and the name, exactly; the SQLSTATE is the wire's (postgres/server `sqlstate`).
    #[test]
    fn a_failed_domain_check_reports_postgresqls_message_and_name() {
        let mut bad = Vec::new();
        let conn = open();
        for ddl in [
            "CREATE DOMAIN dm AS integer CHECK (VALUE > 0)",
            "CREATE DOMAIN d2 AS integer CHECK (VALUE > 0) CHECK (VALUE < 100)",
            "CREATE DOMAIN dn AS integer CONSTRAINT positive CHECK (VALUE > 0)",
            "CREATE TABLE tm(id INTEGER PRIMARY KEY, x dm) STRICT",
            "CREATE TABLE tn(id INTEGER PRIMARY KEY, x dn) STRICT",
            "INSERT INTO tm VALUES (2, 5)",
        ] {
            conn.execute(ddl).unwrap();
        }
        for (sql, want) in [
            ("SELECT CAST(-5 AS dm)", r#"value for domain dm violates check constraint "dm_check""#),
            ("INSERT INTO tm VALUES (1, -5)", r#"value for domain dm violates check constraint "dm_check""#),
            ("UPDATE tm SET x = -1 WHERE id = 2", r#"value for domain dm violates check constraint "dm_check""#),
            ("SELECT CAST(500 AS d2)", r#"value for domain d2 violates check constraint "d2_check1""#),
            ("SELECT CAST(-1 AS d2)", r#"value for domain d2 violates check constraint "d2_check""#),
            ("SELECT CAST(-5 AS dn)", r#"value for domain dn violates check constraint "positive""#),
            ("INSERT INTO tn VALUES (1, -5)", r#"value for domain dn violates check constraint "positive""#),
        ] {
            let got = conn.prepare(sql).and_then(|mut s| s.run_collect_rows()).map(|_| ());
            match &got {
                Err(e) if e.to_string() == want => {}
                other => bad.push(format!("{sql} gave {other:?}, not PostgreSQL 18's {want:?}")),
            }
        }
        assert!(bad.is_empty(), "CLAIM: a failed domain CHECK did not report PostgreSQL's message and constraint name: {bad:?}");
    }

    /// Runs `steps` in order on a fresh connection, on a thread of its own: the first error, the last
    /// statement's first column as text, or `Err("timeout")` after 10 s (a red: it never finished).
    fn run_bounded(steps: Vec<String>) -> Result<Option<String>, String> {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let got = (|| -> crate::Result<Option<String>> {
                let conn = open();
                let mut last = None;
                for sql in &steps {
                    let rows = conn.prepare(sql)?.run_collect_rows()?;
                    last = rows.first().and_then(|r| r.first()).and_then(|v| v.to_text().map(str::to_string));
                }
                Ok(last)
            })();
            let _ = tx.send(got.map_err(|e| e.to_string()));
        });
        rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap_or_else(|_| Err("timeout".to_string()))
    }

    /// Review 0e8b1cf18..0a4a127a1 numeric HIGH (DECISIONS d1a8dcb982): numeric(p, s) took any
    /// precision and scale, and an insert rescaled to the scale BEFORE the precision check, so
    /// numeric(2000000000, 2000000000) asked one rescale for 2*10^9 digits. PostgreSQL refuses p
    /// outside 1..1000 and s outside -1000..1000, at CREATE TABLE and at a cast, with 22023 (PG 18.6's
    /// messages for this exact SQL: artie raw frontier/fastest/lanes/engine/raw/
    /// pg18-numeric-typmod-range.txt 7a1d98f8d2, never ours). Control: numeric(1000, 1000) holds 0.5.
    #[test]
    fn a_numeric_type_outside_postgresqls_typmod_range_is_refused() {
        let table = |p: &str| {
            vec![
                format!("CREATE TABLE a(id INTEGER PRIMARY KEY, x numeric({p})) STRICT"),
                "INSERT INTO a VALUES (1, 1)".to_string(),
            ]
        };
        let mut bad = Vec::new();
        for (steps, want) in [
            (table("1001, 0"), "NUMERIC precision 1001 must be between 1 and 1000"),
            (table("0, 0"), "NUMERIC precision 0 must be between 1 and 1000"),
            (table("1000, 1001"), "NUMERIC scale 1001 must be between -1000 and 1000"),
            (table("2000000000, 2000000000"), "NUMERIC precision 2000000000 must be between 1 and 1000"),
            (vec!["SELECT CAST(1 AS numeric(1001, 0))".to_string()], "NUMERIC precision 1001 must be between 1 and 1000"),
            (vec!["SELECT CAST(1 AS numeric(1000, 1001))".to_string()], "NUMERIC scale 1001 must be between -1000 and 1000"),
        ] {
            let got = run_bounded(steps.clone());
            if !matches!(&got, Err(m) if m.contains(want)) {
                bad.push(format!("{steps:?} gave {got:?}, not PostgreSQL 18's {want:?}"));
            }
        }
        let control = run_bounded(vec![
            "CREATE TABLE w(id INTEGER PRIMARY KEY, x numeric(1000, 1000)) STRICT".to_string(),
            "INSERT INTO w VALUES (1, '0.5')".to_string(),
            "SELECT CAST(count(*) AS TEXT) FROM w".to_string(),
        ]);
        assert_eq!(control, Ok(Some("1".to_string())), "premise: numeric(1000, 1000) does not hold 0.5, as PostgreSQL's does");
        assert!(bad.is_empty(), "CLAIM: a numeric typmod outside PostgreSQL's range was not refused as PostgreSQL refuses it: {bad:?}");
    }

    /// Found while making numeric one bounded type (class (a), DECISIONS 994f7d41e0): a typmod
    /// rescale TRUNCATED (bigdecimal's `with_scale` drops digits), where PostgreSQL rounds half away
    /// from zero, and a value that rounds past the precision is PostgreSQL's 22003 "numeric field
    /// overflow". PG 18.6's answers for this exact SQL (artie raw frontier/fastest/lanes/engine/raw/
    /// pg18-numeric-typmod-round.txt b4512c63d5): 1.005, -1.005, 1.004, 2.675 into numeric(10, 2)
    /// read 1.01, -1.01, 1.00, 2.68; '-0.125' into numeric(4, 2) is -0.13; 99999999.995 into
    /// numeric(10, 2) overflows; 99999999.994 reads 99999999.99.
    #[test]
    fn a_numeric_typmod_rounds_half_away_from_zero_as_postgresql_does() {
        let mut bad = Vec::new();
        let table = |rows: &str, read: &str| {
            vec![
                "CREATE TABLE r(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT".to_string(),
                format!("INSERT INTO r VALUES {rows}"),
                read.to_string(),
            ]
        };
        for (steps, want) in [
            (table("(1, '1.005')", "SELECT CAST(x AS TEXT) FROM r"), Ok("1.01")),
            (table("(1, '-1.005')", "SELECT CAST(x AS TEXT) FROM r"), Ok("-1.01")),
            (table("(1, '1.004')", "SELECT CAST(x AS TEXT) FROM r"), Ok("1.00")),
            (table("(1, '2.675')", "SELECT CAST(x AS TEXT) FROM r"), Ok("2.68")),
            (table("(1, '99999999.994')", "SELECT CAST(x AS TEXT) FROM r"), Ok("99999999.99")),
            (table("(1, '99999999.995')", "SELECT CAST(x AS TEXT) FROM r"), Err("numeric field overflow")),
            // Through a numeric(4, 2) column (PG 18.6: -0.13, raw acf4b78d7f): a CAST to numeric here
            // yields its ENCODE's blob, not a decoded value, so the CAST form cannot be read back.
            (
                vec![
                    "CREATE TABLE q(id INTEGER PRIMARY KEY, x numeric(4, 2)) STRICT".to_string(),
                    "INSERT INTO q VALUES (1, '-0.125')".to_string(),
                    "SELECT CAST(x AS TEXT) FROM q".to_string(),
                ],
                Ok("-0.13"),
            ),
        ] {
            let got = run_bounded(steps.clone());
            let ok = match (&got, &want) {
                (Ok(Some(v)), Ok(w)) => v == w,
                (Err(m), Err(w)) => m.contains(w),
                _ => false,
            };
            if !ok {
                bad.push(format!("{:?} gave {got:?}, not PostgreSQL 18's {want:?}", &steps[1..]));
            }
        }
        assert!(bad.is_empty(), "CLAIM: a numeric typmod rounded or overflowed unlike PostgreSQL: {bad:?}");
    }

    /// Review C38 MED 2 (artie frontier/fastest/reviews/REVIEW-claude-cards-C27-C39-20261015.md): the
    /// bounded type had no INPUT EXPONENT bound. PostgreSQL refuses an exponent past INT_MAX / 2
    /// (1073741823) with 22003 even for a zero, and an exponent no integer holds alike. PG 18.6's
    /// answers for this exact SQL (artie raw frontier/fastest/lanes/engine/raw/pg18-numeric-exponent.txt
    /// e4c073c95b): '0e1073741823' * x is 0.00; '0e1073741824' * x and '0e9999999999999999999' * x
    /// overflow.
    #[test]
    fn a_numeric_input_exponent_is_bounded_as_postgresql_does() {
        let setup = || {
            vec![
                "CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT".to_string(),
                "INSERT INTO t VALUES (1, 1)".to_string(),
            ]
        };
        let overflow = "value overflows numeric format";
        let mut bad = Vec::new();
        for (sql, want) in [
            ("SELECT '0e1073741823' * x FROM t", Ok("0.00")),
            ("SELECT '0e1073741824' * x FROM t", Err(overflow)),
            ("SELECT '0e9999999999999999999' * x FROM t", Err(overflow)),
        ] {
            let mut steps = setup();
            steps.push(sql.to_string());
            let got = run_bounded(steps);
            let ok = match (&got, &want) {
                (Ok(Some(v)), Ok(w)) => v == w,
                (Err(m), Err(w)) => m.contains(w),
                _ => false,
            };
            if !ok {
                bad.push(format!("{sql} gave {got:?}, not PostgreSQL 18's {want:?}"));
            }
        }
        assert!(bad.is_empty(), "CLAIM: a numeric input exponent outside PostgreSQL's bound was not refused as PostgreSQL refuses it: {bad:?}");
    }

    /// Review C38 MED 1: the class fix's ALTER TABLE ADD COLUMN check refused numeric(p, s) on a
    /// NON-STRICT table, where the name is only a type name with SQLite's affinity and no custom type
    /// applies, and where CREATE TABLE accepts it. The rule must be CREATE TABLE's: the typmod is
    /// bounded only where the column uses the numeric custom type (a STRICT table). Engine
    /// consistency (both statements are this engine's), not a PostgreSQL oracle: a non-STRICT
    /// numeric(1001, 0) is accepted by CREATE TABLE here.
    #[test]
    fn alter_table_add_column_bounds_a_numeric_typmod_only_where_create_table_does() {
        let mut bad = Vec::new();
        for strict in [false, true] {
            let opt = if strict { " STRICT" } else { "" };
            let created = run_bounded(vec![format!("CREATE TABLE c(id INTEGER PRIMARY KEY, y numeric(1001, 0)){opt}")]);
            let altered = run_bounded(vec![
                format!("CREATE TABLE a(id INTEGER PRIMARY KEY){opt}"),
                "ALTER TABLE a ADD COLUMN y numeric(1001, 0)".to_string(),
            ]);
            if created.is_ok() != altered.is_ok() {
                bad.push(format!("strict {strict}: CREATE TABLE gave {created:?} but ALTER TABLE ADD COLUMN gave {altered:?}"));
            }
        }
        assert!(bad.is_empty(), "CLAIM: ALTER TABLE ADD COLUMN and CREATE TABLE disagree on a numeric typmod: {bad:?}");
    }

    /// Class (a) (DECISIONS 994f7d41e0), the typmod's other DDL: ALTER TABLE ADD COLUMN declared a
    /// numeric(p, s) column with no check at all (CREATE TABLE at least counted the parameters).
    /// PG 18.6 refuses numeric(1001, 0) and numeric(1000, 1001) there with its 22023 messages and
    /// adds numeric(10, 2) (artie raw pg18-numeric-typmod-round.txt 048831ffdf, appended).
    #[test]
    fn alter_table_add_column_refuses_a_numeric_typmod_outside_postgresqls_range() {
        let base = || vec!["CREATE TABLE al(id INTEGER PRIMARY KEY) STRICT".to_string()];
        let mut bad = Vec::new();
        for (col, want) in [
            ("y numeric(1001, 0)", Err("NUMERIC precision 1001 must be between 1 and 1000")),
            ("z numeric(1000, 1001)", Err("NUMERIC scale 1001 must be between -1000 and 1000")),
            ("w numeric(10, 2)", Ok(())),
        ] {
            let mut steps = base();
            steps.push(format!("ALTER TABLE al ADD COLUMN {col}"));
            let got = run_bounded(steps);
            let ok = match (&got, &want) {
                (Ok(_), Ok(())) => true,
                (Err(m), Err(w)) => m.contains(w),
                _ => false,
            };
            if !ok {
                bad.push(format!("ADD COLUMN {col} gave {got:?}, not PostgreSQL 18's {want:?}"));
            }
        }
        assert!(bad.is_empty(), "CLAIM: ALTER TABLE ADD COLUMN took a numeric typmod unlike PostgreSQL: {bad:?}");
    }

    /// Review 0e8b1cf18..0a4a127a1 numeric MED 1 (DECISIONS d1a8dcb982): a product's scale was the sum
    /// of the display scales uncapped (PostgreSQL caps it at 16383), and add, sub and mul did not
    /// refuse a result past 131072 integer digits (PostgreSQL: 22003, "value overflows numeric
    /// format"). Expected values are PG 18.6's for this exact SQL over these tables (artie raw
    /// pg18-numeric-typmod-range.txt 7059fd299c): t.x = 1, u.x = 10, both numeric(10, 2). Add and sub
    /// past the range are reached through a 131072-digit text operand: here a product's result does
    /// not stay numeric, so `(a * x) + (b * x)` is SQLite arithmetic, not numeric's.
    #[test]
    fn a_numeric_result_outside_postgresqls_range_is_refused() {
        let setup = || {
            vec![
                "CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT".to_string(),
                "INSERT INTO t VALUES (1, 1)".to_string(),
                "CREATE TABLE u(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT".to_string(),
                "INSERT INTO u VALUES (1, 10)".to_string(),
            ]
        };
        let overflow = "value overflows numeric format";
        // 131072 nines: the most integer digits PostgreSQL holds; one more digit overflows.
        let nines = "9".repeat(131_072);
        let mut bad = Vec::new();
        for (sql, want) in [
            ("SELECT '0e-16383' * x FROM t".to_string(), Ok(16385usize)),
            ("SELECT '1e131070' * x FROM u".to_string(), Ok(131075)),
            ("SELECT '1e131071' * x FROM u".to_string(), Err(overflow)),
            (format!("SELECT '{nines}' + x FROM u"), Err(overflow)),
            (format!("SELECT x + '{nines}' FROM u"), Err(overflow)),
            (format!("SELECT '-{nines}' - x FROM u"), Err(overflow)),
            (format!("SELECT '{nines}' - x FROM u"), Ok(131075)),
        ] {
            let mut steps = setup();
            steps.push(sql.clone());
            let sql: String = sql.chars().take(60).collect();
            let got = run_bounded(steps);
            let ok = match (&got, &want) {
                (Ok(Some(v)), Ok(len)) => v.len() == *len,
                (Err(m), Err(w)) => m.contains(w),
                _ => false,
            };
            if !ok {
                let shown = match &got {
                    Ok(Some(v)) => format!("a value of {} characters ({}...)", v.len(), v.chars().take(24).collect::<String>()),
                    other => format!("{other:?}"),
                };
                bad.push(format!("{sql} gave {shown}, not PostgreSQL 18's {want:?} (a length, or an error)"));
            }
        }
        assert!(bad.is_empty(), "CLAIM: a numeric result outside PostgreSQL's range diverged: {bad:?}");
    }

    /// Found fixing Codex C25 HIGH 3: a numeric product's scale is the sum of its operands' DISPLAY
    /// scales in PostgreSQL (numeric.c `mul_var`'s rscale = dscale1 + dscale2, each at least 0), but
    /// bigdecimal's product of a zero is a plain 0 and a negative-scale operand subtracts from the
    /// scale. Expected values are PostgreSQL 18.6's for this exact SQL over this table (artie raw
    /// frontier/fastest/lanes/engine/raw/pg18-numeric-mul-scale.txt 69b25bea64), never ours.
    #[test]
    fn a_numeric_product_keeps_postgresqls_display_scale() {
        let conn = open();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 1)").unwrap();
        let mut bad = Vec::new();
        for (sql, want) in [
            ("SELECT 0 * x FROM t", "0.00"),
            ("SELECT '0' * x FROM t", "0.00"),
            ("SELECT '1e5' * x FROM t", "100000.00"),
            ("SELECT '0.000' * x FROM t", "0.00000"),
            ("SELECT x * '0e-3' FROM t", "0.00000"),
            ("SELECT '1e5' + x FROM t", "100001.00"),
            ("SELECT '0e5' - x FROM t", "-1.00"),
        ] {
            let got = conn.prepare(sql).and_then(|mut stmt| stmt.run_collect_rows());
            if !matches!(&got, Ok(rows) if rows.len() == 1 && rows[0][0].to_text() == Some(want)) {
                bad.push(format!("{sql} gave {got:?}, not {want}"));
            }
        }
        assert!(bad.is_empty(), "CLAIM: a numeric result's display scale diverged from PostgreSQL 18: {bad:?}");
    }

    /// Codex C25 HIGH 3 (artie frontier/fastest/reviews/REVIEW-codex-11-engine-2f480432a..0e8b1cf18.md):
    /// a numeric ZERO kept any exponent the parser gave it, and the range check passed every zero,
    /// so `'0e-2000000000' / x` asked `pg_numeric_div` for 10^1999999000 (and `'0e1000000' / x` for
    /// 10^1000022). Expected values are PostgreSQL 18.6's for this exact SQL over this table (artie
    /// raw frontier/fastest/lanes/engine/raw/pg18-numeric-zero-scale.txt d034bf749e), never ours:
    /// a zero past 16383 fractional digits is refused, "value overflows numeric format"; a zero with
    /// a positive exponent is 0. Each runs on a thread of its own within 10 s: a timeout is the red.
    #[test]
    fn a_numeric_zero_at_any_scale_computes_as_postgresql_does() {
        let zeros = |n: usize| format!("0.{}", "0".repeat(n));
        let mut bad = Vec::new();
        for (sql, want) in [
            ("SELECT '0e-2000000000' / x FROM t", Err("value overflows numeric format")),
            ("SELECT '0e-16384' / x FROM t", Err("value overflows numeric format")),
            ("SELECT '0e1000000' / x FROM t", Ok(zeros(20))),
            ("SELECT '0e-1000' / x FROM t", Ok(zeros(1000))),
            ("SELECT '0e1000000' * x FROM t", Ok(zeros(2))),
            ("SELECT '0e1000000' + x FROM t", Ok("1.00".to_string())),
        ] {
            let (tx, rx) = std::sync::mpsc::channel();
            let owned = sql.to_string();
            std::thread::spawn(move || {
                let got = (|| -> crate::Result<Option<String>> {
                    let conn = open();
                    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x numeric(10, 2)) STRICT")?;
                    conn.execute("INSERT INTO t VALUES (1, 1)")?;
                    let rows = conn.prepare(&owned)?.run_collect_rows()?;
                    Ok(rows.first().and_then(|r| r[0].to_text().map(str::to_string)))
                })();
                let _ = tx.send(got.map_err(|e| e.to_string()));
            });
            let got = rx.recv_timeout(std::time::Duration::from_secs(10));
            let ok = match (&got, &want) {
                (Ok(Err(m)), Err(w)) => m.contains(w),
                (Ok(Ok(Some(v))), Ok(w)) => v == w,
                _ => false,
            };
            let shown = |g: &Result<Result<Option<String>, String>, _>| {
                format!("{g:?}").chars().take(200).collect::<String>()
            };
            if !ok {
                bad.push(format!("{sql} gave {}, not {}", shown(&got), format!("{want:?}").chars().take(60).collect::<String>()));
            }
        }
        assert!(bad.is_empty(), "CLAIM: numeric zero diverged from PostgreSQL 18 (a timeout: it never finished): {bad:?}");
    }
}
