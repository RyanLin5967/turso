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
    /// The column, for a direct column reference; None for a derived value
    /// (`derived_operand_info`). Only mutant `operand_keeps_column_typmod` reads it.
    column: Option<Column>,
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
            column: Some(col.clone()),
            type_def: Arc::clone(type_def),
        });
    }
    None
}

/// The custom type of a comparison operand that is not a column of one: a value whose type
/// `order_by::expr_custom_type` knows (an aggregate over such a column, `sum(b)` of numeric, which
/// is numeric text; a derived relation's typed column; an operator result the type defines).
/// Engine review 2bdcd32c6 HIGH 5: `sum(b) = 13` over numeric(8, 2) compared the text '13.00'
/// with the number 13 by SQLite's type order (false), and `HAVING sum(b) > 5` kept every group,
/// where PostgreSQL compares numerics. Used only when neither operand is a column of a custom
/// type, so a column's own rules (and an index seek's, `key_takes_type_operator`) are unchanged.
/// Mutant `derived_value_compares_plainly` (test builds only): None, the plain comparison, as
/// before.
fn derived_operand_info(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Option<ExprCustomTypeInfo> {
    if crate::branch::store::fe_mutant("derived_value_compares_plainly") {
        return None;
    }
    // By the values it yields (`expr_value_type`): a CAST that yields its type's ENCODE (numeric's
    // blob) is not an operand of the type's operators, which refuse a blob (review of this lane's
    // fixes, item 1). Mutant `unapplied_cast_typed` (test builds only): `expr_custom_type`, as
    // before.
    let typed = if crate::branch::store::fe_mutant("unapplied_cast_typed") {
        crate::translate::order_by::expr_custom_type
    } else {
        crate::translate::order_by::expr_value_type
    };
    let (type_def, _) = typed(expr, referenced_tables?, resolver.schema())?;
    Some(ExprCustomTypeInfo {
        type_name: type_def.name.to_lowercase(),
        column: None,
        type_def,
    })
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
    column: Option<&Column>,
    type_def: &Arc<TypeDef>,
    which: EncodeArg,
) -> Option<OperatorEncodeInfo> {
    column.filter(|_| cfg!(test)).map(|column| OperatorEncodeInfo {
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
    value_type: Option<&Arc<TypeDef>>,
) -> Option<ResolvedOperator> {
    let op_str = operator_to_str(op)?;
    // `value` in a domain CHECK translated outside any table (CAST to the domain) is a value of
    // that domain (`ProgramBuilder::id_value_type`).
    let value_info = |e: &ast::Expr| {
        let td = value_type?;
        matches!(e, ast::Expr::Id(name) if name.as_str().eq_ignore_ascii_case("value")).then(|| ExprCustomTypeInfo {
            type_name: td.name.to_lowercase(),
            column: None,
            type_def: td.clone(),
        })
    };
    // A value of a domain takes its base type's operators (`Schema::operator_type_of`).
    let with_operators = |info: ExprCustomTypeInfo| {
        let type_def = resolver.schema().operator_type_of(&info.type_def);
        ExprCustomTypeInfo {
            type_name: type_def.name.to_lowercase(),
            column: info.column,
            type_def,
        }
    };
    let lhs_info = expr_custom_type_info(e1, referenced_tables, resolver)
        .or_else(|| value_info(e1))
        .map(with_operators);
    let rhs_info = expr_custom_type_info(e2, referenced_tables, resolver)
        .or_else(|| value_info(e2))
        .map(with_operators);

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

    // An arithmetic operator a type defines (numeric's '+', date's '-') is that type's
    // PostgreSQL operator with every operand it can take: its function receives the other
    // operand as given, whatever it is (a constant, another type's column, a plain column, an
    // expression), and refuses what PostgreSQL has no operator for (fastest-wire 68f301ef0: date had
    // none, so `d + 1` added its text as a number; bigint * numeric took the plain '*' and lost the
    // scale). When only one side's type defines it, that side's; when both do, the left one's. A comparison keeps the rules below
    // (its operand must be compatible), since an index seek encodes that operand. Mutant
    // `arithmetic_operand_must_be_compatible` (test builds only): as before.
    let arithmetic = matches!(
        op,
        ast::Operator::Add | ast::Operator::Subtract | ast::Operator::Multiply | ast::Operator::Divide
    ) && !crate::branch::store::fe_mutant("arithmetic_operand_must_be_compatible");
    if arithmetic {
        let defines = |info: &Option<ExprCustomTypeInfo>| {
            info.as_ref().and_then(|i| {
                i.type_def
                    .operators()
                    .iter()
                    .find(|o| o.op == op_str)
                    .and_then(|o| o.func_name.clone())
            })
        };
        let resolved = |func_name: String| ResolvedOperator {
            func_name,
            swap_args: false,
            negate: false,
            encode_info: None,
        };
        match (defines(&lhs_info), defines(&rhs_info)) {
            // Both sides' types define it (two numerics; or a date and a numeric, which
            // PostgreSQL has no operator for): the left one's, which refuses an operand it
            // cannot take rather than answering the plain arithmetic of two texts.
            (Some(f), _) | (None, Some(f)) => return Some(resolved(f)),
            (None, None) => {}
        }
        // Neither operand a column of such a type: a value of one (`derived_operand_info`:
        // sum(b), max(b), a derived relation's typed column, b + 1) computes by its type's
        // operator too, the same rule (engine review 2bdcd32c6 HIGH 5, arithmetic half:
        // `sum(b) + 1` over numeric(8, 2) was SQLite's 14.0 on the numeric text, where PostgreSQL
        // answers 14.00). Mutant `derived_value_computes_plainly` (test builds only): SQLite's
        // arithmetic, as before.
        if lhs_info.is_none() && rhs_info.is_none() && !crate::branch::store::fe_mutant("derived_value_computes_plainly") {
            let lhs_derived = derived_operand_info(e1, referenced_tables, resolver).map(with_operators);
            let rhs_derived = derived_operand_info(e2, referenced_tables, resolver).map(with_operators);
            if let Some(f) = defines(&lhs_derived).or_else(|| defines(&rhs_derived)) {
                return Some(resolved(f));
            }
        }
    }

    // A comparison between operands neither of which is a column of a custom type compares
    // through the type of the values they yield, when one is known (`derived_operand_info`:
    // `sum(b) = 13` over numeric), by the cases below.
    let comparison = matches!(
        op,
        ast::Operator::Equals
            | ast::Operator::NotEquals
            | ast::Operator::Less
            | ast::Operator::LessEquals
            | ast::Operator::Greater
            | ast::Operator::GreaterEquals
    );
    let (lhs_info, rhs_info) = if comparison && lhs_info.is_none() && rhs_info.is_none() {
        (
            derived_operand_info(e1, referenced_tables, resolver).map(with_operators),
            derived_operand_info(e2, referenced_tables, resolver).map(with_operators),
        )
    } else {
        (lhs_info, rhs_info)
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

    // A comparison operand that is a value of the other side's own type (b + 10, b * 1.0, a
    // derived relation's typed column, against a column b of numeric) compares through the type's
    // operator, as two columns of it do (second review of the fe-rev2bd-types fixes, item 2:
    // `b < b + 10` compared texts). Mutant `column_against_value_plain` (test builds only): only a
    // compatible constant, as before.
    let same_type_value = |e: &ast::Expr, info: &ExprCustomTypeInfo| {
        comparison
            && !crate::branch::store::fe_mutant("column_against_value_plain")
            && derived_operand_info(e, referenced_tables, resolver)
                .map(with_operators)
                .is_some_and(|d| d.type_name == info.type_name)
    };

    // Case 2: LHS is custom type, RHS is a constant operand.
    if let Some(ref lhs) = lhs_info {
        if operand_compatible(e2, lhs.type_def.value_input_type(), resolver) || same_type_value(e2, lhs) {
            if let Some((func_name, swap_args, negate)) = find_in_type_def(&lhs.type_def) {
                return Some(ResolvedOperator {
                    func_name,
                    swap_args,
                    negate,
                    encode_info: encode_info(lhs.column.as_ref(), &lhs.type_def, EncodeArg::Second),
                });
            }
        }
    }

    // Case 3: RHS is custom type, LHS is a constant operand (reversed).
    if let Some(ref rhs) = rhs_info {
        if operand_compatible(e1, rhs.type_def.value_input_type(), resolver) || same_type_value(e1, rhs) {
            if let Some((func_name, swap_args, negate)) = find_in_type_def(&rhs.type_def) {
                return Some(ResolvedOperator {
                    func_name,
                    swap_args,
                    negate,
                    encode_info: encode_info(rhs.column.as_ref(), &rhs.type_def, EncodeArg::First),
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
        // A domain's key is its base type's (`seek_type_of`), as `find_custom_type_operator`
        // compares it.
        Some(info) => {
            info.type_name.eq_ignore_ascii_case(type_name)
                || seek_type_of(resolver.schema(), &info.type_def).name.eq_ignore_ascii_case(&type_def.name)
        }
        // A value of the column's own type (`find_custom_type_operator`'s `same_type_value`).
        None => {
            operand_compatible(key, type_def.value_input_type(), resolver)
                || (!crate::branch::store::fe_mutant("column_against_value_plain")
                    && derived_operand_info(key, referenced_tables, resolver)
                        .is_some_and(|d| seek_type_of(resolver.schema(), &d.type_def).name.eq_ignore_ascii_case(&type_def.name)))
        }
    }
}

/// The type an index on a column of `type_def` is sought and checked by: a domain's base type
/// (`Schema::operator_type_of`), whose ENCODE made the index's keys and whose '=' a scan compares
/// with (review of this lane's fixes, item 3: a domain over numeric sought its raw key among
/// numeric's encoded blobs, so `a = 10` answered by the index unlike the scan). Mutant
/// `domain_seek_by_its_own_typedef` (test builds only): `type_def`, as before.
pub(crate) fn seek_type_of(schema: &crate::schema::Schema, type_def: &Arc<TypeDef>) -> Arc<TypeDef> {
    if crate::branch::store::fe_mutant("domain_seek_by_its_own_typedef") {
        return type_def.clone();
    }
    schema.operator_type_of(type_def)
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

    // A domain's CHECK reads the domain's value, as PostgreSQL's does: `reg` holds the CAST's
    // ENCODE (its stored form), so `value` is bound to it DECODEd through the chain (engine review
    // 2bdcd32c6 HIGH 8: over numeric(8, 2) the CHECK compared the encoded blob; numeric's operators
    // refuse a blob, as its INSERT does). A chain with no DECODE (a domain over integer) reads `reg`
    // as it is. Mutant `domain_takes_no_base_operator` (test builds only): `reg`, as before.
    let value_reg = if chain.iter().any(|td| td.decode().is_some())
        && !crate::branch::store::fe_mutant("domain_takes_no_base_operator")
    {
        let decoded = program.alloc_register();
        program.emit_insn(Insn::Copy {
            src_reg: reg,
            dst_reg: decoded,
            extra_amount: 0,
        });
        let cast_col = Column::new(
            None,
            chain.first().map(|td| td.name.clone()).unwrap_or_default(),
            None,
            None,
            Type::Null,
            None,
            ColDef::default(),
        );
        let skip_label = program.allocate_label();
        program.emit_insn(Insn::IsNull {
            reg: decoded,
            target_pc: skip_label,
        });
        for td in chain.iter().rev() {
            if let Some(decode_expr) = td.decode() {
                emit_type_expr(program, decode_expr, decoded, decoded, &cast_col, td, resolver)?;
            }
        }
        program.preassign_label_to_next_insn(skip_label);
        decoded
    } else {
        reg
    };

    for td in chain {
        for (dc, constraint_name) in td.domain_checks.iter().zip(crate::schema::domain_check_names(td)) {

            // Bind `value` → its register, translate check expr, verify truthy. `value` is a value
            // of this domain, so a comparison of it takes the domain's base type's operators
            // (engine review 2bdcd32c6 HIGH 8: CAST(-1 AS dpos) passed CHECK (VALUE > 0) over
            // numeric).
            program.id_register_overrides.insert("value", value_reg);
            let outer_value_type = program.id_value_type.replace(td.clone());

            let expr_result_reg = program.alloc_register();
            let translated = translate_expr(program, None, &dc.check, expr_result_reg, resolver);

            program.id_value_type = outer_value_type;
            program.id_register_overrides.remove("value");
            translated?;

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

/// The parameters the domain chain from `type_name` gives `type_def`: the base parameters of the
/// domain whose base is `type_def` (`TypeDef::domain_base_params`), or none.
/// Mutant `domain_params_ignored` (test builds only): none, as before.
fn domain_base_params(resolver: &Resolver, type_name: &str, type_def: &TypeDef) -> Vec<Box<ast::Expr>> {
    if crate::branch::store::fe_mutant("domain_params_ignored") {
        return Vec::new();
    }
    let schema = resolver.schema();
    let target = crate::schema::type_key(&type_def.name);
    let mut current = schema.get_type_def_unchecked(type_name);
    // A chain is acyclic (CREATE DOMAIN refuses a cycle); the bound only guards a corrupt registry.
    for _ in 0..64 {
        let Some(td) = current else { break };
        if !td.is_domain {
            break;
        }
        if crate::schema::type_key(td.base()) == target {
            return td.domain_base_params.clone();
        }
        current = schema.get_type_def_unchecked(td.base());
    }
    Vec::new()
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
    program.id_register_overrides.insert("value", value_reg);

    // Set up type parameter overrides. Capture the result so we can
    // clean up overrides even if param translation fails.
    let param_result: Result<()> = (|| {
        // Skip `value` param (already handled above); match remaining params
        // against the user-provided ty_params by position.
        let user_params: Vec<_> = type_def.user_params().collect();
        // A column naming a domain with no parameters of its own takes the ones its domain gave
        // this type (`CREATE DOMAIN dn AS numeric(8, 2)`; fastest-wire 73ca2b720).
        let domain_params;
        let params: &[Box<ast::Expr>] = if column.ty_params.is_empty() {
            domain_params = domain_base_params(resolver, &column.ty_str, type_def);
            &domain_params
        } else {
            &column.ty_params
        };
        for (i, param) in user_params.iter().enumerate() {
            if let Some(param_expr) = params.get(i) {
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

    /// fastest-wire ef7738ee2 (wirefuzz against PostgreSQL 18.6, wire-fuzz3-1): sum over a
    /// numeric(6, 2) column holding 0.00 and NULL is numeric 0.00, keeping its operands' scale (here
    /// '0', SQLite's sum of the decoded text), and is typed numeric; and an UPDATE assigning 10000 to
    /// it is refused even when no row matches ("numeric field overflow", PostgreSQL's 22003 at plan
    /// time). Red by the sum claim at the base.
    #[test]
    fn a_numeric_sum_keeps_its_scale_and_an_overflowing_constant_is_refused() {
        let conn = open();
        conn.execute("CREATE TABLE t0(id INTEGER PRIMARY KEY, c0 numeric(6, 2)) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t0 VALUES (1, 0.00), (2, NULL)").unwrap();
        let mut wrong = Vec::new();
        let stmt = conn.prepare("SELECT sum(c0) FROM t0");
        let ty = stmt
            .as_ref()
            .ok()
            .and_then(|s| s.get_column_type_info(0).ok().flatten())
            .map(|i| i.declared_name.to_ascii_lowercase());
        let got = stmt.and_then(|mut s| s.run_collect_rows());
        if !matches!(&got, Ok(rows) if rows.len() == 1 && rows[0][0].to_text() == Some("0.00")) || ty.as_deref() != Some("numeric") {
            wrong.push(format!("sum(c0) = {got:?} typed {ty:?}, PostgreSQL 0.00 numeric"));
        }
        match conn.execute("UPDATE t0 SET c0 = 10000 WHERE id = 3") {
            Err(e) if e.to_string().contains("numeric field overflow") => {}
            other => wrong.push(format!("UPDATE c0 = 10000 matching no row: {other:?}, PostgreSQL 22003")),
        }
        assert!(wrong.is_empty(), "CLAIM: a numeric sum and constant as PostgreSQL: {wrong:#?}");
    }

    /// Lead ruling 2026-10-19 (fastest-wire 68f301ef0, PostgreSQL 18.6 recorded: timestamp -
    /// timestamp and timestamptz - timestamptz are the interval '00:00:00'): with no interval type,
    /// arithmetic over timestamp, timestamptz and time is REFUSED as not supported (0A000 at the
    /// wire), never answered. At the base each is the plain arithmetic of two texts, silently: ts -
    /// ts answered 0 and ts + 1 2027. Each refusal is `LimboError::FeatureNotSupported`, "interval
    /// arithmetic is not supported", over a row of NULLs too.
    /// date - date (a day count) is the control.
    #[test]
    fn timestamp_and_time_arithmetic_is_refused_until_an_interval_type_exists() {
        let conn = open();
        conn.execute("CREATE TABLE s(id INTEGER PRIMARY KEY, a timestamp, b timestamp, z timestamptz, y timestamptz, t time, u time, d date, e date) STRICT").unwrap();
        conn.execute(
            "INSERT INTO s VALUES (1, '2026-10-19 10:00:00', '2026-10-19 09:00:00', '2026-10-19 10:00:00+00:00', '2026-10-19 10:00:00+00:00', '10:00:00', '09:00:00', '2026-10-19', '2026-10-18')",
        )
        .unwrap();
        conn.execute("INSERT INTO s(id) VALUES (2)").unwrap();
        let mut wrong = Vec::new();
        for expr in ["a - b", "z - y", "t - u", "a + 1", "1 + a", "a - 1", "z + 1", "t + 1"] {
            for id in [1, 2] {
                match conn.prepare(&format!("SELECT {expr} FROM s WHERE id = {id}")).and_then(|mut s| s.run_collect_rows()) {
                    Err(e) if format!("{e:?}").starts_with("FeatureNotSupported(") && e.to_string() == "interval arithmetic is not supported" => {}
                    other => wrong.push(format!("{expr} (row {id}): {other:?}")),
                }
            }
        }
        match conn.prepare("SELECT d - e FROM s WHERE id = 1").and_then(|mut s| s.run_collect_rows()) {
            Ok(rows) if rows[0][0].to_string() == "1" => {}
            other => wrong.push(format!("control d - e: {other:?}")),
        }
        assert!(wrong.is_empty(), "CLAIM: refused as not supported, never answered: {wrong:#?}");
    }

    /// fastest-wire 68f301ef0 (PostgreSQL 18.6 recorded on iccad2): a timestamptz literal whose
    /// offset has no minutes ('+00', '-03') or no colon ('+0530') is a valid PostgreSQL input, stored
    /// as the instant it names; here the ENCODE's datetime(value) took only '+HH:MM' and refused it
    /// 'invalid timestamp value'. Stored as UTC text, as the '+00:00' control is. Recorded: '+00';
    /// '-03', '+0530' and '-0330' by PostgreSQL's ISO 8601 offset forms, not recorded here.
    #[test]
    fn a_timestamptz_offset_without_minutes_or_colon_is_accepted() {
        let conn = open();
        conn.execute("CREATE TABLE z(id INTEGER PRIMARY KEY, t timestamptz) STRICT").unwrap();
        let mut wrong = Vec::new();
        for (id, lit, want) in [
            (1, "2026-10-19 10:00:00+00:00", "2026-10-19 10:00:00"),
            (2, "2026-10-19 10:00:00+00", "2026-10-19 10:00:00"),
            (3, "2026-10-19 10:00:00-03", "2026-10-19 13:00:00"),
            (4, "2026-10-19 10:00:00+0530", "2026-10-19 04:30:00"),
            (5, "2026-10-19 10:00:00.5-0330", "2026-10-19 13:30:00.5"),
        ] {
            if let Err(e) = conn.execute(&format!("INSERT INTO z VALUES ({id}, '{lit}')")) {
                wrong.push(format!("{lit}: refused: {e}"));
                continue;
            }
            let got = conn
                .prepare(&format!("SELECT t FROM z WHERE id = {id}"))
                .and_then(|mut s| s.run_collect_rows())
                .map(|r| r[0][0].to_string());
            if got.as_ref().ok().map(String::as_str) != Some(want) {
                wrong.push(format!("{lit}: stored {got:?}, PostgreSQL's instant {want}"));
            }
        }
        assert!(wrong.is_empty(), "CLAIM: PostgreSQL's timestamptz offsets: {wrong:#?}");
    }

    /// fastest-wire 68f301ef0 (PostgreSQL 18.6 recorded on iccad2,
    /// pg18-wirefuzz-arithmetic_over_rows_answers_as_postgresql_does.out): arithmetic over a date
    /// and over a numeric meeting another type answers as PostgreSQL's operators do. SILENT at the
    /// base: date has no '+' or '-', so `1 + d`, `d + 1` and `d - 1` added the date's text as a
    /// number (2027, 2027, 2025 for 2026-10-19). Recorded: those three are 2026-10-20, 2026-10-20
    /// and 2026-10-18, typed date, and `b * n` (bigint 2 times numeric(8, 2) 10.50) is 21.00,
    /// numeric. By PostgreSQL's operator table (date + int4, int4 + date, date - int4 are date;
    /// date - date is int4; numeric with an integer operand is numeric), not recorded here: `d + i`
    /// over an INTEGER column, `d - e` = 1 and not typed date, `i * n` = 31.50, `n + i` = 13.50.
    /// Refused as PostgreSQL refuses them (no such operator): `d + e`, `1 - d`, `d + 1.5`.
    #[test]
    fn date_and_numeric_arithmetic_across_types_answers_as_postgresql() {
        let conn = open();
        conn.execute(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, d date, e date, n numeric(8, 2), b bigint, i INTEGER) STRICT",
        )
        .unwrap();
        conn.execute("INSERT INTO t VALUES (1, '2026-10-19', '2026-10-18', 10.50, 2, 3)").unwrap();
        let mut wrong = Vec::new();
        for (expr, want, ty) in [
            ("1 + d", "2026-10-20", Some("date")),
            ("d + 1", "2026-10-20", Some("date")),
            ("d - 1", "2026-10-18", Some("date")),
            ("d + i", "2026-10-22", Some("date")),
            ("d - e", "1", None),
            ("b * n", "21.00", Some("numeric")),
            ("i * n", "31.50", Some("numeric")),
            ("n + i", "13.50", Some("numeric")),
        ] {
            let sql = format!("SELECT {expr} FROM t");
            let stmt = conn.prepare(&sql);
            let got_ty = stmt
                .as_ref()
                .ok()
                .and_then(|s| s.get_column_type_info(0).ok().flatten())
                .map(|i| i.declared_name.to_ascii_lowercase());
            let got = stmt.and_then(|mut s| s.run_collect_rows()).map(|rows| rows[0][0].to_string());
            let ty_ok = match ty {
                Some(t) => got_ty.as_deref() == Some(t),
                None => got_ty.as_deref() != Some("date"),
            };
            if got.as_ref().ok().map(String::as_str) != Some(want) || !ty_ok {
                wrong.push(format!("{expr}: {got:?} typed {got_ty:?}, PostgreSQL {want} typed {ty:?}"));
            }
        }
        for expr in ["d + e", "1 - d", "d + 1.5"] {
            let got = conn.prepare(&format!("SELECT {expr} FROM t")).and_then(|mut s| s.run_collect_rows());
            if got.is_ok() {
                wrong.push(format!("{expr}: {got:?}, PostgreSQL refuses it (no such operator)"));
            }
        }
        assert!(wrong.is_empty(), "CLAIM: date and numeric arithmetic as PostgreSQL: {wrong:#?}");
    }

    /// fastest-wire 76b5a2550 (wire-rev-uw3; PostgreSQL 18.6 recorded on iccad2,
    /// pg18-numeric-derived.out): over a numeric(8, 2) column holding 2.50, 123456.78 and 10.00, a
    /// direct `ORDER BY b` orders as numbers, but `b + 1 ... ORDER BY 1` and `b` read through a view,
    /// a derived table and a CREATE TABLE AS copy ordered as text (10.00, 123456.78, 2.50): a silent
    /// wrong row order. Each result column is also typed numeric, as PostgreSQL types it, a scalar
    /// subquery's max(b) included (`get_column_type_info`'s declared name). The direct column is
    /// the control. Red by its claims at the base.
    #[test]
    fn a_numeric_column_orders_and_types_as_numeric_through_every_relation() {
        let conn = open();
        for sql in [
            "CREATE TABLE n(id INTEGER PRIMARY KEY, b numeric(8, 2)) STRICT",
            "INSERT INTO n VALUES (1, 2.50), (2, 123456.78), (3, 10.00)",
            "CREATE VIEW v AS SELECT b FROM n",
            "CREATE TABLE c AS SELECT b FROM n",
        ] {
            conn.execute(sql).unwrap();
        }
        let mut wrong = Vec::new();
        for (sql, want) in [
            ("SELECT b FROM n ORDER BY b", &["2.50", "10.00", "123456.78"][..]),
            ("SELECT b + 1 AS x FROM n ORDER BY 1", &["3.50", "11.00", "123457.78"][..]),
            ("SELECT b FROM v ORDER BY b", &["2.50", "10.00", "123456.78"][..]),
            ("SELECT b FROM (SELECT b FROM n) d ORDER BY b", &["2.50", "10.00", "123456.78"][..]),
            ("SELECT b FROM c ORDER BY b", &["2.50", "10.00", "123456.78"][..]),
            ("SELECT (SELECT max(b) FROM n) AS m", &["123456.78"][..]),
        ] {
            let stmt = conn.prepare(sql);
            let ty = stmt
                .as_ref()
                .ok()
                .and_then(|s| s.get_column_type_info(0).ok().flatten())
                .map(|i| i.declared_name.to_ascii_lowercase());
            let got = stmt.and_then(|mut s| s.run_collect_rows()).map(|rows| {
                rows.iter().map(|r| r[0].to_text().unwrap_or("NULL").to_string()).collect::<Vec<_>>()
            });
            let want: Vec<String> = want.iter().map(|w| w.to_string()).collect();
            if got.as_ref().ok() != Some(&want) || ty.as_deref() != Some("numeric") {
                wrong.push(format!("{sql}: rows {got:?} type {ty:?}, PostgreSQL {want:?} numeric"));
            }
        }
        assert!(wrong.is_empty(), "CLAIM: numeric through every relation, as PostgreSQL: {wrong:#?}");
    }

    /// fastest-wire c15466936 (wirefuzz against PostgreSQL 18.6): a value too long for varchar(n) is
    /// refused as a classified string-data-right-truncation error in PostgreSQL's words ("value too
    /// long for type character varying(5)"; 22001 on the wire), not as a RAISE whose message the wire
    /// would have to read; and an UPDATE assigning such a constant is refused even when no row matches,
    /// since PostgreSQL coerces the constant to the column's type when it plans the statement. A value
    /// that fits is the control. At the base the INSERT raises 'value too long for varchar' and the
    /// UPDATE answers 0 rows: red by its claim.
    #[test]
    fn a_value_too_long_for_varchar_is_a_truncation_error_even_when_no_row_matches() {
        let conn = open();
        conn.execute("CREATE TABLE vc(id INTEGER PRIMARY KEY, v varchar(5)) STRICT")
            .unwrap();
        conn.execute("INSERT INTO vc VALUES (2, 'abcde')").unwrap();
        let mut wrong = Vec::new();
        for sql in ["INSERT INTO vc VALUES (1, 'abcdef')", "UPDATE vc SET v = 'abcdef' WHERE id = 1"] {
            match conn.execute(sql) {
                Err(e)
                    if format!("{e:?}").starts_with("StringDataRightTruncation(")
                        && e.to_string() == "value too long for type character varying(5)" => {}
                other => wrong.push(format!("{sql}: {other:?}")),
            }
        }
        assert!(
            wrong.is_empty(),
            "CLAIM: each is refused as StringDataRightTruncation, in PostgreSQL's words: {wrong:#?}"
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

    /// fastest-wire 0b092d531 (PGDIV, recorded against PostgreSQL 18.6): varchar(n)'s length coercion
    /// cuts a value whose characters past n are all spaces to n characters and stores it; only a
    /// non-space past n is refused (22001). Per column, at the store: a bound 'abc   ' stores 'abc' in
    /// varchar(3), a literal 'ab      ' stores 'ab   ' in varchar(5), an UPDATE assigns the cut value;
    /// a tab past n is not a space and is refused, and 'abcdef' still is. A comparison is not an
    /// assignment: `v = 'abc   '` finds no 'abc' (PostgreSQL compares the texts), by the seek of an
    /// index on v and by a scan alike. At the base each store raises 'value too long'.
    #[test]
    fn a_varchar_value_whose_excess_is_spaces_is_cut_to_its_length() {
        let conn = open();
        conn.execute("CREATE TABLE vc(id INTEGER PRIMARY KEY, v varchar(5), w varchar(3)) STRICT")
            .unwrap();
        conn.execute("CREATE INDEX vcw ON vc(w)").unwrap();
        let mut wrong = Vec::new();
        let stored = |conn: &Arc<crate::Connection>, id: i64| -> Option<(String, String)> {
            let mut stmt = conn.prepare(&format!("SELECT v, w FROM vc WHERE id = {id}")).ok()?;
            let rows = stmt.run_collect_rows().ok()?;
            let row = rows.first()?;
            Some((row[0].to_string(), row[1].to_string()))
        };
        for (sql, id, want) in [
            ("INSERT INTO vc VALUES (20, 'ab      ', 'x')", 20, ("ab   ", "x")),
            ("INSERT INTO vc VALUES (21, 'abcde ', 'abc   ')", 21, ("abcde", "abc")),
        ] {
            if let Err(e) = conn.execute(sql) {
                wrong.push(format!("{sql}: {e}"));
            } else if stored(&conn, id) != Some((want.0.to_string(), want.1.to_string())) {
                wrong.push(format!("{sql}: stored {:?}, PostgreSQL {want:?}", stored(&conn, id)));
            }
        }
        match conn.prepare("INSERT INTO vc VALUES (22, 'p', ?1)").and_then(|mut s| {
            s.bind_at(1.try_into().unwrap(), Value::build_text("abc   "))?;
            s.run_collect_rows()
        }) {
            Ok(_) if stored(&conn, 22) == Some(("p".to_string(), "abc".to_string())) => {}
            other => wrong.push(format!("bound 'abc   ' into varchar(3): {other:?}, stored {:?}", stored(&conn, 22))),
        }
        match conn.execute("UPDATE vc SET v = 'xy        ' WHERE id = 20") {
            Ok(_) if stored(&conn, 20).map(|r| r.0) == Some("xy   ".to_string()) => {}
            other => wrong.push(format!("UPDATE to 'xy        ': {other:?}, stored {:?}", stored(&conn, 20))),
        }
        for sql in ["INSERT INTO vc VALUES (23, 'abcde\t', 'x')", "INSERT INTO vc VALUES (24, 'abcdef', 'x')"] {
            match conn.execute(sql) {
                Err(e) if e.to_string() == "value too long for type character varying(5)" => {}
                other => wrong.push(format!("{sql}: {other:?}, PostgreSQL refuses it (22001)")),
            }
        }
        assert!(wrong.is_empty(), "CLAIM: PostgreSQL's excess-space rule, per column: {wrong:#?}");
        answers_by_both_plans(
            &conn,
            "vc",
            "vcw",
            &[("w = 'abc'", None, 2), ("w = 'abc   '", None, 0), ("w = ?1", Some(Value::build_text("abc   ")), 0)],
        );
    }

    /// A range over an indexed varchar(n) or smallint column with a bound its ENCODE refuses ('value
    /// too long', 'out of range') answers what a scan answers: the bound compares as itself. Engine
    /// review 20 item 9 made a refused key's seek EMPTY, which is right for '=' and wrong for a
    /// range: `v < 'abcdef'` over varchar(3) holds 'abc', and `s < 40000` over smallint holds 7.
    #[test]
    fn a_range_with_a_bound_the_type_refuses_answers_as_a_scan_does() {
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
            &[("v < 'abcdef'", None, 1), ("v > 'abcdef'", None, 0), ("v <= 'abd   '", None, 1)],
        );
        answers_by_both_plans(&conn, "t", "ts", &[("s < 40000", None, 1), ("s > -40000", None, 1), ("s > 40000", None, 0)]);
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

    /// Every row of `sql` on `conn`, each value as its display text (NULL as "NULL"), or the error.
    fn table_of(conn: &Arc<crate::Connection>, sql: &str) -> Result<Vec<Vec<String>>, String> {
        let rows = conn.prepare(sql).and_then(|mut s| s.run_collect_rows()).map_err(|e| e.to_string())?;
        Ok(rows
            .iter()
            .map(|r| r.iter().map(|v| if matches!(v, Value::Null) { "NULL".to_string() } else { v.to_string() }).collect())
            .collect())
    }

    /// Runs each `(sql, want)` on `conn` and lists every one whose rows are not `want` (rows joined
    /// by ';', values by ','), naming `source` as the oracle.
    fn diverging(conn: &Arc<crate::Connection>, cases: &[(&str, &str)], source: &str) -> Vec<String> {
        let mut bad = Vec::new();
        for (sql, want) in cases {
            let got = table_of(conn, sql).map(|rows| rows.iter().map(|r| r.join(",")).collect::<Vec<_>>().join(";"));
            if got.as_deref() != Ok(*want) {
                bad.push(format!("{sql} gave {got:?}, not {source}'s {want:?}"));
            }
        }
        bad
    }

    /// Engine review 2bdcd32c6 HIGH 5 (artie frontier/fastest/reviews/REVIEW-engine-2bdcd32c6.md, raw
    /// types/probe2.out.txt): sum over numeric(8, 2) is the numeric text '13.00' (97e6bf4cb), but a
    /// comparison of it was not dispatched through numeric's operators, so it compared as text
    /// against a number: `sum(b) = 13` and `sum(b) < 100` were false, and `HAVING sum(b) > 5` kept
    /// every group. A value of a custom type that is not a column (an aggregate of one, a derived
    /// relation's typed column) compares through the type's operator, as PostgreSQL compares a
    /// numeric. Expected values are PostgreSQL 18's semantics for numeric comparison (numeric =,
    /// <, > against an integer constant), not run here; booleans as SQLite's 1 and 0. The direct
    /// column comparison is the control.
    #[test]
    fn a_numeric_aggregate_or_derived_value_compares_through_numerics_operators() {
        let conn = open();
        conn.execute("CREATE TABLE n(id INTEGER PRIMARY KEY, b numeric(8, 2)) STRICT").unwrap();
        conn.execute("INSERT INTO n VALUES (1, 1.00), (2, 2.00), (3, NULL), (4, 10.00)").unwrap();
        let bad = diverging(
            &conn,
            &[
                ("SELECT b = 10 FROM n WHERE id = 4", "1"),
                ("SELECT sum(b) = 13, sum(b) < 100, sum(b) > 5, sum(b) = 13.00, sum(b) <> 13 FROM n", "1,1,1,1,0"),
                ("SELECT id FROM n GROUP BY id HAVING sum(b) > 5 ORDER BY id", "4"),
                ("SELECT id FROM n GROUP BY id HAVING sum(b) < 1.5 ORDER BY id", "1"),
                ("SELECT s = 13, s < 100 FROM (SELECT sum(b) AS s FROM n)", "1,1"),
                ("SELECT max(b) = 10, min(b) < 2 FROM n", "1,1"),
            ],
            "PostgreSQL 18",
        );
        assert!(bad.is_empty(), "CLAIM: a numeric aggregate or derived value compared as text: {bad:#?}");
    }

    /// Engine review 2bdcd32c6 HIGH 8 (raw types/probe3.out.txt): a domain over numeric(8, 2) took
    /// none of numeric's operators (a domain's TypeDef has none of its own), so its CHECK compared
    /// the stored encoding, a blob, with the number: `dpos CHECK (VALUE > 0)` accepted -1 and 0.001
    /// (stored 0.00), `dneg CHECK (VALUE < 0)` refused -1 and 1, and CAST(-1 AS dpos) passed; a
    /// WHERE comparison and an ORDER BY of such a column were not numeric's either. A value of a
    /// domain takes its base type's operators, as PostgreSQL's domains do. Expected values are
    /// PostgreSQL 18.6's for this exact SQL (UW run typesrev-oracle2, zonky embedded-postgres
    /// 18.6.0; artie raw frontier/fastest/lanes/engine/raw/pg18-types-rev2bd.txt 42503e4e38), never
    /// ours; the refusal message is PG's own, here without its SQLSTATE (23514).
    #[test]
    fn a_domain_over_numeric_checks_compares_and_orders_as_numeric() {
        let conn = open();
        for sql in [
            "CREATE DOMAIN dpos AS numeric(8, 2) CHECK (VALUE > 0)",
            "CREATE DOMAIN dneg AS numeric(8, 2) CHECK (VALUE < 0)",
            "CREATE TABLE d(id INTEGER PRIMARY KEY, a dpos, b dneg) STRICT",
        ] {
            conn.execute(sql).unwrap();
        }
        let pos = r#"value for domain dpos violates check constraint "dpos_check""#;
        let neg = r#"value for domain dneg violates check constraint "dneg_check""#;
        let mut bad = Vec::new();
        for (sql, want) in [
            ("INSERT INTO d(id, a) VALUES (1, -1)", Err(pos)),
            ("INSERT INTO d(id, a) VALUES (2, 0.001)", Err(pos)),
            ("INSERT INTO d(id, a) VALUES (3, 1.5)", Ok(())),
            ("INSERT INTO d(id, b) VALUES (4, -1)", Ok(())),
            ("INSERT INTO d(id, b) VALUES (5, 1)", Err(neg)),
            ("INSERT INTO d(id, a) VALUES (6, 10), (7, 9)", Ok(())),
            ("UPDATE d SET a = -2 WHERE id = 3", Err(pos)),
            ("SELECT CAST(-1 AS dpos)", Err(pos)),
            ("SELECT CAST(-3 AS dneg)", Ok(())),
        ] {
            let got = conn.prepare(sql).and_then(|mut s| s.run_collect_rows()).map(|_| ()).map_err(|e| e.to_string());
            let ok = match (&got, want) {
                (Ok(()), Ok(())) => true,
                (Err(m), Err(w)) => m == w,
                _ => false,
            };
            if !ok {
                bad.push(format!("{sql} gave {got:?}, not PostgreSQL 18's {want:?}"));
            }
        }
        bad.extend(diverging(
            &conn,
            &[
                ("SELECT id FROM d WHERE a > 5 ORDER BY id", "6;7"),
                ("SELECT a FROM d WHERE a IS NOT NULL ORDER BY a", "1.50;9.00;10.00"),
                ("SELECT id, a, b FROM d ORDER BY id", "3,1.50,NULL;4,NULL,-1.00;6,10.00,NULL;7,9.00,NULL"),
            ],
            "PostgreSQL 18",
        ));
        assert!(bad.is_empty(), "CLAIM: a domain over numeric did not check, compare or order as numeric: {bad:#?}");
    }

    /// Engine review 2bdcd32c6 HIGH 4 (raws types/probe1-3): with custom types on, a plain NUMERIC
    /// or DECIMAL column of a non-STRICT table read through a subquery, a view or a CTE was taken for
    /// the custom numeric type: the derived column's type name is its affinity's, "NUMERIC", and
    /// `order_by::expr_custom_type` looked it up ignoring STRICT. So sum(d) was numeric text, a text
    /// row errored "invalid numeric value", INT64_MAX + INT64_MAX summed past the integer range, and
    /// CREATE TABLE AS from the view failed "numeric requires 2 parameter(s)". Such a column is
    /// SQLite's, typed only by a custom type its source carries. Expected values are python sqlite3
    /// 3.46.1's for this exact SQL (UW run typesrev-oracle2; artie raw
    /// frontier/fastest/lanes/engine/raw/sqlite-types-rev2bd.txt 42503e4e38), never ours.
    #[test]
    fn a_plain_numeric_column_through_a_derived_relation_stays_sqlites() {
        let conn = open();
        for sql in [
            "CREATE TABLE p(id INTEGER PRIMARY KEY, x NUMERIC, d DECIMAL(10,2))",
            "INSERT INTO p VALUES (1, 5, 1.5), (2, 'abc', 2.25), (3, 1.5, 3)",
            "CREATE VIEW pv AS SELECT x, d FROM p",
            "CREATE TABLE big(id INTEGER PRIMARY KEY, x NUMERIC)",
            "INSERT INTO big VALUES (1, 9223372036854775807), (2, 9223372036854775807)",
        ] {
            conn.execute(sql).unwrap();
        }
        let mut bad = diverging(
            &conn,
            &[
                ("SELECT typeof(sum(d)), sum(d) FROM (SELECT d FROM p)", "real,6.75"),
                ("SELECT sum(d) = 6.75, sum(d) > 100 FROM (SELECT d FROM p)", "1,0"),
                ("WITH c AS (SELECT d FROM p) SELECT typeof(sum(d)), sum(d) FROM c", "real,6.75"),
                ("SELECT typeof(sum(d)), sum(d) FROM pv", "real,6.75"),
                ("SELECT typeof(sum(x)), sum(x) FROM (SELECT x FROM p)", "real,6.5"),
                ("SELECT typeof(sum(x)), sum(x) FROM pv", "real,6.5"),
                ("SELECT x FROM (SELECT x FROM p) ORDER BY x", "1.5;5;abc"),
            ],
            "SQLite 3.46.1",
        );
        match table_of(&conn, "SELECT sum(x) FROM (SELECT x FROM big)") {
            Err(e) if e.contains("integer overflow") => {}
            other => bad.push(format!("sum of two INT64_MAX through a subquery gave {other:?}, not SQLite's integer overflow")),
        }
        match conn.execute("CREATE TABLE pc AS SELECT x FROM pv") {
            Ok(()) => bad.extend(diverging(
                &conn,
                &[
                    ("SELECT sql FROM sqlite_schema WHERE name = 'pc'", "CREATE TABLE pc(x NUM)"),
                    ("SELECT typeof(x), x FROM pc ORDER BY rowid", "integer,5;text,abc;real,1.5"),
                ],
                "SQLite 3.46.1",
            )),
            Err(e) => bad.push(format!("CREATE TABLE pc AS SELECT x FROM pv refused: {e}; SQLite creates pc(x NUM)")),
        }
        assert!(bad.is_empty(), "CLAIM: a plain NUMERIC column read through a derived relation was not SQLite's: {bad:#?}");
    }

    /// Engine review 2bdcd32c6 HIGH 7 (raw types/probe3.out.txt): a CREATE TABLE AS that keeps a
    /// custom type makes a STRICT copy (437f337be), and gave every other column the STRICT name of
    /// its affinity, so `mix(b numeric(8, 2), y INTEGER) STRICT` copied text 'abc' and blob X'01'
    /// from a non-STRICT INT column without a type check: a STRICT table holding values STRICT
    /// forbids, which then refused an INSERT of its own rows. A copied column whose values the
    /// STRICT name may not hold takes ANY. Expected: the values are copied as they are and the copy
    /// takes its own rows again, as python sqlite3 3.46.1 copies and re-inserts them (`mixplain`,
    /// UW run typesrev-oracle2; artie raw frontier/fastest/lanes/engine/raw/sqlite-types-rev2bd.txt
    /// 42503e4e38). The control: a column copied from a STRICT INTEGER column still refuses text,
    /// as STRICT INTEGER does in that raw (`st`).
    #[test]
    fn a_strict_create_table_as_copy_holds_only_values_its_types_take() {
        let conn = open();
        for sql in [
            "CREATE TABLE n(id INTEGER PRIMARY KEY, k INTEGER, b numeric(8, 2)) STRICT",
            "INSERT INTO n VALUES (1, 10, 1.00), (2, 20, 2.00)",
            "CREATE TABLE lax(id INTEGER PRIMARY KEY, y INT)",
            "INSERT INTO lax VALUES (1, 'abc'), (2, x'01')",
        ] {
            conn.execute(sql).unwrap();
        }
        let mut bad = Vec::new();
        for sql in [
            "CREATE TABLE mix AS SELECT n.b, lax.y FROM n JOIN lax USING (id)",
            "INSERT INTO mix SELECT * FROM mix",
            "INSERT INTO mix VALUES (5, 'abc')",
            "CREATE TABLE keep AS SELECT n.k, n.b FROM n",
        ] {
            if let Err(e) = conn.execute(sql) {
                bad.push(format!("{sql} refused: {e}; SQLite copies and inserts these values"));
            }
        }
        bad.extend(diverging(
            &conn,
            &[(
                "SELECT b, typeof(y), quote(y) FROM mix ORDER BY rowid",
                "1.00,text,'abc';2.00,blob,X'01';1.00,text,'abc';2.00,blob,X'01';5.00,text,'abc'",
            )],
            "SQLite 3.46.1",
        ));
        match conn.execute("INSERT INTO keep VALUES ('abc', 1)") {
            Err(e) if e.to_string().contains("cannot store TEXT value in INTEGER column") => {}
            other => bad.push(format!("text into keep.k (copied from STRICT INTEGER n.k) gave {other:?}, not STRICT's refusal")),
        }
        assert!(bad.is_empty(), "CLAIM: a STRICT CREATE TABLE AS copy held or refused values unlike its types: {bad:#?}");
    }

    /// Engine review 2bdcd32c6 HIGH 6 (raw types/probe2.out.txt): CREATE TABLE AS over numeric
    /// arithmetic or a window sum failed "type numeric requires 2 parameter(s), got 0": the result is
    /// numeric with no typmod (437f337be keeps the type a value carries), which a column here cannot
    /// declare. PostgreSQL 18.6 creates both copies; its values for this exact SQL (UW run
    /// typesrev-oracle2; artie raw frontier/fastest/lanes/engine/raw/pg18-types-rev2bd.txt
    /// 42503e4e38) are the expected ones, never ours.
    #[test]
    fn create_table_as_over_numeric_arithmetic_or_a_window_sum_succeeds() {
        let conn = open();
        conn.execute("CREATE TABLE n(id INTEGER PRIMARY KEY, b numeric(8, 2)) STRICT").unwrap();
        conn.execute("INSERT INTO n VALUES (1, 1.00), (2, 2.00), (3, NULL), (4, 10.00)").unwrap();
        let mut bad = Vec::new();
        for sql in [
            "CREATE TABLE nc AS SELECT id, b, b + 1 AS b1 FROM n",
            "CREATE TABLE nw AS SELECT id, sum(b) OVER () AS sw FROM n",
        ] {
            if let Err(e) = conn.execute(sql) {
                bad.push(format!("{sql} refused: {e}; PostgreSQL 18 creates it"));
            }
        }
        bad.extend(diverging(
            &conn,
            &[
                ("SELECT id, b, b1 FROM nc ORDER BY id", "1,1.00,2.00;2,2.00,3.00;3,NULL,NULL;4,10.00,11.00"),
                ("SELECT count(*) FROM nw", "4"),
            ],
            "PostgreSQL 18",
        ));
        assert!(bad.is_empty(), "CLAIM: CREATE TABLE AS over numeric arithmetic or a window sum failed: {bad:#?}");
    }

    /// Engine review 2bdcd32c6 MED 6 (raw types/probe2.out.txt): a window sum over numeric(8, 2)
    /// never became the numeric sum (97e6bf4cb's optimizer pass walked only the plan's aggregates),
    /// so `sum(b) OVER (...)` answered SQLite's 1.0, 3.0 where PostgreSQL answers 1.00, 3.00, and the
    /// numeric sum's inverse step (a frame that drops rows) was never reached. Expected values are
    /// PostgreSQL 18.6's for this exact SQL (UW run typesrev-oracle2; artie raw
    /// frontier/fastest/lanes/engine/raw/pg18-types-rev2bd.txt 42503e4e38), never ours. A frame of
    /// `1 PRECEDING AND CURRENT ROW` drops a row at every step (the inverse); `OVER ()` does not.
    #[test]
    fn a_window_sum_over_numeric_sums_as_numeric() {
        let conn = open();
        conn.execute("CREATE TABLE n(id INTEGER PRIMARY KEY, b numeric(8, 2)) STRICT").unwrap();
        conn.execute("INSERT INTO n VALUES (1, 1.00), (2, 2.00), (3, NULL), (4, 10.00)").unwrap();
        let mut bad = diverging(
            &conn,
            &[
                (
                    "SELECT id, sum(b) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM n ORDER BY id",
                    "1,1.00;2,3.00;3,2.00;4,10.00",
                ),
                (
                    "SELECT id, sum(b) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) FROM n ORDER BY id",
                    "1,2.00;2,NULL;3,10.00;4,NULL",
                ),
                ("SELECT id, sum(b) OVER () FROM n ORDER BY id", "1,13.00;2,13.00;3,13.00;4,13.00"),
            ],
            "PostgreSQL 18",
        );
        match conn.execute("CREATE TABLE nw AS SELECT id, sum(b) OVER () AS sw FROM n") {
            Ok(()) => bad.extend(diverging(
                &conn,
                &[("SELECT id, sw FROM nw ORDER BY id", "1,13.00;2,13.00;3,13.00;4,13.00")],
                "PostgreSQL 18",
            )),
            Err(e) => bad.push(format!("CREATE TABLE nw AS SELECT id, sum(b) OVER () refused: {e}")),
        }
        assert!(bad.is_empty(), "CLAIM: a window sum over numeric was not numeric's: {bad:#?}");
    }

    /// Engine review 2bdcd32c6 MED 7 (raw types/probe1.out.txt): 58045fb81 ENCODEs a SET's literal
    /// once at plan time, as PostgreSQL coerces a constant when it plans (22001 even when no row
    /// matches), but an UPDATE with SQLite's OR clause is not PostgreSQL's statement: `UPDATE OR
    /// IGNORE vc SET v = 'abcdef' WHERE id = 1` over a STRICT varchar(5) column was refused with no
    /// row to update. SQLite runs a constraint per row, so an UPDATE OR IGNORE that matches no row
    /// succeeds and changes nothing (python sqlite3 3.46.1, UW run typesrev-oracle2; artie raw
    /// frontier/fastest/lanes/engine/raw/sqlite-types-rev2bd.txt 42503e4e38, the CHECK (length(v) <=
    /// 5) table). The plain UPDATE stays refused, as PostgreSQL 18.6 refuses it in that run (22001):
    /// the control.
    #[test]
    fn update_or_ignore_matching_no_row_is_not_refused_by_a_plan_time_encode() {
        let conn = open();
        conn.execute("CREATE TABLE vc(id INTEGER PRIMARY KEY, v varchar(5)) STRICT").unwrap();
        let mut bad = Vec::new();
        match conn.execute("UPDATE vc SET v = 'abcdef' WHERE id = 1") {
            Err(e) if e.to_string().contains("value too long for type character varying(5)") => {}
            other => bad.push(format!("plain UPDATE gave {other:?}, not PostgreSQL's 22001 (control)")),
        }
        match conn.execute("UPDATE OR IGNORE vc SET v = 'abcdef' WHERE id = 1") {
            Ok(()) => bad.extend(diverging(&conn, &[("SELECT changes(), count(*) FROM vc", "0,0")], "SQLite 3.46.1")),
            Err(e) => bad.push(format!("UPDATE OR IGNORE matching no row refused: {e}; SQLite succeeds")),
        }
        assert!(bad.is_empty(), "CLAIM: UPDATE OR IGNORE matching no row was refused: {bad:#?}");
    }

    /// Engine review 2bdcd32c6 MED 9 (raw types/probe2.out.txt): a CAST to a custom type yields
    /// the type's ENCODE, its stored form (the CAST design: `CAST(42 AS cents)` is 4200), so
    /// CAST(1.5 AS numeric(8, 2)) is numeric's encoded blob; yet 437f337be typed it as numeric, so
    /// `get_column_type_info` announced numeric over a blob, a derived relation's column claimed
    /// numeric values it did not hold, and CREATE TABLE AS declared numeric(8, 2) and ENCODEd the
    /// blob again. PostgreSQL 18.6 answers 1.50 typed numeric (UW run typesrev-oracle2; artie raw
    /// frontier/fastest/lanes/engine/raw/pg18-types-rev2bd.txt 42503e4e38). The claim here is that
    /// a result is never announced as a type whose values it does not hold: either PostgreSQL's
    /// answer (1.50 announced numeric) or the blob announced as the blob it is (BLOB); and a copy
    /// holds what its SELECT yields, as SQLite's CREATE TABLE AS does. A CAST whose type's DECODE is
    /// the value itself (date) is that type: the control.
    #[test]
    fn a_cast_to_an_encoding_type_is_announced_as_what_it_yields() {
        let conn = open();
        let mut bad = Vec::new();
        for sql in [
            "SELECT CAST(1.5 AS numeric(8, 2))",
            "SELECT c FROM (SELECT CAST(1.5 AS numeric(8, 2)) AS c)",
        ] {
            let stmt = conn.prepare(sql);
            let ty = stmt
                .as_ref()
                .ok()
                .and_then(|s| s.get_column_type_info(0).ok().flatten())
                .map(|i| i.declared_name.to_ascii_lowercase());
            let got = stmt.and_then(|mut s| s.run_collect_rows()).map(|rows| rows[0][0].clone());
            let consistent = match (&got, ty.as_deref()) {
                (Ok(v), Some("numeric")) => v.to_text() == Some("1.50"),
                (Ok(Value::Blob(_)), Some("blob")) => true,
                _ => false,
            };
            if !consistent {
                bad.push(format!("{sql}: value {got:?} announced {ty:?}: neither PostgreSQL's 1.50 numeric nor a blob announced BLOB"));
            }
        }
        let selected = table_of(&conn, "SELECT hex(CAST(1.5 AS numeric(8, 2)))");
        match conn.execute("CREATE TABLE cc AS SELECT CAST(1.5 AS numeric(8, 2)) AS c") {
            Ok(()) => {
                let copied = table_of(&conn, "SELECT hex(c) FROM cc");
                if copied != selected {
                    bad.push(format!("the copy holds {copied:?}, its SELECT yields {selected:?}"));
                }
            }
            Err(e) => bad.push(format!("CREATE TABLE cc AS SELECT CAST(1.5 AS numeric(8, 2)) refused: {e}")),
        }
        let date = conn.prepare("SELECT CAST('2024-01-02' AS date)").ok();
        let date_ty = date
            .as_ref()
            .and_then(|s| s.get_column_type_info(0).ok().flatten())
            .map(|i| i.declared_name.to_ascii_lowercase());
        if date_ty.as_deref() != Some("date") {
            bad.push(format!("CAST('2024-01-02' AS date) announced {date_ty:?}, not date (control)"));
        }
        assert!(bad.is_empty(), "CLAIM: a CAST to a custom type was announced as a type it does not yield: {bad:#?}");
    }

    /// The HIGH 8 class's residual (engine review 2bdcd32c6 HIGH 8; found by probe
    /// typesrev2bd-probeA at 323655cfe): with a domain over numeric(8, 2) taking numeric's
    /// operators, its `+` is numeric's (a + 1 is 2.50) but its sum was still SQLite's (real 11.5),
    /// because the numeric sum is chosen by the argument type's own `+` and a domain has none.
    /// Expected values are PostgreSQL 18.6's for this exact SQL (UW run typesrev2bd-oracle3; artie
    /// raw frontier/fastest/lanes/engine/raw/pg18-types-rev2bd-2.txt 83ba1bd4ed), never ours: sum
    /// of a domain over numeric is numeric 11.50, a window's too. `a + 1` is the control.
    #[test]
    fn a_sum_over_a_domain_over_numeric_sums_as_numeric() {
        let conn = open();
        for sql in [
            "CREATE DOMAIN dpos AS numeric(8, 2) CHECK (VALUE > 0)",
            "CREATE TABLE d(id INTEGER PRIMARY KEY, a dpos) STRICT",
            "INSERT INTO d VALUES (1, 1.5), (2, 10)",
        ] {
            conn.execute(sql).unwrap();
        }
        let mut bad = diverging(
            &conn,
            &[
                ("SELECT a + 1 FROM d ORDER BY id", "2.50;11.00"),
                ("SELECT sum(a) FROM d", "11.50"),
                ("SELECT sum(a) OVER (ORDER BY id) FROM d ORDER BY id", "1.50;11.50"),
            ],
            "PostgreSQL 18",
        );
        let ty = conn
            .prepare("SELECT sum(a) FROM d")
            .ok()
            .and_then(|s| s.get_column_type_info(0).ok().flatten())
            .map(|i| i.declared_name.to_ascii_lowercase());
        if ty.as_deref() != Some("numeric") {
            bad.push(format!("sum(a) announced {ty:?}, not PostgreSQL's numeric"));
        }
        assert!(bad.is_empty(), "CLAIM: a sum over a domain over numeric was not numeric's: {bad:#?}");
    }

    /// The HIGH 5 class's arithmetic half (engine review 2bdcd32c6 HIGH 5; raw types/probe2 and
    /// probe typesrev2bd-probeA at 323655cfe): an arithmetic operator a type defines takes every
    /// operand of a COLUMN of that type (da850ed1c), but a numeric value that is not a column (sum(b),
    /// max(b), a derived relation's typed column, b + 1) took SQLite's arithmetic on its text:
    /// `sum(b) + 1` was 14.0 and `(b + 1) * 2` 4.0 where PostgreSQL answers 14.00 and 4.00, though
    /// both are typed numeric. Expected values are PostgreSQL 18.6's for this exact SQL (UW run
    /// typesrev2bd-oracle3; artie raw frontier/fastest/lanes/engine/raw/pg18-types-rev2bd-2.txt
    /// 83ba1bd4ed), never ours; the derived column is read WHERE bb IS NOT NULL so the row order does
    /// not depend on where NULL sorts. `b * 2` over the column is the control.
    #[test]
    fn arithmetic_over_a_numeric_aggregate_or_derived_value_is_numerics() {
        let conn = open();
        conn.execute("CREATE TABLE n(id INTEGER PRIMARY KEY, b numeric(8, 2)) STRICT").unwrap();
        conn.execute("INSERT INTO n VALUES (1, 1.00), (2, 2.00), (3, NULL), (4, 10.00)").unwrap();
        let bad = diverging(
            &conn,
            &[
                ("SELECT b * 2 FROM n WHERE id = 4", "20.00"),
                ("SELECT sum(b) + 1, sum(b) - 0.5, max(b) * 2 FROM n", "14.00,12.50,20.00"),
                ("SELECT s + 1, 1 + s FROM (SELECT sum(b) AS s FROM n) q", "14.00,14.00"),
                ("SELECT bb * 2 FROM (SELECT b AS bb FROM n) q WHERE bb IS NOT NULL ORDER BY bb", "2.00;4.00;20.00"),
                ("SELECT (b + 1) * 2 FROM n ORDER BY id", "4.00;6.00;NULL;22.00"),
            ],
            "PostgreSQL 18",
        );
        assert!(bad.is_empty(), "CLAIM: arithmetic over a numeric value that is not a column was SQLite's: {bad:#?}");
    }

    /// Fresh-context review of this lane's fixes (item 1): `CAST(x AS NUMERIC)` gives numeric no
    /// parameters, so `translate_expr` runs SQLite's CAST, but `order_by::expr_custom_type` typed it
    /// numeric, and the derived-value operators of c8c6dd210 and 2f1ecab8f then sent it to numeric's
    /// operators: `CAST('7' AS NUMERIC) / 2` was numeric_div's 3.5000000000000000 and `= 'abc'`
    /// raised. A CAST the translator does not apply is not the type's. Expected values are python
    /// sqlite3 3.46.1's for this exact SQL (UW run typesrev2bd-oracle4; artie raw
    /// frontier/fastest/lanes/engine/raw/sqlite-types-rev2bd-2.txt 9294fdb8a2), never ours.
    #[test]
    fn a_sqlite_cast_to_numeric_keeps_sqlites_operators() {
        let conn = open();
        for sql in [
            "CREATE TABLE p(id INTEGER PRIMARY KEY, x)",
            "INSERT INTO p VALUES (1, '10'), (2, '9'), (3, 'abc')",
        ] {
            conn.execute(sql).unwrap();
        }
        let bad = diverging(
            &conn,
            &[
                ("SELECT CAST('7' AS NUMERIC) / 2, typeof(CAST('7' AS NUMERIC) / 2)", "3,integer"),
                ("SELECT typeof(CAST(1 AS NUMERIC) + 1), CAST(1 AS NUMERIC) + 1", "integer,2"),
                ("SELECT CAST('5' AS NUMERIC) = 'abc', CAST('5' AS NUMERIC) < 'abc'", "0,1"),
                ("SELECT CAST(5 AS NUMERIC) / 0", "NULL"),
                ("SELECT CAST('2.5' AS NUMERIC) * 2, typeof(CAST('2.5' AS NUMERIC) * 2)", "5.0,real"),
                ("SELECT id FROM p ORDER BY CAST(x AS NUMERIC), id", "3;2;1"),
                ("SELECT id FROM p WHERE CAST(x AS NUMERIC) > 9.5 ORDER BY id", "1"),
            ],
            "SQLite 3.46.1",
        );
        assert!(bad.is_empty(), "CLAIM: SQLite's CAST(x AS NUMERIC) took numeric's operators: {bad:#?}");
    }

    /// Fresh-context review of this lane's fixes (item 4): e090ecc38 types a CAST by its type only
    /// when every DECODE of its chain is literally `value`, so CAST(1 AS boolean), whose DECODE is
    /// `CASE WHEN value THEN 1 ELSE 0 END` (the identity on what its ENCODE stores, 0 or 1), was
    /// announced INTEGER, and a derived relation's and a CREATE TABLE AS copy's column lost the
    /// type. PostgreSQL 18.6 types it boolean (UW run typesrev2bd-oracle4; artie raw
    /// frontier/fastest/lanes/engine/raw/pg18-types-rev2bd-3.txt 9294fdb8a2).
    #[test]
    fn a_cast_to_boolean_is_announced_boolean() {
        let conn = open();
        let declared = |sql: &str| {
            conn.prepare(sql)
                .ok()
                .and_then(|s| s.get_column_type_info(0).ok().flatten())
                .map(|i| i.declared_name.to_ascii_lowercase())
        };
        let mut bad = Vec::new();
        if let Err(e) = conn.execute("CREATE TABLE cb AS SELECT CAST(1 AS boolean) AS f") {
            bad.push(format!("CREATE TABLE cb AS SELECT CAST(1 AS boolean) refused: {e}"));
        }
        for sql in [
            "SELECT CAST(1 AS boolean)",
            "SELECT f FROM (SELECT CAST(0 AS boolean) AS f)",
            "SELECT f FROM cb",
        ] {
            let ty = declared(sql);
            if ty.as_deref() != Some("boolean") {
                bad.push(format!("{sql} announced {ty:?}, not PostgreSQL's boolean"));
            }
        }
        assert!(bad.is_empty(), "CLAIM: a CAST to boolean was not announced boolean: {bad:#?}");
    }

    /// Fresh-context review of this lane's fixes (item 3): with a domain over numeric(8, 2) taking
    /// numeric's operators (76fa781f8), a scan finds `a = 10` through numeric_eq, but an index on
    /// the column still sought the raw key 10 among numeric's encoded blobs (the seek, the
    /// equality's agreement check, the IN-seek refusal and the term's consumption read the domain's
    /// own TypeDef, which has no ENCODE and no '='): the answer depended on the plan. Expected values
    /// are PostgreSQL 18.6's for this exact SQL with that index (UW run typesrev2bd-oracle4; artie
    /// raw frontier/fastest/lanes/engine/raw/pg18-types-rev2bd-3.txt 9294fdb8a2), by the seek and by
    /// a scan (NOT INDEXED); that `a = 10` seeks the index is a premise.
    #[test]
    fn a_domain_over_numeric_answers_by_its_index_as_by_a_scan() {
        let conn = open();
        for sql in [
            "CREATE DOMAIN dpos AS numeric(8, 2) CHECK (VALUE > 0)",
            "CREATE TABLE d(id INTEGER PRIMARY KEY, a dpos) STRICT",
            "INSERT INTO d VALUES (3, 1.5), (6, 10), (7, 9)",
            "CREATE INDEX da ON d(a)",
        ] {
            conn.execute(sql).unwrap();
        }
        let seek = "SELECT id FROM d WHERE a = 10 ORDER BY id";
        assert!(seeks(&conn, seek, "d", "da"), "premise: {seek} seeks index da: {:?}", plan(&conn, seek));
        let mut bad = Vec::new();
        for (pred, want) in [("a = 10", "6"), ("a = 1.5", "3"), ("a IN (10, 9)", "6;7"), ("a = 10.001", "")] {
            for from in ["d", "d NOT INDEXED"] {
                let sql = format!("SELECT id FROM {from} WHERE {pred} ORDER BY id");
                bad.extend(diverging(&conn, &[(sql.as_str(), want)], "PostgreSQL 18"));
            }
        }
        assert!(bad.is_empty(), "CLAIM: a domain over numeric answered by its index unlike a scan: {bad:#?}");
    }

    /// Fresh-context review of this lane's fixes (item 5): a materialized subquery's typed numeric
    /// column compares through numeric's '=' in a scan (c8c6dd210), but a probe of the
    /// subquery through an ephemeral index on it compares the key plainly with the column's
    /// decoded text, so the answer could depend on the plan: `s = 2` against '2.00'. Expected
    /// values are PostgreSQL 18.6's numeric comparison for this exact SQL (UW run
    /// typesrev2bd-oracle5; artie raw frontier/fastest/lanes/engine/raw/pg18-types-rev2bd-4.txt
    /// e0503a1e62), never ours. The join probes the CTE once per row of n.
    #[test]
    fn a_materialized_numeric_column_answers_by_a_probe_as_by_a_scan() {
        let conn = open();
        conn.execute("CREATE TABLE n(id INTEGER PRIMARY KEY, b numeric(8, 2)) STRICT").unwrap();
        conn.execute("INSERT INTO n VALUES (1, 1.00), (2, 2.00), (3, NULL), (4, 10.00)").unwrap();
        let bad = diverging(
            &conn,
            &[
                ("WITH q AS MATERIALIZED (SELECT b AS s FROM n) SELECT count(*) FROM q WHERE s = 2", "1"),
                ("WITH q AS MATERIALIZED (SELECT b AS s FROM n) SELECT count(*) FROM n JOIN q ON q.s = 2", "4"),
                ("WITH q AS MATERIALIZED (SELECT b AS s FROM n) SELECT count(*) FROM n JOIN q ON q.s > 1.5", "8"),
            ],
            "PostgreSQL 18",
        );
        assert!(bad.is_empty(), "CLAIM: a materialized numeric column answered by a probe unlike a scan: {bad:#?}");
    }

    /// Second fresh-context review of this lane's fixes (item 1): since c8c6dd210 a scan compares
    /// `b + 1 = 3` over numeric(8, 2) through numeric_eq (the value is numeric's text '3.00'), but an
    /// index on the expression `b + 1` sought the plain key 3 among those texts, and a range on it
    /// compared text with a number by SQLite's type order: the answer depended on the plan.
    /// Expected values are PostgreSQL 18.6's for this exact SQL with that index (UW run
    /// typesrev2bd-oracle6; artie raw frontier/fastest/lanes/engine/raw/pg18-types-rev2bd-5.txt
    /// 6fbfd1c0d1), never ours, by the index and by a scan (NOT INDEXED).
    #[test]
    fn an_index_on_a_numeric_expression_answers_as_a_scan() {
        let conn = open();
        for sql in [
            "CREATE TABLE n(id INTEGER PRIMARY KEY, b numeric(8, 2)) STRICT",
            "INSERT INTO n VALUES (1, 2.00), (2, 9.00), (3, NULL)",
            "CREATE INDEX nb1 ON n(b + 1)",
        ] {
            conn.execute(sql).unwrap();
        }
        let mut bad = Vec::new();
        for (pred, want) in [("b + 1 = 3", "1"), ("b + 1 = 10", "2"), ("b + 1 > 2", "1;2"), ("b + 1 < 4", "1")] {
            for from in ["n", "n NOT INDEXED"] {
                let sql = format!("SELECT id FROM {from} WHERE {pred} ORDER BY id");
                bad.extend(diverging(&conn, &[(sql.as_str(), want)], "PostgreSQL 18"));
            }
        }
        assert!(bad.is_empty(), "CLAIM: an index on a numeric expression answered unlike a scan: {bad:#?}");
    }

    /// Second fresh-context review of this lane's fixes (item 2): a numeric column compared with a
    /// numeric value that is neither a constant nor a column (b + 10, b * 1.0, a derived relation's
    /// typed column) took the plain comparison of their texts, since the column's rule takes only a
    /// compatible constant: `b < b + 10` missed 2.00 ('2.00' < '12.00' is false as text), and
    /// `b = b * 1.0` compared '2.00' with '2.000'. A value of the column's own type compares through
    /// the type's operator, as two columns of it do. Expected values are PostgreSQL 18.6's for this
    /// exact SQL (UW run typesrev2bd-oracle6; artie raw
    /// frontier/fastest/lanes/engine/raw/pg18-types-rev2bd-5.txt 6fbfd1c0d1), never ours.
    #[test]
    fn a_numeric_column_compares_with_a_numeric_value_through_numerics_operators() {
        let conn = open();
        conn.execute("CREATE TABLE n(id INTEGER PRIMARY KEY, b numeric(8, 2)) STRICT").unwrap();
        conn.execute("INSERT INTO n VALUES (1, 1.00), (2, 2.00), (3, NULL), (4, 10.00)").unwrap();
        let bad = diverging(
            &conn,
            &[
                ("SELECT count(*) FROM n WHERE b < b + 10", "3"),
                ("SELECT count(*) FROM n WHERE b = b * 1.0", "3"),
                ("SELECT count(*) FROM n WHERE b + 10 > b", "3"),
                ("SELECT n.id FROM n JOIN (SELECT b * 1.0 AS s FROM n) q ON n.b = q.s ORDER BY n.id", "1;2;4"),
            ],
            "PostgreSQL 18",
        );
        assert!(bad.is_empty(), "CLAIM: a numeric column compared with a numeric value as text: {bad:#?}");
    }

    /// Third fresh-context review of this lane's fixes (item 1): a join on numeric values compares
    /// through numeric_eq in a nested loop, but a hash join hashes and compares the values
    /// plainly, so `t1.b = t2.c * 1.0` ('0.50' against '0.500') and numeric(8, 2) against
    /// numeric(8, 3) found no pair when the planner chose a hash join. Expected values are
    /// PostgreSQL 18.6's for this exact SQL (UW run typesrev2bd-oracle7; artie raw
    /// frontier/fastest/lanes/engine/raw/pg18-types-rev2bd-6.txt), never ours. The same-typmod
    /// join is the control. 100 unindexed rows a side, so a hash join is worth planning.
    #[test]
    fn a_join_on_numeric_values_answers_as_numerics_equality_by_any_plan() {
        let conn = open();
        for sql in [
            "CREATE TABLE t1(id INTEGER PRIMARY KEY, b numeric(8, 2)) STRICT",
            "CREATE TABLE t2(id INTEGER PRIMARY KEY, c numeric(8, 2), d numeric(8, 3)) STRICT",
            "INSERT INTO t1 WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 100) SELECT i, i / 2.0 FROM s",
            "INSERT INTO t2 WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 100) SELECT i, i / 2.0, i / 2.0 FROM s",
        ] {
            conn.execute(sql).unwrap();
        }
        let bad = diverging(
            &conn,
            &[
                ("SELECT count(*) FROM t1 JOIN t2 ON t1.b = t2.c", "100"),
                ("SELECT count(*) FROM t1 JOIN t2 ON t1.b = t2.c * 1.0", "100"),
                ("SELECT count(*) FROM t1 JOIN t2 ON t1.b = t2.d", "100"),
            ],
            "PostgreSQL 18",
        );
        assert!(bad.is_empty(), "CLAIM: a join on numeric values answered unlike numeric's equality: {bad:#?}");
    }
}
