use super::*;
use crate::numeric::pg::{PgNumericOp, PgNumericType};

pub(crate) fn pg_numeric_type(expr: &Expr, tables: Option<&TableReferences>, resolver: &Resolver) -> PgNumericType {
    use PgNumericType::*;
    match expr {
        Expr::Column { table, column, .. } => {
            if let Some((_, reference)) = tables.and_then(|t| t.find_table_by_internal_id(*table)) {
                if let Table::FromClauseSubquery(subquery) = reference {
                    let plan = &subquery.plan;
                    if let Some(result) = plan.select_result_columns().get(*column) {
                        return pg_numeric_type(&result.expr, Some(plan.select_table_references()), resolver);
                    }
                }
            }
            resolve_column_type_str(*table, *column, tables, resolver)
                .and_then(|name| pg_type_name(&name, resolver)).unwrap_or(Dynamic)
        }
        Expr::Cast { type_name: Some(name), .. } => pg_type_name(&name.name, resolver).unwrap_or(Dynamic),
        Expr::Literal(ast::Literal::Numeric(n)) => {
            if n.parse::<i32>().is_ok() { Int4 }
            else if n.parse::<i64>().is_ok() { Int8 }
            else { Numeric }
        }
        Expr::Unary(UnaryOperator::Negative, arg) => {
            if let Expr::Literal(ast::Literal::Numeric(n)) = arg.as_ref() {
                let signed = format!("-{n}");
                if signed.parse::<i32>().is_ok() { return Int4; }
                if signed.parse::<i64>().is_ok() { return Int8; }
            }
            pg_numeric_type(arg, tables, resolver)
        }
        Expr::Unary(_, arg) | Expr::Collate(arg, _) => pg_numeric_type(arg, tables, resolver),
        Expr::Parenthesized(args) if args.len() == 1 => pg_numeric_type(&args[0], tables, resolver),
        Expr::Binary(a, op, b) if matches!(op, ast::Operator::Add | ast::Operator::Subtract | ast::Operator::Multiply | ast::Operator::Divide) => {
            pg_numeric_type(a, tables, resolver).wider(pg_numeric_type(b, tables, resolver))
        }
        Expr::FunctionCall { name, args, .. } => {
            let first = args.first().map(|arg| pg_numeric_type(arg, tables, resolver)).unwrap_or(Dynamic);
            match name.as_str().to_ascii_lowercase().as_str() {
                "sum" => match first { Int2 | Int4 => Int8, Int8 | Numeric => Numeric, other => other },
                "abs" | "min" | "max" | "coalesce" | "nullif" => first,
                "count" | "row_number" | "rank" | "dense_rank" => Int8,
                _ => Dynamic,
            }
        }
        Expr::Variable(v) => v.col_type.as_ref().and_then(|name| pg_type_name(name.as_ref(), resolver)).unwrap_or(Dynamic),
        Expr::Case { when_then_pairs, else_expr, .. } => {
            when_then_pairs.iter().map(|(_, e)| pg_numeric_type(e, tables, resolver))
                .chain(else_expr.iter().map(|e| pg_numeric_type(e, tables, resolver)))
                .fold(Dynamic, PgNumericType::wider)
        }
        _ => Dynamic,
    }
}

fn pg_type_name(name: &str, resolver: &Resolver) -> Option<PgNumericType> {
    PgNumericType::from_name(name).or_else(|| {
        let resolved = resolver.schema().resolve_type_unchecked(name).ok()??;
        resolved.chain.iter().find_map(|t| PgNumericType::from_name(t.base()))
            .or_else(|| PgNumericType::from_name(&resolved.primitive))
    })
}

pub(super) fn emit_pg_numeric(program: &mut ProgramBuilder, op: PgNumericOp, ty: PgNumericType,
    lhs: usize, rhs: Option<usize>, dest: usize) {
    program.emit_insn(Insn::PgNumeric { op, ty, lhs, rhs, dest });
}

pub(crate) fn emit_pg_column_coercions(program: &mut ProgramBuilder, resolver: &Resolver, columns: &[Column],
    start: usize, only: Option<&crate::translate::plan::ColumnMask>, layout: &ColumnLayout) {
    if resolver.dialect.name() != "postgres" { return; }
    for (i, col) in columns.iter().enumerate() {
        if col.is_array() || col.is_virtual_generated() || only.is_some_and(|only| !only.get(i)) { continue; }
        if let Some(ty) = pg_type_name(&col.ty_str, resolver).filter(|t| *t != PgNumericType::Numeric) {
            let reg = layout.to_register(start, i);
            emit_pg_numeric(program, PgNumericOp::Cast { source: PgNumericType::Dynamic }, ty, reg, None, reg);
        }
    }
}

pub(super) fn translate_pg_numeric(program: &mut ProgramBuilder, tables: Option<&TableReferences>, expr: &Expr,
    target: usize, resolver: &Resolver) -> Result<bool> {
    if resolver.dialect.name() != "postgres" { return Ok(false); }
    match expr {
        Expr::Cast { expr: arg, type_name: Some(name) } => {
            let Some(ty) = PgNumericType::from_name(&name.name) else { return Ok(false); };
            let source = pg_numeric_type(arg, tables, resolver);
            if let Expr::Literal(ast::Literal::Numeric(text)) = arg.as_ref() {
                if source == PgNumericType::Numeric {
                    program.emit_insn(Insn::String8 { value: text.clone(), dest: target });
                } else { translate_expr(program, tables, arg, target, resolver)?; }
            } else { translate_expr(program, tables, arg, target, resolver)?; }
            emit_pg_numeric(program, PgNumericOp::Cast { source }, ty, target, None, target);
        }
        Expr::Binary(a, op, b) => {
            let op = match op {
                ast::Operator::Add => PgNumericOp::Add,
                ast::Operator::Subtract => PgNumericOp::Subtract,
                ast::Operator::Multiply => PgNumericOp::Multiply,
                ast::Operator::Divide => PgNumericOp::Divide,
                _ => return Ok(false),
            };
            let ty = pg_numeric_type(expr, tables, resolver);
            let lhs = program.alloc_register();
            let rhs = program.alloc_register();
            translate_expr(program, tables, a, lhs, resolver)?;
            translate_expr(program, tables, b, rhs, resolver)?;
            emit_pg_numeric(program, op, ty, lhs, Some(rhs), target);
        }
        Expr::Unary(UnaryOperator::Negative, arg) if !matches!(arg.as_ref(), Expr::Literal(_)) => {
            let ty = pg_numeric_type(arg, tables, resolver);
            let reg = program.alloc_register();
            translate_expr(program, tables, arg, reg, resolver)?;
            emit_pg_numeric(program, PgNumericOp::Negate, ty, reg, None, target);
        }
        Expr::FunctionCall { name, args, .. } if name.as_str().eq_ignore_ascii_case("abs") && args.len() == 1 => {
            let ty = pg_numeric_type(&args[0], tables, resolver);
            let reg = program.alloc_register();
            translate_expr(program, tables, &args[0], reg, resolver)?;
            emit_pg_numeric(program, PgNumericOp::Abs, ty, reg, None, target);
        }
        _ => return Ok(false),
    }
    Ok(true)
}
