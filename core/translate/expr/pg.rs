use super::*;
use crate::numeric::pg::{PgNumericOp, PgNumericType};
use crate::translate::plan::{Plan, QueryDestination};

/// Operand widths on the engine tree. The frontend preserves inferred expression types as
/// casts; schema/derived-column resolution also covers direct PG engine and parser callers.
pub(crate) fn pg_numeric_type(
    expr: &Expr,
    tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Result<PgNumericType> {
    let _nest = crate::stack::NestGuard::enter("a recursive step")?;
    use PgNumericType::*;
    Ok(match expr {
        Expr::Column { table, column, .. } => {
            if let Some((_, Table::FromClauseSubquery(subquery))) =
                tables.and_then(|t| t.find_table_by_internal_id(*table))
            {
                return pg_plan_column_type(&subquery.plan, *column, resolver);
            }
            resolve_column_type_str(*table, *column, tables, resolver)
                .and_then(|name| pg_type_name(&name, resolver))
                .unwrap_or(Dynamic)
        }
        Expr::Cast {
            type_name: Some(name),
            ..
        } => pg_type_name(&name.name, resolver).unwrap_or(Dynamic),
        Expr::Literal(ast::Literal::Numeric(n)) => {
            if n.parse::<i32>().is_ok() {
                Int4
            } else if n.parse::<i64>().is_ok() {
                Int8
            } else {
                Numeric
            }
        }
        Expr::Unary(UnaryOperator::Negative, arg) => {
            if let Expr::Literal(ast::Literal::Numeric(n)) = arg.as_ref() {
                let signed = format!("-{n}");
                if signed.parse::<i32>().is_ok() {
                    return Ok(Int4);
                }
                if signed.parse::<i64>().is_ok() {
                    return Ok(Int8);
                }
            }
            pg_numeric_type(arg, tables, resolver)?
        }
        Expr::Unary(_, arg) | Expr::Collate(arg, _) => pg_numeric_type(arg, tables, resolver)?,
        Expr::Parenthesized(args) if args.len() == 1 => {
            pg_numeric_type(&args[0], tables, resolver)?
        }
        Expr::Binary(a, op, b)
            if matches!(
                op,
                ast::Operator::Add
                    | ast::Operator::Subtract
                    | ast::Operator::Multiply
                    | ast::Operator::Divide
            ) =>
        {
            pg_numeric_type(a, tables, resolver)?
                .arithmetic_type(pg_numeric_type(b, tables, resolver)?)
        }
        Expr::FunctionCall { name, args, .. } => {
            let first = args
                .first()
                .map(|arg| pg_numeric_type(arg, tables, resolver))
                .transpose()?
                .unwrap_or(Dynamic);
            match name.as_str().to_ascii_lowercase().as_str() {
                "sum" => first.sum_type(),
                "abs" | "nullif" => first,
                "min" | "max" | "coalesce" => {
                    let mut common = first;
                    for arg in args.iter().skip(1) {
                        common = common.wider(pg_numeric_type(arg, tables, resolver)?);
                    }
                    common
                }
                "count" | "row_number" | "rank" | "dense_rank" => Int8,
                "length" | "char_length" | "character_length" | "octet_length" | "strpos" => Int4,
                "random" | "sqrt" | "exp" | "ln" | "sin" | "cos" | "tan" => Float8,
                _ => Dynamic,
            }
        }
        Expr::Register(reg) => resolver
            .register_pg_types
            .get(reg)
            .copied()
            .unwrap_or(Dynamic),
        Expr::Variable(v) => v
            .col_type
            .as_ref()
            .and_then(|name| pg_type_name(name.as_ref(), resolver))
            .unwrap_or(Dynamic),
        Expr::Case {
            when_then_pairs,
            else_expr,
            ..
        } => {
            let mut common = Dynamic;
            for (_, expr) in when_then_pairs {
                common = common.wider(pg_numeric_type(expr, tables, resolver)?);
            }
            if let Some(expr) = else_expr {
                common = common.wider(pg_numeric_type(expr, tables, resolver)?);
            }
            common
        }
        _ => Dynamic,
    })
}

fn pg_select_column_type(
    plan: &crate::translate::plan::SelectPlan,
    column: usize,
    resolver: &Resolver,
) -> Result<PgNumericType> {
    if !plan.values.is_empty() {
        let mut common = PgNumericType::Dynamic;
        for row in &plan.values {
            if let Some(expr) = row.get(column) {
                common = common.wider(pg_numeric_type(
                    expr,
                    Some(&plan.table_references),
                    resolver,
                )?);
            }
        }
        return Ok(common);
    }
    plan.result_columns
        .get(column)
        .map(|result| pg_numeric_type(&result.expr, Some(&plan.table_references), resolver))
        .unwrap_or(Ok(PgNumericType::Dynamic))
}

fn pg_plan_column_type(plan: &Plan, column: usize, resolver: &Resolver) -> Result<PgNumericType> {
    let _nest = crate::stack::NestGuard::enter("a recursive step")?;
    match plan {
        Plan::Select(select) => pg_select_column_type(select, column, resolver),
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            let mut common = pg_select_column_type(right_most, column, resolver)?;
            for (select, _) in left {
                common = common.wider(pg_select_column_type(select, column, resolver)?);
            }
            Ok(common)
        }
        Plan::RecursiveCte(cte) => pg_plan_column_type(&cte.initial_query, column, resolver),
        _ => Ok(PgNumericType::Dynamic),
    }
}

pub(super) fn pg_type_name(name: &str, resolver: &Resolver) -> Option<PgNumericType> {
    PgNumericType::from_name(name).or_else(|| {
        let resolved = resolver.schema().resolve_type_unchecked(name).ok()??;
        // A domain inherits its base's SQL type. A custom type's storage primitive does
        // not: boolean and oid both store integers without becoming PostgreSQL int4.
        for ty in &resolved.chain {
            if !ty.is_domain {
                return PgNumericType::from_name(&ty.name);
            }
            if let Some(base) = PgNumericType::from_name(ty.base()) {
                return Some(base);
            }
        }
        None
    })
}

pub(super) fn emit_pg_numeric(
    program: &mut ProgramBuilder,
    op: PgNumericOp,
    ty: PgNumericType,
    lhs: usize,
    rhs: Option<usize>,
    dest: usize,
) {
    program.emit_insn(Insn::PgNumeric {
        op,
        ty,
        lhs,
        rhs,
        dest,
    });
}

fn column_numeric_type(column: &Column, resolver: &Resolver) -> Option<PgNumericType> {
    if column.is_array() || column.is_virtual_generated() {
        return None;
    }
    pg_type_name(&column.ty_str, resolver).filter(|t| *t != PgNumericType::Numeric)
}

pub(crate) fn emit_pg_assignment(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    column: &Column,
    reg: usize,
    source: PgNumericType,
) {
    if resolver.dialect.name() != "postgres" {
        return;
    }
    if let Some(target) = column_numeric_type(column, resolver) {
        emit_pg_numeric(
            program,
            PgNumericOp::Cast { source },
            target,
            reg,
            None,
            reg,
        );
    }
}

pub(crate) fn emit_pg_column_coercions(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    columns: &[Column],
    start: usize,
    only: Option<&crate::translate::plan::ColumnMask>,
    layout: &ColumnLayout,
) {
    if resolver.dialect.name() != "postgres" {
        return;
    }
    for (i, col) in columns.iter().enumerate() {
        if only.is_some_and(|only| !only.get(i)) {
            continue;
        }
        emit_pg_assignment(
            program,
            resolver,
            col,
            layout.to_register(start, i),
            PgNumericType::Dynamic,
        );
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PgCoercion {
    pub source: PgNumericType,
    pub target: Option<PgNumericType>,
}

pub(crate) fn pg_insert_coercions(
    table: &Table,
    columns: &[ast::Name],
    resolver: &Resolver,
) -> Vec<PgCoercion> {
    if resolver.dialect.name() != "postgres" {
        return Vec::new();
    }
    let targets: Vec<_> = if columns.is_empty() {
        table
            .columns()
            .iter()
            .filter(|c| !c.is_generated() && !c.hidden())
            .collect()
    } else {
        columns
            .iter()
            .filter_map(|name| {
                table
                    .get_column_by_name(name.as_str())
                    .map(|(_, column)| column)
            })
            .collect()
    };
    targets
        .into_iter()
        .map(|column| PgCoercion {
            source: PgNumericType::Dynamic,
            target: column_numeric_type(column, resolver),
        })
        .collect()
}

/// Resolve source kinds before sort/compound materialization loses runtime value provenance.
pub(crate) fn prepare_pg_result_coercions(plan: &mut Plan, resolver: &Resolver) -> Result<()> {
    if resolver.dialect.name() != "postgres" {
        return Ok(());
    }
    let count = match plan.select_query_destination() {
        Some(QueryDestination::CoroutineYield { pg_coercions, .. }) => pg_coercions.len(),
        _ => 0,
    };
    let sources = (0..count)
        .map(|i| pg_plan_column_type(plan, i, resolver))
        .collect::<Result<Vec<_>>>()?;
    if let Some(QueryDestination::CoroutineYield { pg_coercions, .. }) =
        plan.select_query_destination_mut()
    {
        for (coercion, source) in pg_coercions.iter_mut().zip(sources) {
            coercion.source = source;
        }
    }
    Ok(())
}

// Materialize the common source type before UNION sorting/deduplication and before
// assignment. Otherwise numeric 0.5 in a float8 UNION would round as numeric (1)
// instead of as the UNION's float8 (0).
fn normalize_pg_select(
    select: &mut crate::translate::plan::SelectPlan,
    sources: &[PgNumericType],
    resolver: &Resolver,
) -> Result<()> {
    fn cast(
        expr: &mut Expr,
        target: PgNumericType,
        tables: &TableReferences,
        resolver: &Resolver,
    ) -> Result<()> {
        if target != PgNumericType::Dynamic
            && pg_numeric_type(expr, Some(tables), resolver)? != target
        {
            let old = std::mem::replace(expr, Expr::Literal(ast::Literal::Null));
            *expr = Expr::Cast {
                expr: Box::new(old),
                type_name: Some(ast::Type {
                    name: target.name().into(),
                    size: None,
                    array_dimensions: 0,
                }),
            };
        }
        Ok(())
    }
    for (column, source) in select.result_columns.iter_mut().zip(sources) {
        cast(
            &mut column.expr,
            *source,
            &select.table_references,
            resolver,
        )?;
    }
    for row in &mut select.values {
        for (expr, source) in row.iter_mut().zip(sources) {
            cast(expr, *source, &select.table_references, resolver)?;
        }
    }
    if let QueryDestination::CoroutineYield { pg_coercions, .. } = &mut select.query_destination {
        for (coercion, source) in pg_coercions.iter_mut().zip(sources) {
            coercion.source = *source;
        }
    }
    Ok(())
}

/// Coerce every compound arm at planning time, including FROM and CTE subqueries.
pub(crate) fn prepare_pg_compound_source_types(plan: &mut Plan, resolver: &Resolver) -> Result<()> {
    if resolver.dialect.name() != "postgres" {
        return Ok(());
    }
    let sources = (0..plan.select_result_columns().len())
        .map(|i| pg_plan_column_type(plan, i, resolver))
        .collect::<Result<Vec<_>>>()?;
    if let Plan::CompoundSelect {
        left, right_most, ..
    } = plan
    {
        for (select, _) in left {
            normalize_pg_select(select, &sources, resolver)?;
        }
        normalize_pg_select(right_most, &sources, resolver)?;
    }
    Ok(())
}

/// INSERT VALUES has a target context per cell. A VALUES relation instead has a common
/// source type per column, just as each arm of a compound SELECT does.
pub(crate) fn prepare_pg_values_source_types(
    plan: &mut crate::translate::plan::SelectPlan,
    resolver: &Resolver,
) -> Result<()> {
    if resolver.dialect.name() != "postgres"
        || matches!(&plan.query_destination,
        QueryDestination::CoroutineYield { pg_coercions, .. } if !pg_coercions.is_empty())
    {
        return Ok(());
    }
    let sources = (0..plan.result_columns.len())
        .map(|i| pg_select_column_type(plan, i, resolver))
        .collect::<Result<Vec<_>>>()?;
    normalize_pg_select(plan, &sources, resolver)
}

pub(crate) fn emit_pg_result_coercions(
    program: &mut ProgramBuilder,
    coercions: &[PgCoercion],
    start: usize,
) {
    for (i, coercion) in coercions.iter().enumerate() {
        if let Some(target) = coercion.target {
            emit_pg_numeric(
                program,
                PgNumericOp::Cast {
                    source: coercion.source,
                },
                target,
                start + i,
                None,
                start + i,
            );
        }
    }
}

pub(crate) fn emit_pg_values_coercions(
    program: &mut ProgramBuilder,
    plan: &crate::translate::plan::SelectPlan,
    row: &[Expr],
    start: usize,
    resolver: &Resolver,
) -> Result<()> {
    if let QueryDestination::CoroutineYield { pg_coercions, .. } = &plan.query_destination {
        for (i, (coercion, expr)) in pg_coercions.iter().zip(row).enumerate() {
            if let Some(target) = coercion.target {
                let source = pg_numeric_type(expr, Some(&plan.table_references), resolver)?;
                emit_pg_numeric(
                    program,
                    PgNumericOp::Cast { source },
                    target,
                    start + i,
                    None,
                    start + i,
                );
            }
        }
    }
    Ok(())
}

pub(super) fn pg_binary_operation(
    op: &ast::Operator,
    a: PgNumericType,
    b: PgNumericType,
) -> Option<(PgNumericOp, PgNumericType)> {
    let ty = if matches!(
        op,
        ast::Operator::Add
            | ast::Operator::Subtract
            | ast::Operator::Multiply
            | ast::Operator::Divide
    ) {
        a.arithmetic_type(b)
    } else {
        a.wider(b)
    };
    let op = match op {
        ast::Operator::Add => PgNumericOp::Add,
        ast::Operator::Subtract => PgNumericOp::Subtract,
        ast::Operator::Multiply => PgNumericOp::Multiply,
        ast::Operator::Divide => PgNumericOp::Divide,
        ast::Operator::Equals if ty == PgNumericType::Numeric => PgNumericOp::Equal,
        ast::Operator::NotEquals if ty == PgNumericType::Numeric => PgNumericOp::NotEqual,
        ast::Operator::Less if ty == PgNumericType::Numeric => PgNumericOp::Less,
        ast::Operator::LessEquals if ty == PgNumericType::Numeric => PgNumericOp::LessEqual,
        ast::Operator::Greater if ty == PgNumericType::Numeric => PgNumericOp::Greater,
        ast::Operator::GreaterEquals if ty == PgNumericType::Numeric => PgNumericOp::GreaterEqual,
        _ => return None,
    };
    Some((op, ty))
}

pub(super) fn translate_pg_numeric(
    program: &mut ProgramBuilder,
    tables: Option<&TableReferences>,
    expr: &Expr,
    target: usize,
    resolver: &Resolver,
) -> Result<bool> {
    if resolver.dialect.name() != "postgres" {
        return Ok(false);
    }
    match expr {
        Expr::Literal(ast::Literal::Numeric(text))
            if pg_numeric_type(expr, tables, resolver)? == PgNumericType::Numeric =>
        {
            program.emit_insn(Insn::String8 {
                value: text.clone(),
                dest: target,
            });
            emit_pg_numeric(
                program,
                PgNumericOp::Cast {
                    source: PgNumericType::Numeric,
                },
                PgNumericType::Numeric,
                target,
                None,
                target,
            );
        }
        Expr::Cast {
            expr: arg,
            type_name: Some(name),
        } => {
            let Some(ty) = PgNumericType::from_name(&name.name) else {
                return Ok(false);
            };
            if ty == PgNumericType::Numeric && name.size.is_some() {
                return Ok(false);
            }
            let source = pg_numeric_type(arg, tables, resolver)?;
            translate_expr(program, tables, arg, target, resolver)?;
            emit_pg_numeric(
                program,
                PgNumericOp::Cast { source },
                ty,
                target,
                None,
                target,
            );
        }
        Expr::Unary(UnaryOperator::Negative, arg) => {
            if let Expr::Literal(ast::Literal::Numeric(text)) = arg.as_ref() {
                if let Ok(value) = format!("-{text}").parse::<i64>() {
                    program.emit_insn(Insn::Integer {
                        value,
                        dest: target,
                    });
                    return Ok(true);
                }
            }
            let ty = pg_numeric_type(arg, tables, resolver)?;
            let reg = program.alloc_register();
            translate_expr(program, tables, arg, reg, resolver)?;
            emit_pg_numeric(program, PgNumericOp::Negate, ty, reg, None, target);
        }
        Expr::FunctionCall { name, args, .. }
            if name.as_str().eq_ignore_ascii_case("abs") && args.len() == 1 =>
        {
            let ty = pg_numeric_type(&args[0], tables, resolver)?;
            let reg = program.alloc_register();
            translate_expr(program, tables, &args[0], reg, resolver)?;
            emit_pg_numeric(program, PgNumericOp::Abs, ty, reg, None, target);
        }
        _ => return Ok(false),
    }
    Ok(true)
}

pub(crate) fn record_pg_column_type(resolver: &mut Resolver, column: &Column, reg: usize) {
    if resolver.dialect.name() == "postgres" {
        if let Some(ty) = pg_type_name(&column.ty_str, resolver) {
            resolver.register_pg_types.insert(reg, ty);
        }
    }
}

pub(crate) fn emit_pg_assignment_expr(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    column: &Column,
    expr: &Expr,
    tables: Option<&TableReferences>,
    reg: usize,
) -> Result<()> {
    if resolver.dialect.name() == "postgres" {
        emit_pg_assignment(
            program,
            resolver,
            column,
            reg,
            pg_numeric_type(expr, tables, resolver)?,
        );
    }
    Ok(())
}
