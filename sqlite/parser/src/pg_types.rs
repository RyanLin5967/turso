/// PostgreSQL's result type for arithmetic on two known numeric scalar types.
/// Unlike a common CASE/UNION type, real combined with another numeric type
/// selects a double-precision operator. Unknown and nonnumeric types remain
/// the caller's context-resolution responsibility.
pub fn numeric_arithmetic_result_oid(lhs: u32, rhs: u32) -> Option<u32> {
    const NUMERIC_TYPES: [u32; 6] = [21, 23, 20, 1700, 700, 701];
    let a = NUMERIC_TYPES.iter().position(|oid| *oid == lhs)?;
    let b = NUMERIC_TYPES.iter().position(|oid| *oid == rhs)?;
    if (lhs == 700 && rhs != 700) || (rhs == 700 && lhs != 700) {
        Some(701)
    } else {
        Some(NUMERIC_TYPES[a.max(b)])
    }
}
