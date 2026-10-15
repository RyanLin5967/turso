use super::*;
use crate::translate::plan::BitSet;
use crate::vdbe::insn::NullMatchingMask;
use turso_parser::ast::NullsOrder;

fn index_seek_affinities(seek_def: &SeekDef, seek_key: &SeekKey) -> String {
    // Apply the constraint's resolved comparison affinity to the seek key,
    // not the indexed column's affinity.
    seek_def
        .iter(seek_key)
        .zip(seek_def.iter_affinity(seek_key))
        .map(|(key_component, aff)| match key_component {
            SeekKeyComponent::Expr(expr) if aff.expr_needs_no_affinity_change(expr) => {
                affinity::SQLITE_AFF_BLOB
            }
            _ => aff.aff_mask(),
        })
        .collect()
}

/// ENCODE each custom-type key component of an index seek in place, as the index stores it. For a
/// type with a function '=' (`seek_key_eq_function`), `emit_seek_key_encode_or_empty`: a key the
/// ENCODE refuses or does not keep exactly makes the seek empty (a jump to `loop_end`).
#[allow(clippy::too_many_arguments)]
fn encode_seek_keys_for_custom_types(
    program: &mut ProgramBuilder,
    tables: &TableReferences,
    seek_index: &Arc<Index>,
    start_reg: usize,
    num_keys: usize,
    idx_col_offset: usize,
    loop_end: BranchOffset,
    resolver: &Resolver<'_>,
) -> crate::Result<()> {
    let table = tables
        .find_table_by_identifier(&seek_index.table_name)
        .or_else(|| tables.find_table_by_table_name(&seek_index.table_name));
    let table = match table {
        Some(t) => t,
        None => return Ok(()),
    };
    let columns = table.columns();
    for i in 0..num_keys {
        let idx_col_pos = idx_col_offset + i;
        if idx_col_pos >= seek_index.columns.len() {
            break;
        }
        let idx_col = &seek_index.columns[idx_col_pos];
        let table_col = match columns.get(idx_col.pos_in_table) {
            Some(c) => c,
            None => continue,
        };
        let type_def = match resolver
            .schema()
            .get_type_def(&table_col.ty_str, table.is_strict())
        {
            Some(td) => td,
            None => continue,
        };
        let encode_expr = match type_def.encode() {
            Some(e) => e,
            None => continue,
        };
        let reg = start_reg + i;
        let skip_label = program.allocate_label();
        program.emit_insn(Insn::IsNull {
            reg,
            target_pc: skip_label,
        });
        if let Some(eq_func) = crate::translate::expr::seek_key_eq_function(type_def) {
            emit_seek_key_encode_or_empty(
                program,
                SeekKeyEncode {
                    encode_expr,
                    reg,
                    column: table_col,
                    type_def: &**type_def,
                    eq_func,
                    loop_end,
                    done: skip_label,
                },
                resolver,
            )?;
        } else {
            crate::translate::expr::emit_type_expr(
                program,
                encode_expr,
                reg,
                reg,
                table_col,
                type_def,
                resolver,
            )?;
        }
        program.preassign_label_to_next_insn(skip_label);
    }
    Ok(())
}

/// One seek key component of a custom type with a function '=' (`emit_seek_key_encode_or_empty`).
struct SeekKeyEncode<'a> {
    encode_expr: &'a turso_parser::ast::Expr,
    /// Holds the key as given; holds its encoding once the code below falls through to `done`.
    reg: usize,
    column: &'a crate::schema::Column,
    type_def: &'a crate::schema::TypeDef,
    eq_func: &'a str,
    /// The seek's loop end: an empty seek.
    loop_end: BranchOffset,
    /// Past the key's encoding.
    done: BranchOffset,
}

/// Engine review 16 HIGH 2: an index seek's key never goes through an ENCODE that can raise or
/// lose precision. The ENCODE runs inside a catch region (`Insn::CatchBegin`; a RAISE in it jumps
/// too, `ProgramBuilder::catch_raise_target`), into a register of its own; the encoding is decoded
/// and compared with the key under the type's own '='. A key the ENCODE refuses (numeric's "out of
/// range", a length check's 'value too long') or does not keep (1.501 cut to numeric(10, 2)'s 1.50)
/// equals no stored value, so the seek is empty: what a scan, which passes the operand to the
/// operator as given, answers. Otherwise the key becomes its encoding, and the WHERE term stays for
/// the type's operator to re-check each row (`seek_key_eq_function`).
///
/// ```text
///   CatchBegin refused
///   encoded = ENCODE(key)          ; RAISE -> Goto refused
///   args[0] = DECODE(encoded)
///   args[1] = key
///   equal   = eq_func(args)
///   CatchEnd
///   IfNot equal -> loop_end        ; NULL too
///   key = encoded
///   Goto done
/// refused:
///   CatchEnd
///   Goto loop_end
/// ```
fn emit_seek_key_encode_or_empty(
    program: &mut ProgramBuilder,
    key: SeekKeyEncode<'_>,
    resolver: &Resolver<'_>,
) -> crate::Result<()> {
    let func = resolver.resolve_function(key.eq_func, 2)?.ok_or_else(|| {
        crate::LimboError::InternalError(format!("function not found: {}", key.eq_func))
    })?;
    let refused = program.allocate_label();
    let encoded = program.alloc_register();
    let args = program.alloc_registers(2);
    let equal = program.alloc_register();
    program.emit_insn(Insn::CatchBegin { target_pc: refused });
    let outer = program.catch_raise_target.replace(refused);
    let round_trip = crate::translate::expr::emit_type_expr(
        program,
        key.encode_expr,
        key.reg,
        encoded,
        key.column,
        key.type_def,
        resolver,
    )
    .and_then(|_| match key.type_def.decode() {
        Some(decode_expr) => crate::translate::expr::emit_type_expr(
            program,
            decode_expr,
            encoded,
            args,
            key.column,
            key.type_def,
            resolver,
        )
        .map(|_| ()),
        None => {
            program.emit_insn(Insn::Copy {
                src_reg: encoded,
                dst_reg: args,
                extra_amount: 0,
            });
            Ok(())
        }
    });
    program.catch_raise_target = outer;
    round_trip?;
    program.emit_insn(Insn::Copy {
        src_reg: key.reg,
        dst_reg: args + 1,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: args,
        dest: equal,
        func: crate::function::FuncCtx {
            func,
            arg_count: 2,
        },
    });
    program.emit_insn(Insn::CatchEnd);
    program.emit_insn(Insn::IfNot {
        reg: equal,
        target_pc: key.loop_end,
        jump_if_null: true,
    });
    program.emit_insn(Insn::Copy {
        src_reg: encoded,
        dst_reg: key.reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Goto {
        target_pc: key.done,
    });
    program.preassign_label_to_next_insn(refused);
    program.emit_insn(Insn::CatchEnd);
    program.emit_insn(Insn::Goto {
        target_pc: key.loop_end,
    });
    Ok(())
}

/// Seek-based loop setup.
///
/// A seek loop has a real two-phase contract:
/// 1. Emit and position using the start bound.
/// 2. Emit the termination bound and anchor `loop_start`.
pub(super) struct SeekEmitter<'a, 'plan> {
    program: &'a mut ProgramBuilder,
    tables: &'a TableReferences,
    seek_def: &'a SeekDef,
    t_ctx: &'a mut TranslateCtx<'plan>,
    seek_cursor_id: usize,
    start_reg: usize,
    loop_end: BranchOffset,
    seek_index: Option<&'a Arc<Index>>,
    is_index: bool,
}

impl<'a, 'plan> SeekEmitter<'a, 'plan> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        program: &'a mut ProgramBuilder,
        tables: &'a TableReferences,
        seek_def: &'a SeekDef,
        t_ctx: &'a mut TranslateCtx<'plan>,
        seek_cursor_id: usize,
        start_reg: usize,
        loop_end: BranchOffset,
        seek_index: Option<&'a Arc<Index>>,
    ) -> Self {
        Self {
            program,
            tables,
            seek_def,
            t_ctx,
            seek_cursor_id,
            start_reg,
            loop_end,
            seek_index,
            is_index: seek_index.is_some(),
        }
    }

    /// Emit the start bound and position the cursor at the first candidate row.
    fn emit_start_bound(&mut self, use_bloom_filter: bool) -> Result<()> {
        if self.seek_def.prefix.is_empty()
            && matches!(self.seek_def.start.last_component, SeekKeyComponent::None)
        {
            match self.seek_def.iter_dir {
                IterationDirection::Forwards => {
                    if self.seek_index.is_some_and(|index| {
                        index.columns[0].effective_nulls_order() == NullsOrder::First
                    }) {
                        self.program.emit_null(self.start_reg, None);
                        self.program.emit_insn(Insn::SeekGT {
                            is_index: self.is_index,
                            cursor_id: self.seek_cursor_id,
                            start_reg: self.start_reg,
                            num_regs: 1,
                            target_pc: self.loop_end,
                        });
                    } else {
                        self.program.emit_insn(Insn::Rewind {
                            cursor_id: self.seek_cursor_id,
                            pc_if_empty: self.loop_end,
                        });
                    }
                }
                IterationDirection::Backwards => {
                    if self.seek_index.is_some_and(|index| {
                        index.columns[0].effective_nulls_order() == NullsOrder::Last
                    }) {
                        self.program.emit_null(self.start_reg, None);
                        self.program.emit_insn(Insn::SeekLT {
                            is_index: self.is_index,
                            cursor_id: self.seek_cursor_id,
                            start_reg: self.start_reg,
                            num_regs: 1,
                            target_pc: self.loop_end,
                        });
                    } else {
                        self.program.emit_insn(Insn::Last {
                            cursor_id: self.seek_cursor_id,
                            pc_if_empty: self.loop_end,
                        });
                    }
                }
            }
            return Ok(());
        }

        for (i, key) in self.seek_def.iter(&self.seek_def.start).enumerate() {
            let reg = self.start_reg + i;
            match key {
                SeekKeyComponent::Expr(expr) => {
                    translate_expr_no_constant_opt(
                        self.program,
                        Some(self.tables),
                        expr,
                        reg,
                        &self.t_ctx.resolver,
                        NoConstantOptReason::RegisterReuse,
                    )?;
                    // A NULL key can never satisfy `=`, so the loop is done as
                    // soon as one shows up. `IS` matches NULL instead: keep the
                    // NULL in the seek register and let the index comparison
                    // find the rows whose key component is NULL.
                    if !expr.is_nonnull(self.tables)
                        && !self.seek_def.is_null_matching_key_component(i)
                    {
                        self.program.emit_insn(Insn::IsNull {
                            reg,
                            target_pc: self.loop_end,
                        });
                    }
                }
                SeekKeyComponent::Null => self.program.emit_null(reg, None),
                SeekKeyComponent::None => {
                    unreachable!("None component is not possible in iterator")
                }
            }
        }
        let num_regs = self.seek_def.size(&self.seek_def.start);
        // Which key components match NULL rather than comparing with `=`; the
        // seek and the bloom-filter probe keep their "NULL key cannot match"
        // shortcut for the rest.
        let mut null_matching_bits = BitSet::default();
        for i in 0..num_regs {
            if self.seek_def.is_null_matching_key_component(i) {
                null_matching_bits.set(i)?;
            }
        }
        let null_matching_mask = NullMatchingMask::from(null_matching_bits);

        if let Some(idx) = self.seek_index {
            encode_seek_keys_for_custom_types(
                self.program,
                self.tables,
                idx,
                self.start_reg,
                num_regs,
                0,
                self.loop_end,
                &self.t_ctx.resolver,
            )?;
            let affinities = index_seek_affinities(self.seek_def, &self.seek_def.start);
            if affinities.chars().any(|c| c != affinity::SQLITE_AFF_BLOB) {
                self.program.emit_insn(Insn::Affinity {
                    start_reg: self.start_reg,
                    count: std::num::NonZeroUsize::new(num_regs).unwrap(),
                    affinities,
                });
            }
            if use_bloom_filter {
                turso_assert!(
                    idx.ephemeral,
                    "bloom filter can only be used with ephemeral indexes"
                );
                // The probe treats a NULL key as "definitely absent", which
                // would skip rows whose key IS NULL. `emit_autoindex` never
                // builds a filter for a NULL-matching seek, so probing one
                // here means the build and probe decisions have diverged.
                turso_assert!(
                    null_matching_mask.is_empty(),
                    "a NULL-matching seek must not probe a bloom filter"
                );
                self.program.emit_insn(Insn::Filter {
                    cursor_id: self.seek_cursor_id,
                    key_reg: self.start_reg,
                    num_keys: num_regs,
                    target_pc: self.loop_end,
                });
            }
        }

        match self.seek_def.start.op {
            SeekOp::GE { eq_only } => self.program.emit_insn(Insn::SeekGE {
                is_index: self.is_index,
                cursor_id: self.seek_cursor_id,
                start_reg: self.start_reg,
                num_regs,
                target_pc: self.loop_end,
                eq_only,
                null_matching_mask,
            }),
            SeekOp::GT => self.program.emit_insn(Insn::SeekGT {
                is_index: self.is_index,
                cursor_id: self.seek_cursor_id,
                start_reg: self.start_reg,
                num_regs,
                target_pc: self.loop_end,
            }),
            SeekOp::LE { eq_only } => self.program.emit_insn(Insn::SeekLE {
                is_index: self.is_index,
                cursor_id: self.seek_cursor_id,
                start_reg: self.start_reg,
                num_regs,
                target_pc: self.loop_end,
                eq_only,
                null_matching_mask,
            }),
            SeekOp::LT => self.program.emit_insn(Insn::SeekLT {
                is_index: self.is_index,
                cursor_id: self.seek_cursor_id,
                start_reg: self.start_reg,
                num_regs,
                target_pc: self.loop_end,
            }),
        };

        Ok(())
    }

    /// Emit the end bound check and anchor the loop-start label.
    fn emit_termination(&mut self, loop_start: BranchOffset) -> Result<()> {
        if self.seek_def.prefix.is_empty()
            && matches!(self.seek_def.end.last_component, SeekKeyComponent::None)
        {
            self.program.preassign_label_to_next_insn(loop_start);
            match self.seek_def.iter_dir {
                IterationDirection::Forwards => {
                    if self.seek_index.is_some_and(|index| {
                        index.columns[0].effective_nulls_order() == NullsOrder::Last
                    }) {
                        self.program.emit_null(self.start_reg, None);
                        self.program.emit_insn(Insn::IdxGE {
                            cursor_id: self.seek_cursor_id,
                            start_reg: self.start_reg,
                            num_regs: 1,
                            target_pc: self.loop_end,
                        });
                    }
                }
                IterationDirection::Backwards => {
                    if self.seek_index.is_some_and(|index| {
                        index.columns[0].effective_nulls_order() == NullsOrder::First
                    }) {
                        self.program.emit_null(self.start_reg, None);
                        self.program.emit_insn(Insn::IdxLE {
                            cursor_id: self.seek_cursor_id,
                            start_reg: self.start_reg,
                            num_regs: 1,
                            target_pc: self.loop_end,
                        });
                    }
                }
            }
            return Ok(());
        }

        let num_regs = self.seek_def.size(&self.seek_def.end);
        let last_reg = self.start_reg + self.seek_def.prefix.len();
        match &self.seek_def.end.last_component {
            SeekKeyComponent::Expr(expr) => {
                translate_expr_no_constant_opt(
                    self.program,
                    Some(self.tables),
                    expr,
                    last_reg,
                    &self.t_ctx.resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                if let Some(idx) = self.seek_index {
                    encode_seek_keys_for_custom_types(
                        self.program,
                        self.tables,
                        idx,
                        last_reg,
                        1,
                        self.seek_def.prefix.len(),
                        self.loop_end,
                        &self.t_ctx.resolver,
                    )?;
                    let affinities = index_seek_affinities(self.seek_def, &self.seek_def.end);
                    if affinities.chars().any(|c| c != affinity::SQLITE_AFF_BLOB) {
                        self.program.emit_insn(Insn::Affinity {
                            start_reg: self.start_reg,
                            count: std::num::NonZeroUsize::new(num_regs).unwrap(),
                            affinities,
                        });
                    }
                }
                if !expr.is_nonnull(self.tables) {
                    self.program.emit_insn(Insn::IsNull {
                        reg: last_reg,
                        target_pc: self.loop_end,
                    });
                }
            }
            SeekKeyComponent::Null => self.program.emit_null(last_reg, None),
            SeekKeyComponent::None => {}
        }

        self.program.preassign_label_to_next_insn(loop_start);
        let mut rowid_reg = None;
        let mut affinity = None;
        if !self.is_index {
            rowid_reg = Some(self.program.alloc_register());
            self.program.emit_insn(Insn::RowId {
                cursor_id: self.seek_cursor_id,
                dest: rowid_reg.unwrap(),
            });

            affinity = if let Some(table_ref) = self
                .tables
                .joined_tables()
                .iter()
                .find(|t| t.columns().iter().any(|c| c.is_rowid_alias()))
            {
                if let Some(rowid_col_idx) =
                    table_ref.columns().iter().position(|c| c.is_rowid_alias())
                {
                    Some(table_ref.columns()[rowid_col_idx].affinity())
                } else {
                    Some(Affinity::Numeric)
                }
            } else {
                Some(Affinity::Numeric)
            };
        }

        match (self.is_index, self.seek_def.end.op) {
            (true, SeekOp::GE { .. }) => self.program.emit_insn(Insn::IdxGE {
                cursor_id: self.seek_cursor_id,
                start_reg: self.start_reg,
                num_regs,
                target_pc: self.loop_end,
            }),
            (true, SeekOp::GT) => self.program.emit_insn(Insn::IdxGT {
                cursor_id: self.seek_cursor_id,
                start_reg: self.start_reg,
                num_regs,
                target_pc: self.loop_end,
            }),
            (true, SeekOp::LE { .. }) => self.program.emit_insn(Insn::IdxLE {
                cursor_id: self.seek_cursor_id,
                start_reg: self.start_reg,
                num_regs,
                target_pc: self.loop_end,
            }),
            (true, SeekOp::LT) => self.program.emit_insn(Insn::IdxLT {
                cursor_id: self.seek_cursor_id,
                start_reg: self.start_reg,
                num_regs,
                target_pc: self.loop_end,
            }),
            (false, SeekOp::GE { .. }) => self.program.emit_insn(Insn::Ge {
                lhs: rowid_reg.unwrap(),
                rhs: self.start_reg,
                target_pc: self.loop_end,
                flags: CmpInsFlags::default()
                    .jump_if_null()
                    .with_affinity(affinity.unwrap()),
                collation: self.program.curr_collation(),
            }),
            (false, SeekOp::GT) => self.program.emit_insn(Insn::Gt {
                lhs: rowid_reg.unwrap(),
                rhs: self.start_reg,
                target_pc: self.loop_end,
                flags: CmpInsFlags::default()
                    .jump_if_null()
                    .with_affinity(affinity.unwrap()),
                collation: self.program.curr_collation(),
            }),
            (false, SeekOp::LE { .. }) => self.program.emit_insn(Insn::Le {
                lhs: rowid_reg.unwrap(),
                rhs: self.start_reg,
                target_pc: self.loop_end,
                flags: CmpInsFlags::default()
                    .jump_if_null()
                    .with_affinity(affinity.unwrap()),
                collation: self.program.curr_collation(),
            }),
            (false, SeekOp::LT) => self.program.emit_insn(Insn::Lt {
                lhs: rowid_reg.unwrap(),
                rhs: self.start_reg,
                target_pc: self.loop_end,
                flags: CmpInsFlags::default()
                    .jump_if_null()
                    .with_affinity(affinity.unwrap()),
                collation: self.program.curr_collation(),
            }),
        }
        Ok(())
    }

    pub(super) fn emit(mut self, loop_start: BranchOffset, use_bloom_filter: bool) -> Result<()> {
        self.emit_start_bound(use_bloom_filter)?;
        self.emit_termination(loop_start)
    }
}
