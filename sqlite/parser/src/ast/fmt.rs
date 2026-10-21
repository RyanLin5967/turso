//! AST node format
use std::fmt::{self, Display, Formatter, Write};

use crate::ast::*;
use crate::token::TokenType;
use crate::token::TokenType::*;
use crate::Result;

use crate::ast::TableInternalId;

/// Context to be used in ToSqlString
pub trait ToSqlContext {
    /// Given an id, get the table name
    /// First Option indicates whether the table exists
    ///
    /// Currently not considering aliases
    fn get_table_name(&self, _id: TableInternalId) -> Option<&str> {
        None
    }

    /// Given a table id and a column index, get the column name
    /// First Option indicates whether the column exists
    /// Second Option indicates whether the column has a name
    fn get_column_name(&self, _table_id: TableInternalId, _col_idx: usize) -> Option<Option<&str>> {
        None
    }

    // help function to handle missing table/column names
    fn get_table_and_column_names(
        &self,
        table_id: TableInternalId,
        col_idx: usize,
    ) -> (String, String) {
        let table_name = self
            .get_table_name(table_id)
            .map(|s| s.to_owned())
            .unwrap_or_else(|| format!("t{}", table_id.0));

        let column_name = self
            .get_column_name(table_id, col_idx)
            .map(|opt| {
                opt.map(|s| s.to_owned())
                    .unwrap_or_else(|| format!("c{col_idx}"))
            })
            .unwrap_or_else(|| format!("c{col_idx}"));

        (table_name, column_name)
    }
}

pub struct WriteTokenStream<'a, T: Write> {
    write: &'a mut T,
    spaced: bool,
}

impl<'a, T: Write> WriteTokenStream<'a, T> {
    /// Create a new token stream writing to the specified writer
    pub fn new(write: &'a mut T) -> Self {
        Self {
            write,
            spaced: true,
        }
    }
}

impl<T: Write> TokenStream for WriteTokenStream<'_, T> {
    type Error = fmt::Error;

    fn append(&mut self, ty: TokenType, value: Option<&str>) -> fmt::Result {
        if !self.spaced {
            match ty {
                TK_COMMA | TK_SEMI | TK_RP | TK_DOT | TK_LBRACKET | TK_RBRACKET => {}
                _ => {
                    self.write.write_char(' ')?;
                    self.spaced = true;
                }
            };
        }

        match (ty, ty.as_str(), value) {
            (TK_BLOB, None, value) => {
                if let Some(str) = value {
                    self.write.write_str(str)?;
                } else {
                    self.write.write_str("X''")?;
                }
                Ok(())
            }
            (_, ty_str, value) => {
                if let Some(str) = ty_str {
                    self.write.write_str(str)?;
                    self.spaced = ty == TK_LP || ty == TK_DOT || ty == TK_LBRACKET;
                    // str should not be whitespace
                }

                if let Some(str) = value {
                    // trick for pretty-print
                    self.spaced = str.bytes().all(|b| b.is_ascii_whitespace());
                    self.write.write_str(str)?;
                }

                Ok(())
            }
        }
    }
}

pub struct BlankContext;

impl ToSqlContext for BlankContext {}

/// Stream of token
pub trait TokenStream {
    /// Potential error raised
    type Error;

    /// Push token to this stream
    fn append(&mut self, ty: TokenType, value: Option<&str>) -> Result<(), Self::Error>;

    /// Interspace iterator with commas
    fn comma<I, C: ToSqlContext>(&mut self, items: I, context: &C) -> Result<(), Self::Error>
    where
        I: IntoIterator,
        I::Item: ToTokens,
    {
        let iter = items.into_iter();
        for (i, item) in iter.enumerate() {
            if i != 0 {
                self.append(TK_COMMA, None)?;
            }
            item.to_tokens(self, context)?;
        }
        Ok(())
    }
}

pub struct SqlDisplayer<'a, 'b, C: ToSqlContext, T: ToTokens> {
    ctx: &'a C,
    start_node: &'b T,
}

impl<'a, 'b, C: ToSqlContext, T: ToTokens> SqlDisplayer<'a, 'b, C, T> {
    /// Create a new displayer with the specified context and AST node
    pub fn new(ctx: &'a C, start_node: &'b T) -> Self {
        Self { ctx, start_node }
    }
}

impl<'a, 'b, C: ToSqlContext, T: ToTokens> Display for SqlDisplayer<'a, 'b, C, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut stream = WriteTokenStream::new(f);
        self.start_node.to_tokens(&mut stream, self.ctx)
    }
}

/// Generate token(s) from AST node
/// Also implements Display to make sure devs won't forget Display
pub trait ToTokens: Display {
    /// Send token(s) to the specified stream with context
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error>;

    // Return displayer representation with context
    fn displayer<'a, 'b, C: ToSqlContext>(&'b self, ctx: &'a C) -> SqlDisplayer<'a, 'b, C, Self>
    where
        Self: Sized,
    {
        SqlDisplayer::new(ctx, self)
    }
}

macro_rules! impl_display_for_to_tokens {
    ($struct:ty) => {
        impl Display for $struct {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                self.displayer(&BlankContext).fmt(f)
            }
        }
    };
}

impl<T: ?Sized + ToTokens> ToTokens for &T {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        ToTokens::to_tokens(&**self, s, context)
    }
}

impl<T: ?Sized + ToTokens> ToTokens for Box<T> {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        ToTokens::to_tokens(self.as_ref(), s, context)
    }
}

impl ToTokens for String {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_ANY, Some(self.as_ref()))
    }
}

impl_display_for_to_tokens!(Cmd);
impl ToTokens for Cmd {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Explain(stmt) => {
                s.append(TK_EXPLAIN, None)?;
                stmt.to_tokens(s, context)?;
            }
            Self::ExplainQueryPlan { stmt, format } => {
                s.append(TK_EXPLAIN, None)?;
                s.append(TK_QUERY, None)?;
                s.append(TK_PLAN, None)?;
                if *format == EqpFormat::Json {
                    s.append(TK_ID, Some("FORMAT"))?;
                    s.append(TK_EQ, None)?;
                    s.append(TK_ID, Some("JSON"))?;
                }
                stmt.to_tokens(s, context)?;
            }
            Self::Stmt(stmt) => {
                stmt.to_tokens(s, context)?;
            }
        }
        s.append(TK_SEMI, None)
    }
}

impl_display_for_to_tokens!(Stmt);
impl ToTokens for Stmt {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::AlterTable(AlterTable { name, body }) => {
                s.append(TK_ALTER, None)?;
                s.append(TK_TABLE, None)?;
                name.to_tokens(s, context)?;
                body.to_tokens(s, context)
            }
            Self::Analyze { name } => {
                s.append(TK_ANALYZE, None)?;
                if let Some(name) = name {
                    name.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Attach { expr, db_name, key } => {
                s.append(TK_ATTACH, None)?;
                expr.to_tokens(s, context)?;
                s.append(TK_AS, None)?;
                db_name.to_tokens(s, context)?;
                if let Some(key) = key {
                    s.append(TK_KEY, None)?;
                    key.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Begin { typ, name } => {
                s.append(TK_BEGIN, None)?;
                if let Some(typ) = typ {
                    typ.to_tokens(s, context)?;
                }
                if let Some(name) = name {
                    s.append(TK_TRANSACTION, None)?;
                    name.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Commit { name } => {
                s.append(TK_COMMIT, None)?;
                if let Some(name) = name {
                    s.append(TK_TRANSACTION, None)?;
                    name.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::CreateIndex {
                unique,
                if_not_exists,
                idx_name,
                tbl_name,
                using,
                columns,
                with_clause,
                where_clause,
            } => {
                s.append(TK_CREATE, None)?;
                if *unique {
                    s.append(TK_UNIQUE, None)?;
                }
                s.append(TK_INDEX, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                idx_name.to_tokens(s, context)?;
                s.append(TK_ON, None)?;
                tbl_name.to_tokens(s, context)?;
                if let Some(using) = using {
                    s.append(TK_USING, None)?;
                    using.to_tokens(s, context)?;
                }
                s.append(TK_LP, None)?;
                comma(columns, s, context)?;
                s.append(TK_RP, None)?;
                if !with_clause.is_empty() {
                    s.append(TK_WITH, None)?;
                    s.append(TK_LP, None)?;
                    let mut first = true;
                    for (name, value) in with_clause.iter() {
                        if !first {
                            s.append(TK_COMMA, None)?;
                        }
                        first = false;
                        name.to_tokens(s, context)?;
                        s.append(TK_EQ, None)?;
                        value.to_tokens(s, context)?;
                    }
                    s.append(TK_RP, None)?;
                }
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::CreateTable {
                temporary,
                if_not_exists,
                tbl_name,
                body,
            } => {
                s.append(TK_CREATE, None)?;
                if *temporary {
                    s.append(TK_TEMP, None)?;
                }
                s.append(TK_TABLE, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                tbl_name.to_tokens(s, context)?;
                body.to_tokens(s, context)
            }
            Self::CreateTrigger {
                temporary,
                if_not_exists,
                trigger_name,
                time,
                event,
                tbl_name,
                for_each_row,
                when_clause,
                commands,
            } => {
                s.append(TK_CREATE, None)?;
                if *temporary {
                    s.append(TK_TEMP, None)?;
                }
                s.append(TK_TRIGGER, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                trigger_name.to_tokens(s, context)?;
                if let Some(time) = time {
                    time.to_tokens(s, context)?;
                }
                event.to_tokens(s, context)?;
                s.append(TK_ON, None)?;
                tbl_name.to_tokens(s, context)?;
                if *for_each_row {
                    s.append(TK_FOR, None)?;
                    s.append(TK_EACH, None)?;
                    s.append(TK_ROW, None)?;
                }
                if let Some(when_clause) = when_clause {
                    s.append(TK_WHEN, None)?;
                    when_clause.to_tokens(s, context)?;
                }
                s.append(TK_BEGIN, Some("\n"))?;
                for command in commands {
                    command.to_tokens(s, context)?;
                    s.append(TK_SEMI, Some("\n"))?;
                }
                s.append(TK_END, None)
            }
            Self::CreateView {
                temporary,
                if_not_exists,
                view_name,
                columns,
                select,
            } => {
                s.append(TK_CREATE, None)?;
                if *temporary {
                    s.append(TK_TEMP, None)?;
                }
                s.append(TK_VIEW, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                view_name.to_tokens(s, context)?;
                if !columns.is_empty() {
                    s.append(TK_LP, None)?;
                    comma(columns, s, context)?;
                    s.append(TK_RP, None)?;
                }
                s.append(TK_AS, None)?;
                select.to_tokens(s, context)
            }
            Self::CreateMaterializedView {
                if_not_exists,
                view_name,
                columns,
                select,
            } => {
                s.append(TK_CREATE, None)?;
                s.append(TK_MATERIALIZED, None)?;
                s.append(TK_VIEW, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                view_name.to_tokens(s, context)?;
                if !columns.is_empty() {
                    s.append(TK_LP, None)?;
                    comma(columns, s, context)?;
                    s.append(TK_RP, None)?;
                }
                s.append(TK_AS, None)?;
                select.to_tokens(s, context)
            }
            Self::CreateVirtualTable(CreateVirtualTable {
                if_not_exists,
                tbl_name,
                module_name,
                args,
            }) => {
                s.append(TK_CREATE, None)?;
                s.append(TK_VIRTUAL, None)?;
                s.append(TK_TABLE, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                tbl_name.to_tokens(s, context)?;
                s.append(TK_USING, None)?;
                module_name.to_tokens(s, context)?;
                s.append(TK_LP, None)?;
                if !args.is_empty() {
                    comma(args, s, context)?;
                }
                s.append(TK_RP, None)
            }
            Self::Delete {
                with,
                tbl_name,
                indexed,
                where_clause,
                returning,
            } => {
                if let Some(with) = with {
                    with.to_tokens(s, context)?;
                }
                s.append(TK_DELETE, None)?;
                s.append(TK_FROM, None)?;
                tbl_name.to_tokens(s, context)?;
                if let Some(indexed) = indexed {
                    indexed.to_tokens(s, context)?;
                }
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s, context)?;
                }
                if !returning.is_empty() {
                    s.append(TK_RETURNING, None)?;
                    comma(returning, s, context)?;
                }
                Ok(())
            }
            Self::Detach { name } => {
                s.append(TK_DETACH, None)?;
                name.to_tokens(s, context)
            }
            Self::DropIndex {
                if_exists,
                idx_name,
            } => {
                s.append(TK_DROP, None)?;
                s.append(TK_INDEX, None)?;
                if *if_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                idx_name.to_tokens(s, context)
            }
            Self::DropTable {
                if_exists,
                tbl_name,
            } => {
                s.append(TK_DROP, None)?;
                s.append(TK_TABLE, None)?;
                if *if_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                tbl_name.to_tokens(s, context)
            }
            Self::DropTrigger {
                if_exists,
                trigger_name,
            } => {
                s.append(TK_DROP, None)?;
                s.append(TK_TRIGGER, None)?;
                if *if_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                trigger_name.to_tokens(s, context)
            }
            Self::DropView {
                if_exists,
                view_name,
            } => {
                s.append(TK_DROP, None)?;
                s.append(TK_VIEW, None)?;
                if *if_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                view_name.to_tokens(s, context)
            }
            Self::Insert {
                with,
                or_conflict,
                tbl_name,
                columns,
                body,
                returning,
            } => {
                if let Some(with) = with {
                    with.to_tokens(s, context)?;
                }
                if let Some(ResolveType::Replace) = or_conflict {
                    s.append(TK_REPLACE, None)?;
                } else {
                    s.append(TK_INSERT, None)?;
                    if let Some(or_conflict) = or_conflict {
                        s.append(TK_OR, None)?;
                        or_conflict.to_tokens(s, context)?;
                    }
                }
                s.append(TK_INTO, None)?;
                tbl_name.to_tokens(s, context)?;
                if !columns.is_empty() {
                    s.append(TK_LP, None)?;
                    comma(columns, s, context)?;
                    s.append(TK_RP, None)?;
                }
                body.to_tokens(s, context)?;
                if !returning.is_empty() {
                    s.append(TK_RETURNING, None)?;
                    comma(returning, s, context)?;
                }
                Ok(())
            }
            Self::Pragma { name, body } => {
                s.append(TK_PRAGMA, None)?;
                name.to_tokens(s, context)?;
                if let Some(body) = body {
                    body.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Reindex { name } => {
                s.append(TK_REINDEX, None)?;
                if let Some(name) = name {
                    name.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Optimize { idx_name } => {
                s.append(TK_OPTIMIZE, None)?;
                s.append(TK_INDEX, None)?;
                if let Some(name) = idx_name {
                    name.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Release { name } => {
                s.append(TK_RELEASE, None)?;
                name.to_tokens(s, context)
            }
            Self::Rollback {
                tx_name,
                savepoint_name,
            } => {
                s.append(TK_ROLLBACK, None)?;
                if let Some(tx_name) = tx_name {
                    s.append(TK_TRANSACTION, None)?;
                    tx_name.to_tokens(s, context)?;
                }
                if let Some(savepoint_name) = savepoint_name {
                    s.append(TK_TO, None)?;
                    savepoint_name.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Savepoint { name } => {
                s.append(TK_SAVEPOINT, None)?;
                name.to_tokens(s, context)
            }
            Self::Select(select) => select.to_tokens(s, context),
            Self::Update(Update {
                with,
                or_conflict,
                tbl_name,
                indexed,
                sets,
                from,
                where_clause,
                returning,
            }) => {
                if let Some(with) = with {
                    with.to_tokens(s, context)?;
                }
                s.append(TK_UPDATE, None)?;
                if let Some(or_conflict) = or_conflict {
                    s.append(TK_OR, None)?;
                    or_conflict.to_tokens(s, context)?;
                }
                tbl_name.to_tokens(s, context)?;
                if let Some(indexed) = indexed {
                    indexed.to_tokens(s, context)?;
                }
                s.append(TK_SET, None)?;
                comma(sets, s, context)?;
                if let Some(from) = from {
                    s.append(TK_FROM, None)?;
                    from.to_tokens(s, context)?;
                }
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s, context)?;
                }
                if !returning.is_empty() {
                    s.append(TK_RETURNING, None)?;
                    comma(returning, s, context)?;
                }
                Ok(())
            }
            Self::Vacuum { name, into } => {
                s.append(TK_VACUUM, None)?;
                if let Some(ref name) = name {
                    name.to_tokens(s, context)?;
                }
                if let Some(ref into) = into {
                    s.append(TK_INTO, None)?;
                    into.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::CreateType {
                if_not_exists,
                type_name,
                body,
            } => {
                s.append(TK_CREATE, None)?;
                s.append(TK_TYPE, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                s.append(TK_ID, Some(type_name))?;
                match body {
                    CreateTypeBody::Struct(fields) => {
                        s.append(TK_AS, None)?;
                        s.append(TK_ID, Some("STRUCT"))?;
                        type_fields_to_tokens(fields, s, context)?;
                    }
                    CreateTypeBody::Union(fields) => {
                        s.append(TK_AS, None)?;
                        s.append(TK_ID, Some("UNION"))?;
                        type_fields_to_tokens(fields, s, context)?;
                    }
                    CreateTypeBody::CustomType {
                        params,
                        base,
                        encode,
                        decode,
                        operators,
                        default,
                    } => {
                        // Parameters
                        if !params.is_empty() {
                            s.append(TK_LP, None)?;
                            for (i, param) in params.iter().enumerate() {
                                if i > 0 {
                                    s.append(TK_COMMA, None)?;
                                }
                                s.append(TK_ID, Some(&param.name))?;
                                if let Some(ref ty) = param.ty {
                                    s.append(TK_ID, Some(ty))?;
                                }
                            }
                            s.append(TK_RP, None)?;
                        }
                        // BASE
                        s.append(TK_ID, Some("BASE"))?;
                        s.append(TK_ID, Some(base))?;
                        // ENCODE
                        if let Some(ref encode) = encode {
                            s.append(TK_ID, Some("ENCODE"))?;
                            encode.to_tokens(s, context)?;
                        }
                        // DECODE
                        if let Some(ref decode) = decode {
                            s.append(TK_ID, Some("DECODE"))?;
                            decode.to_tokens(s, context)?;
                        }
                        // DEFAULT
                        if let Some(ref default) = default {
                            s.append(TK_ID, Some("DEFAULT"))?;
                            default.to_tokens(s, context)?;
                        }
                        // OPERATOR clauses
                        for op in operators {
                            s.append(TK_ID, Some("OPERATOR"))?;
                            s.append(TK_STRING, Some(&format!("'{}'", op.op)))?;
                            if let Some(ref func_name) = op.func_name {
                                s.append(TK_ID, Some(func_name))?;
                            }
                        }
                    }
                }
                Ok(())
            }
            Self::CreateDomain {
                if_not_exists,
                domain_name,
                base_type,
                base_type_params,
                default,
                not_null,
                constraints,
            } => {
                s.append(TK_CREATE, None)?;
                s.append(TK_ID, Some("DOMAIN"))?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                s.append(TK_ID, Some(domain_name))?;
                s.append(TK_AS, None)?;
                s.append(TK_ID, Some(base_type))?;
                if !base_type_params.is_empty() {
                    s.append(TK_LP, None)?;
                    for (i, p) in base_type_params.iter().enumerate() {
                        if i > 0 {
                            s.append(TK_COMMA, None)?;
                        }
                        p.to_tokens(s, context)?;
                    }
                    s.append(TK_RP, None)?;
                }
                if let Some(ref def) = default {
                    s.append(TK_DEFAULT, None)?;
                    def.to_tokens(s, context)?;
                }
                if *not_null {
                    s.append(TK_NOT, None)?;
                    s.append(TK_NULL, None)?;
                }
                for c in constraints {
                    if let Some(ref name) = c.name {
                        s.append(TK_CONSTRAINT, None)?;
                        s.append(TK_ID, Some(name))?;
                    }
                    s.append(TK_CHECK, None)?;
                    s.append(TK_LP, None)?;
                    c.check.to_tokens(s, context)?;
                    s.append(TK_RP, None)?;
                }
                Ok(())
            }
            Self::DropType {
                if_exists,
                type_name,
            } => {
                s.append(TK_DROP, None)?;
                s.append(TK_TYPE, None)?;
                if *if_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                s.append(TK_ID, Some(type_name))?;
                Ok(())
            }
            Self::CreateSequence {
                if_not_exists,
                seq_name,
                start,
                increment,
                min_value,
                max_value,
                cycle,
            } => {
                s.append(TK_CREATE, None)?;
                s.append(TK_ID, Some("SEQUENCE"))?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                seq_name.to_tokens(s, context)?;
                if let Some(v) = start {
                    s.append(TK_ID, Some("START"))?;
                    s.append(TK_ID, Some("WITH"))?;
                    s.append(TK_ID, Some(&v.to_string()))?;
                }
                if let Some(v) = increment {
                    s.append(TK_ID, Some("INCREMENT"))?;
                    s.append(TK_ID, Some("BY"))?;
                    s.append(TK_ID, Some(&v.to_string()))?;
                }
                if let Some(v) = min_value {
                    s.append(TK_ID, Some("MINVALUE"))?;
                    s.append(TK_ID, Some(&v.to_string()))?;
                }
                if let Some(v) = max_value {
                    s.append(TK_ID, Some("MAXVALUE"))?;
                    s.append(TK_ID, Some(&v.to_string()))?;
                }
                if *cycle {
                    s.append(TK_ID, Some("CYCLE"))?;
                }
                Ok(())
            }
            Self::DropSequence {
                if_exists,
                seq_name,
            } => {
                s.append(TK_DROP, None)?;
                s.append(TK_ID, Some("SEQUENCE"))?;
                if *if_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                seq_name.to_tokens(s, context)?;
                Ok(())
            }
            Self::DropDomain {
                if_exists,
                domain_name,
            } => {
                s.append(TK_DROP, None)?;
                s.append(TK_ID, Some("DOMAIN"))?;
                if *if_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                s.append(TK_ID, Some(domain_name))?;
                Ok(())
            }
        }
    }
}

impl_display_for_to_tokens!(Expr);
// Operand parentheses (REVIEW-engine-2bdcd32c6 HIGH 3). The printer writes an operand bare where
// SQLite's parser reads the text back as the same tree, and in parentheses where it would not. A
// tree SQLite's parser built always reads back bare (its own Parenthesized nodes print as written),
// so its text is unchanged; a tree built another way, such as the PostgreSQL translator's (pg_query
// drops parentheses), printed `(a + b) * 2` as `a + b * 2`, which reads back as `a + (b * 2)`.
//
// Levels are the parser's (`Parser::current_token_precedence`): OR 0, AND 1, NOT 2, `=`/IS/IN/LIKE/
// BETWEEN/ISNULL 3, `<` 4, bitwise 6, `+` 7, `*` 8, `||`/`->` 9, COLLATE 10, prefix `-`/`+`/`~` 11,
// a subscript 12. The parser reads a binary operator's right operand at one level above it, a prefix
// operator's operand at its own level, BETWEEN's start at 3 and its end, LIKE's pattern and ESCAPE
// at 4. Two questions decide every operand:
// - `absorbs(e, p)`: would an operator of level `p` written right after `e` be read into `e`'s
//   open right end (the operand of a trailing prefix operator, binary operator, BETWEEN or LIKE)?
//   A left operand followed by its operator must not.
// - `leads_below(e, level)`: does `e` begin with a chain of operators (its left spine: binary,
//   postfix, COLLATE, subscript) one of which is below `level`? An operand read at `level` must not.

fn operator_precedence(op: &Operator) -> u8 {
    match op {
        Operator::Or => 0,
        Operator::And => 1,
        Operator::Equals | Operator::NotEquals | Operator::Is | Operator::IsNot => 3,
        Operator::Less
        | Operator::LessEquals
        | Operator::Greater
        | Operator::GreaterEquals
        | Operator::ArrayContains
        | Operator::ArrayOverlap => 4,
        Operator::BitwiseAnd | Operator::BitwiseOr | Operator::LeftShift | Operator::RightShift => {
            6
        }
        Operator::Add | Operator::Subtract => 7,
        Operator::Multiply | Operator::Divide | Operator::Modulus => 8,
        Operator::Concat | Operator::ArrowRight | Operator::ArrowRightShift => 9,
        Operator::BitwiseNot => 11,
    }
}

fn unary_precedence(op: &UnaryOperator) -> u8 {
    match op {
        UnaryOperator::Not => 2,
        UnaryOperator::Negative | UnaryOperator::Positive | UnaryOperator::BitwiseNot => 11,
    }
}

/// Whether an operator of level `p` written right after `expr` is read into `expr`'s open right
/// end, as printed.
fn absorbs(expr: &Expr, p: u8) -> bool {
    let mut expr = expr;
    loop {
        // The level the open right end is read at, and that operand, when it prints bare.
        let (level, open): (u8, Option<&Expr>) = match expr {
            Expr::Binary(_, op, rhs) => {
                let level = operator_precedence(op) + 1;
                (level, Some(&**rhs).filter(|r| !leads_below(r, level)))
            }
            Expr::Unary(op, operand) => {
                let level = unary_precedence(op);
                (level, Some(&**operand).filter(|o| !leads_below(o, level)))
            }
            // The PostgreSQL translator's folded `-5` reads back as a prefix minus.
            Expr::Literal(Literal::Numeric(n)) if n.starts_with('-') => (11, None),
            Expr::Between { end, .. } => (4, Some(&**end).filter(|e| !leads_below(e, 4))),
            Expr::Like { rhs, escape, .. } => {
                let last: &Expr = escape.as_deref().unwrap_or(rhs);
                (4, Some(last).filter(|e| !leads_below(e, 4)))
            }
            _ => return false,
        };
        if level <= p {
            return true;
        }
        match open {
            Some(open) => expr = open,
            None => return false,
        }
    }
}

/// Whether `expr`, as printed, begins with an operator chain one of whose levels is below `level`.
fn leads_below(expr: &Expr, level: u8) -> bool {
    let mut expr = expr;
    loop {
        // The level of the operator after the leading operand, and that operand.
        let (p, lead): (u8, &Expr) = match expr {
            Expr::Binary(lhs, op, _) => (operator_precedence(op), &**lhs),
            Expr::Between { lhs, .. }
            | Expr::InList { lhs, .. }
            | Expr::InSelect { lhs, .. }
            | Expr::InTable { lhs, .. }
            | Expr::Like { lhs, .. }
            | Expr::IsNull(lhs)
            | Expr::NotNull(lhs) => (3, &**lhs),
            Expr::Collate(lhs, _) => (10, &**lhs),
            Expr::Subscript { base, .. } => (12, &**base),
            _ => return false,
        };
        if p < level {
            return true;
        }
        // A leading operand that would absorb its operator prints in parentheses: nothing of it
        // leads.
        if absorbs(lead, p) {
            return false;
        }
        expr = lead;
    }
}

/// An operand, in parentheses when `wrap`.
fn operand<S: TokenStream + ?Sized, C: ToSqlContext>(
    expr: &Expr,
    wrap: bool,
    s: &mut S,
    context: &C,
) -> Result<(), S::Error> {
    if wrap {
        s.append(TK_LP, None)?;
        expr.to_tokens(s, context)?;
        s.append(TK_RP, None)
    } else {
        expr.to_tokens(s, context)
    }
}

impl ToTokens for Expr {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::SubqueryResult { .. } => {
                // FIXME: what to put here? This is a highly "artificial" AST node that has no meaning when stringified.
                Ok(())
            }
            Self::Between {
                lhs,
                not,
                start,
                end,
            } => {
                operand(lhs, absorbs(lhs, 3), s, context)?;
                if *not {
                    s.append(TK_NOT, None)?;
                }
                s.append(TK_BETWEEN, None)?;
                // The start is read at 3 and must not absorb the AND after it.
                let wrap = leads_below(start, 3) || absorbs(start, 1);
                operand(start, wrap, s, context)?;
                s.append(TK_AND, None)?;
                operand(end, leads_below(end, 4), s, context)
            }
            Self::Binary(lhs, op, rhs) => {
                let p = operator_precedence(op);
                operand(lhs, absorbs(lhs, p), s, context)?;
                op.to_tokens(s, context)?;
                operand(rhs, leads_below(rhs, p + 1), s, context)
            }
            Self::Register(reg) => {
                // This is for internal use only, not part of SQL syntax
                // Use a special notation that won't conflict with SQL
                s.append(TK_VARIABLE, Some(&format!("$r{reg}")))
            }
            Self::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                s.append(TK_CASE, None)?;
                if let Some(ref base) = base {
                    base.to_tokens(s, context)?;
                }
                for (when, then) in when_then_pairs {
                    s.append(TK_WHEN, None)?;
                    when.to_tokens(s, context)?;
                    s.append(TK_THEN, None)?;
                    then.to_tokens(s, context)?;
                }
                if let Some(ref else_expr) = else_expr {
                    s.append(TK_ELSE, None)?;
                    else_expr.to_tokens(s, context)?;
                }
                s.append(TK_END, None)
            }
            Self::Cast { expr, type_name } => {
                s.append(TK_CAST, None)?;
                s.append(TK_LP, None)?;
                expr.to_tokens(s, context)?;
                s.append(TK_AS, None)?;
                if let Some(ref type_name) = type_name {
                    type_name.to_tokens(s, context)?;
                }
                s.append(TK_RP, None)
            }
            Self::Collate(expr, collation) => {
                operand(expr, absorbs(expr, 10), s, context)?;
                s.append(TK_COLLATE, None)?;
                s.append(TK_ID, Some(&collation.as_ident()))
            }
            Self::DoublyQualified(db_name, tbl_name, col_name) => {
                db_name.to_tokens(s, context)?;
                s.append(TK_DOT, None)?;
                tbl_name.to_tokens(s, context)?;
                s.append(TK_DOT, None)?;
                col_name.to_tokens(s, context)
            }
            Self::Exists(subquery) => {
                s.append(TK_EXISTS, None)?;
                s.append(TK_LP, None)?;
                subquery.to_tokens(s, context)?;
                s.append(TK_RP, None)
            }
            Self::FunctionCall {
                name,
                distinctness,
                args,
                order_by,
                within_group,
                filter_over,
            } => {
                name.to_tokens(s, context)?;
                s.append(TK_LP, None)?;
                if let Some(distinctness) = distinctness {
                    distinctness.to_tokens(s, context)?;
                }
                if !args.is_empty() {
                    comma(args, s, context)?;
                }
                if !order_by.is_empty() {
                    s.append(TK_ORDER, None)?;
                    s.append(TK_BY, None)?;
                    comma(order_by, s, context)?;
                }
                s.append(TK_RP, None)?;
                if !within_group.is_empty() {
                    s.append(TK_WITHIN, None)?;
                    s.append(TK_GROUP, None)?;
                    s.append(TK_LP, None)?;
                    s.append(TK_ORDER, None)?;
                    s.append(TK_BY, None)?;
                    comma(within_group, s, context)?;
                    s.append(TK_RP, None)?;
                }
                filter_over.to_tokens(s, context)?;
                Ok(())
            }
            Self::FunctionCallStar { name, filter_over } => {
                name.to_tokens(s, context)?;
                s.append(TK_LP, None)?;
                s.append(TK_STAR, None)?;
                s.append(TK_RP, None)?;
                filter_over.to_tokens(s, context)?;
                Ok(())
            }
            Self::Id(id) => id.to_tokens(s, context),
            Self::Column { table, column, .. } => {
                let (tbl_name, col_name) = context.get_table_and_column_names(*table, *column);
                s.append(TK_ID, Some(tbl_name.as_ref()))?;
                s.append(TK_DOT, None)?;
                s.append(TK_ID, Some(col_name.as_ref()))
            }
            Self::InList { lhs, not, rhs } => {
                operand(lhs, absorbs(lhs, 3), s, context)?;
                if *not {
                    s.append(TK_NOT, None)?;
                }
                s.append(TK_IN, None)?;
                s.append(TK_LP, None)?;
                if !rhs.is_empty() {
                    comma(rhs, s, context)?;
                }
                s.append(TK_RP, None)
            }
            Self::InSelect { lhs, not, rhs } => {
                operand(lhs, absorbs(lhs, 3), s, context)?;
                if *not {
                    s.append(TK_NOT, None)?;
                }
                s.append(TK_IN, None)?;
                s.append(TK_LP, None)?;
                rhs.to_tokens(s, context)?;
                s.append(TK_RP, None)
            }
            Self::InTable {
                lhs,
                not,
                rhs,
                args,
            } => {
                operand(lhs, absorbs(lhs, 3), s, context)?;
                if *not {
                    s.append(TK_NOT, None)?;
                }
                s.append(TK_IN, None)?;
                rhs.to_tokens(s, context)?;
                if !args.is_empty() {
                    s.append(TK_LP, None)?;
                    comma(args, s, context)?;
                    s.append(TK_RP, None)?;
                }
                Ok(())
            }
            Self::IsNull(sub_expr) => {
                operand(sub_expr, absorbs(sub_expr, 3), s, context)?;
                s.append(TK_ISNULL, None)
            }
            Self::Like {
                lhs,
                not,
                op,
                rhs,
                escape,
            } => {
                operand(lhs, absorbs(lhs, 3), s, context)?;
                if *not {
                    s.append(TK_NOT, None)?;
                }
                op.to_tokens(s, context)?;
                operand(rhs, leads_below(rhs, 4), s, context)?;
                if let Some(escape) = escape {
                    s.append(TK_ESCAPE, None)?;
                    operand(escape, leads_below(escape, 4), s, context)?;
                }
                Ok(())
            }
            Self::Literal(lit) => lit.to_tokens(s, context),
            Self::Name(name) => name.to_tokens(s, context),
            Self::NotNull(sub_expr) => {
                operand(sub_expr, absorbs(sub_expr, 3), s, context)?;
                s.append(TK_NOTNULL, None)
            }
            Self::Parenthesized(exprs) => {
                s.append(TK_LP, None)?;
                comma(exprs, s, context)?;
                s.append(TK_RP, None)
            }
            Self::Qualified(qualifier, qualified) => {
                qualifier.to_tokens(s, context)?;
                s.append(TK_DOT, None)?;
                qualified.to_tokens(s, context)
            }
            Self::FieldAccess { base, field, .. } => {
                base.to_tokens(s, context)?;
                s.append(TK_DOT, None)?;
                field.to_tokens(s, context)
            }
            Self::Raise(rt, err) => {
                s.append(TK_RAISE, None)?;
                s.append(TK_LP, None)?;
                rt.to_tokens(s, context)?;
                if let Some(err) = err {
                    s.append(TK_COMMA, None)?;
                    err.to_tokens(s, context)?;
                }
                s.append(TK_RP, None)
            }
            Self::RowId { .. } => Ok(()),
            Self::Subquery(query) => {
                s.append(TK_LP, None)?;
                query.to_tokens(s, context)?;
                s.append(TK_RP, None)
            }
            Self::Unary(op, sub_expr) => {
                op.to_tokens(s, context)?;
                operand(
                    sub_expr,
                    leads_below(sub_expr, unary_precedence(op)),
                    s,
                    context,
                )
            }
            Self::Variable(var) => {
                if let Some(name) = var.name.as_deref() {
                    return s.append(TK_VARIABLE, Some(name));
                }

                let indexed = format!("?{}", var.index.get());
                s.append(TK_VARIABLE, Some(indexed.as_str()))
            }
            Self::Default => s.append(TK_DEFAULT, None),
            Self::Array { elements } => {
                s.append(TK_ID, Some("ARRAY"))?;
                s.append(TK_LBRACKET, None)?;
                comma(elements, s, context)?;
                s.append(TK_RBRACKET, None)
            }
            Self::Subscript { base, index } => {
                operand(base, absorbs(base, 12), s, context)?;
                s.append(TK_LBRACKET, None)?;
                index.to_tokens(s, context)?;
                s.append(TK_RBRACKET, None)
            }
        }
    }
}

impl_display_for_to_tokens!(Literal);
impl ToTokens for Literal {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Numeric(ref num) => s.append(TK_FLOAT, Some(num)), // TODO Validate TK_FLOAT
            Self::String(ref str) => s.append(TK_STRING, Some(str)),
            Self::Blob(ref blob) => s.append(TK_BLOB, Some(blob)),
            Self::Keyword(ref str) => s.append(TK_ID, Some(str)), // TODO Validate TK_ID
            Self::Null => s.append(TK_NULL, None),
            Self::True => s.append(TK_ID, Some("TRUE")),
            Self::False => s.append(TK_ID, Some("FALSE")),
            Self::CurrentDate => s.append(TK_CTIME_KW, Some("CURRENT_DATE")),
            Self::CurrentTime => s.append(TK_CTIME_KW, Some("CURRENT_TIME")),
            Self::CurrentTimestamp => s.append(TK_CTIME_KW, Some("CURRENT_TIMESTAMP")),
        }
    }
}

impl_display_for_to_tokens!(LikeOperator);
impl ToTokens for LikeOperator {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(
            TK_LIKE_KW,
            Some(match self {
                Self::Glob => "GLOB",
                Self::Like => "LIKE",
                Self::Match => "MATCH",
                Self::Regexp => "REGEXP",
            }),
        )
    }
}

impl_display_for_to_tokens!(Operator);
impl ToTokens for Operator {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Add => s.append(TK_PLUS, None),
            Self::And => s.append(TK_AND, None),
            Self::ArrowRight => s.append(TK_PTR, Some("->")),
            Self::ArrowRightShift => s.append(TK_PTR, Some("->>")),
            Self::BitwiseAnd => s.append(TK_BITAND, None),
            Self::BitwiseOr => s.append(TK_BITOR, None),
            Self::BitwiseNot => s.append(TK_BITNOT, None),
            Self::Concat => s.append(TK_CONCAT, None),
            Self::Equals => s.append(TK_EQ, None),
            Self::Divide => s.append(TK_SLASH, None),
            Self::Greater => s.append(TK_GT, None),
            Self::GreaterEquals => s.append(TK_GE, None),
            Self::Is => s.append(TK_IS, None),
            Self::IsNot => {
                s.append(TK_IS, None)?;
                s.append(TK_NOT, None)
            }
            Self::LeftShift => s.append(TK_LSHIFT, None),
            Self::Less => s.append(TK_LT, None),
            Self::LessEquals => s.append(TK_LE, None),
            Self::Modulus => s.append(TK_REM, None),
            Self::Multiply => s.append(TK_STAR, None),
            Self::NotEquals => s.append(TK_NE, None),
            Self::Or => s.append(TK_OR, None),
            Self::RightShift => s.append(TK_RSHIFT, None),
            Self::Subtract => s.append(TK_MINUS, None),
            Self::ArrayContains => s.append(TK_ARRAY_CONTAINS, None),
            Self::ArrayOverlap => s.append(TK_ARRAY_OVERLAP, None),
        }
    }
}

impl_display_for_to_tokens!(UnaryOperator);
impl ToTokens for UnaryOperator {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(
            match self {
                Self::BitwiseNot => TK_BITNOT,
                Self::Negative => TK_MINUS,
                Self::Not => TK_NOT,
                Self::Positive => TK_PLUS,
            },
            None,
        )
    }
}

impl_display_for_to_tokens!(Select);
impl ToTokens for Select {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        if let Some(ref with) = self.with {
            with.to_tokens(s, context)?;
        }
        self.body.to_tokens(s, context)?;
        if !self.order_by.is_empty() {
            s.append(TK_ORDER, None)?;
            s.append(TK_BY, None)?;
            comma(&self.order_by, s, context)?;
        }
        if let Some(ref limit) = self.limit {
            limit.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(SelectBody);
impl ToTokens for SelectBody {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.select.to_tokens(s, context)?;
        for compound in &self.compounds {
            compound.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(CompoundSelect);
impl ToTokens for CompoundSelect {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.operator.to_tokens(s, context)?;
        self.select.to_tokens(s, context)
    }
}

impl_display_for_to_tokens!(CompoundOperator);
impl ToTokens for CompoundOperator {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Union => s.append(TK_UNION, None),
            Self::UnionAll => {
                s.append(TK_UNION, None)?;
                s.append(TK_ALL, None)
            }
            Self::Except => s.append(TK_EXCEPT, None),
            Self::Intersect => s.append(TK_INTERSECT, None),
        }
    }
}

impl_display_for_to_tokens!(OneSelect);
impl ToTokens for OneSelect {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Select {
                distinctness,
                columns,
                from,
                where_clause,
                group_by,
                window_clause,
            } => {
                s.append(TK_SELECT, None)?;
                if let Some(ref distinctness) = distinctness {
                    distinctness.to_tokens(s, context)?;
                }
                comma(columns, s, context)?;
                if let Some(ref from) = from {
                    s.append(TK_FROM, None)?;
                    from.to_tokens(s, context)?;
                }
                if let Some(ref where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s, context)?;
                }
                if let Some(ref group_by) = group_by {
                    group_by.to_tokens(s, context)?;
                }
                if !window_clause.is_empty() {
                    s.append(TK_WINDOW, None)?;
                    comma(window_clause, s, context)?;
                }
                Ok(())
            }
            Self::Values(values) => {
                for (i, vals) in values.iter().enumerate() {
                    if i == 0 {
                        s.append(TK_VALUES, None)?;
                    } else {
                        s.append(TK_COMMA, None)?;
                    }
                    s.append(TK_LP, None)?;
                    comma(vals, s, context)?;
                    s.append(TK_RP, None)?;
                }
                Ok(())
            }
        }
    }
}

impl_display_for_to_tokens!(FromClause);
impl ToTokens for FromClause {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.select.as_ref().to_tokens(s, context)?;
        for join in &self.joins {
            join.to_tokens(s, context)?;
        }

        Ok(())
    }
}

impl_display_for_to_tokens!(Distinctness);
impl ToTokens for Distinctness {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(
            match self {
                Self::Distinct => TK_DISTINCT,
                Self::All => TK_ALL,
            },
            None,
        )
    }
}

impl_display_for_to_tokens!(ResultColumn);
impl ToTokens for ResultColumn {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Expr(expr, alias) => {
                expr.to_tokens(s, context)?;
                if let Some(alias) = alias {
                    alias.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Star => s.append(TK_STAR, None),
            Self::TableStar(tbl_name) => {
                tbl_name.to_tokens(s, context)?;
                s.append(TK_DOT, None)?;
                s.append(TK_STAR, None)
            }
        }
    }
}

impl_display_for_to_tokens!(As);
impl ToTokens for As {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::As(ref name) => {
                s.append(TK_AS, None)?;
                name.to_tokens(s, context)
            }
            Self::Elided(ref name) => name.to_tokens(s, context),
            Self::ImplicitColumnName(_) => Ok(()),
        }
    }
}

impl_display_for_to_tokens!(JoinedSelectTable);
impl ToTokens for JoinedSelectTable {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.operator.to_tokens(s, context)?;
        self.table.to_tokens(s, context)?;
        if let Some(ref constraint) = self.constraint {
            constraint.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(SelectTable);
impl ToTokens for SelectTable {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Table(name, alias, indexed) => {
                name.to_tokens(s, context)?;
                if let Some(alias) = alias {
                    alias.to_tokens(s, context)?;
                }
                if let Some(indexed) = indexed {
                    indexed.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::TableCall(name, exprs, alias) => {
                name.to_tokens(s, context)?;
                s.append(TK_LP, None)?;
                comma(exprs, s, context)?;
                s.append(TK_RP, None)?;
                if let Some(alias) = alias {
                    alias.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Select(select, alias) => {
                s.append(TK_LP, None)?;
                select.to_tokens(s, context)?;
                s.append(TK_RP, None)?;
                if let Some(alias) = alias {
                    alias.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Sub(from, alias) => {
                s.append(TK_LP, None)?;
                from.to_tokens(s, context)?;
                s.append(TK_RP, None)?;
                if let Some(alias) = alias {
                    alias.to_tokens(s, context)?;
                }
                Ok(())
            }
        }
    }
}

impl_display_for_to_tokens!(JoinOperator);
impl ToTokens for JoinOperator {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Comma => s.append(TK_COMMA, None),
            Self::TypedJoin(join_type) => {
                if let Some(ref join_type) = join_type {
                    join_type.to_tokens(s, context)?;
                }
                s.append(TK_JOIN, None)
            }
        }
    }
}

impl_display_for_to_tokens!(JoinType);
impl ToTokens for JoinType {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        if self.contains(Self::NATURAL) {
            s.append(TK_JOIN_KW, Some("NATURAL"))?;
        }
        if self.contains(Self::INNER) {
            if self.contains(Self::CROSS) {
                s.append(TK_JOIN_KW, Some("CROSS"))?;
            }
            s.append(TK_JOIN_KW, Some("INNER"))?;
        } else {
            if self.contains(Self::LEFT) {
                if self.contains(Self::RIGHT) {
                    s.append(TK_JOIN_KW, Some("FULL"))?;
                } else {
                    s.append(TK_JOIN_KW, Some("LEFT"))?;
                }
            } else if self.contains(Self::RIGHT) {
                s.append(TK_JOIN_KW, Some("RIGHT"))?;
            }
            if self.contains(Self::OUTER) {
                s.append(TK_JOIN_KW, Some("OUTER"))?;
            }
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(JoinConstraint);
impl ToTokens for JoinConstraint {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::On(expr) => {
                s.append(TK_ON, None)?;
                expr.to_tokens(s, context)
            }
            Self::Using(col_names) => {
                s.append(TK_USING, None)?;
                s.append(TK_LP, None)?;
                comma(col_names, s, context)?;
                s.append(TK_RP, None)
            }
        }
    }
}

impl_display_for_to_tokens!(GroupBy);
impl ToTokens for GroupBy {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        if !self.exprs.is_empty() {
            s.append(TK_GROUP, None)?;
            s.append(TK_BY, None)?;
            comma(&self.exprs, s, context)?;
        }
        if let Some(ref having) = self.having {
            s.append(TK_HAVING, None)?;
            having.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(Name);
impl ToTokens for Name {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_ID, Some(&self.as_ident()))
    }
}

impl_display_for_to_tokens!(QualifiedName);
impl ToTokens for QualifiedName {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        if let Some(ref db_name) = self.db_name {
            db_name.to_tokens(s, context)?;
            s.append(TK_DOT, None)?;
        }
        self.name.to_tokens(s, context)?;
        if let Some(ref alias) = self.alias {
            s.append(TK_AS, None)?;
            alias.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(AlterTableBody);
impl ToTokens for AlterTableBody {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::RenameTo(name) => {
                s.append(TK_RENAME, None)?;
                s.append(TK_TO, None)?;
                name.to_tokens(s, context)
            }
            Self::AddColumn(def) => {
                s.append(TK_ADD, None)?;
                s.append(TK_COLUMNKW, None)?;
                def.to_tokens(s, context)
            }
            Self::AlterColumn { old, new } => {
                s.append(TK_ALTER, None)?;
                s.append(TK_COLUMNKW, None)?;
                old.to_tokens(s, context)?;
                s.append(TK_TO, None)?;
                new.to_tokens(s, context)
            }
            Self::RenameColumn { old, new } => {
                s.append(TK_RENAME, None)?;
                s.append(TK_COLUMNKW, None)?;
                old.to_tokens(s, context)?;
                s.append(TK_TO, None)?;
                new.to_tokens(s, context)
            }
            Self::DropColumn(name) => {
                s.append(TK_DROP, None)?;
                s.append(TK_COLUMNKW, None)?;
                name.to_tokens(s, context)
            }
        }
    }
}

impl_display_for_to_tokens!(CreateTableBody);
impl ToTokens for CreateTableBody {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::ColumnsAndConstraints {
                columns,
                constraints,
                options,
            } => {
                s.append(TK_LP, None)?;
                comma(columns, s, context)?;
                if !constraints.is_empty() {
                    s.append(TK_COMMA, None)?;
                    comma(constraints, s, context)?;
                }
                s.append(TK_RP, None)?;
                // Use the original text if available
                if let Some(ref without_rowid) = options.without_rowid_text {
                    // Split "WITHOUT ROWID" back into tokens
                    let parts: Vec<&str> = without_rowid.split_whitespace().collect();
                    if parts.len() == 2 {
                        s.append(TK_WITHOUT, None)?;
                        s.append(TK_ID, Some(parts[1]))?;
                    }
                }
                if let Some(ref strict) = options.strict_text {
                    s.append(TK_ID, Some(strict))?;
                }
                Ok(())
            }
            Self::AsSelect(select) => {
                s.append(TK_AS, None)?;
                select.to_tokens(s, context)
            }
        }
    }
}

impl_display_for_to_tokens!(ColumnDefinition);
impl ToTokens for ColumnDefinition {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.col_name.to_tokens(s, context)?;
        if let Some(ref col_type) = self.col_type {
            col_type.to_tokens(s, context)?;
        }
        for constraint in &self.constraints {
            constraint.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(NamedColumnConstraint);
impl ToTokens for NamedColumnConstraint {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        if let Some(ref name) = self.name {
            s.append(TK_CONSTRAINT, None)?;
            name.to_tokens(s, context)?;
        }
        self.constraint.to_tokens(s, context)
    }
}

impl_display_for_to_tokens!(ColumnConstraint);
impl ToTokens for ColumnConstraint {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::PrimaryKey {
                order,
                conflict_clause,
                auto_increment,
            } => {
                s.append(TK_PRIMARY, None)?;
                s.append(TK_KEY, None)?;
                if let Some(order) = order {
                    order.to_tokens(s, context)?;
                }
                if let Some(conflict_clause) = conflict_clause {
                    s.append(TK_ON, None)?;
                    s.append(TK_CONFLICT, None)?;
                    conflict_clause.to_tokens(s, context)?;
                }
                if *auto_increment {
                    s.append(TK_AUTOINCR, None)?;
                }
                Ok(())
            }
            Self::NotNull {
                nullable,
                conflict_clause,
            } => {
                if !nullable {
                    s.append(TK_NOT, None)?;
                }
                s.append(TK_NULL, None)?;
                if let Some(conflict_clause) = conflict_clause {
                    s.append(TK_ON, None)?;
                    s.append(TK_CONFLICT, None)?;
                    conflict_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Unique(conflict_clause) => {
                s.append(TK_UNIQUE, None)?;
                if let Some(conflict_clause) = conflict_clause {
                    s.append(TK_ON, None)?;
                    s.append(TK_CONFLICT, None)?;
                    conflict_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Check { expr, source } => {
                s.append(TK_CHECK, None)?;
                s.append(TK_LP, None)?;
                match source {
                    // Emit the expression exactly as the user wrote it so the
                    // spelling survives the canonical re-render (SQLite
                    // reports failed CHECKs with this text). Put the closing
                    // paren on a new line when a line comment might otherwise
                    // swallow it. Closed block comments are safe as-is.
                    Some(src) if !src.is_empty() => {
                        s.append(TK_ID, Some(src))?;
                        if src.contains("--") {
                            s.append(TK_ID, Some("\n"))?;
                        }
                    }
                    _ => expr.to_tokens(s, context)?,
                }
                s.append(TK_RP, None)
            }
            Self::Default(expr) => {
                s.append(TK_DEFAULT, None)?;
                // A translated DEFAULT that is not one of the grammar's bare forms prints in
                // parentheses, or the text does not parse back (REVIEW-engine-2bdcd32c6 HIGH 3).
                if expr.is_bare_default() {
                    expr.to_tokens(s, context)
                } else {
                    s.append(TK_LP, None)?;
                    expr.to_tokens(s, context)?;
                    s.append(TK_RP, None)
                }
            }
            Self::Collate { collation_name } => {
                s.append(TK_COLLATE, None)?;
                collation_name.to_tokens(s, context)
            }
            Self::ForeignKey {
                clause,
                defer_clause,
            } => {
                s.append(TK_REFERENCES, None)?;
                clause.to_tokens(s, context)?;
                if let Some(defer_clause) = defer_clause {
                    defer_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Generated { expr, typ } => {
                s.append(TK_AS, None)?;
                s.append(TK_LP, None)?;
                expr.to_tokens(s, context)?;
                s.append(TK_RP, None)?;
                if let Some(typ) = typ {
                    match typ {
                        GeneratedColumnType::Virtual => s.append(TK_VIRTUAL, None)?,
                        GeneratedColumnType::Stored => s.append(TK_ID, Some("STORED"))?,
                    }
                }
                Ok(())
            }
        }
    }
}

impl_display_for_to_tokens!(NamedTableConstraint);
impl ToTokens for NamedTableConstraint {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        if let Some(ref name) = self.name {
            s.append(TK_CONSTRAINT, None)?;
            name.to_tokens(s, context)?;
        }
        self.constraint.to_tokens(s, context)
    }
}

impl_display_for_to_tokens!(TableConstraint);
impl ToTokens for TableConstraint {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::PrimaryKey {
                columns,
                auto_increment,
                conflict_clause,
            } => {
                s.append(TK_PRIMARY, None)?;
                s.append(TK_KEY, None)?;
                s.append(TK_LP, None)?;
                comma(columns, s, context)?;
                if *auto_increment {
                    s.append(TK_AUTOINCR, None)?;
                }
                s.append(TK_RP, None)?;
                if let Some(conflict_clause) = conflict_clause {
                    s.append(TK_ON, None)?;
                    s.append(TK_CONFLICT, None)?;
                    conflict_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Unique {
                columns,
                conflict_clause,
            } => {
                s.append(TK_UNIQUE, None)?;
                s.append(TK_LP, None)?;
                comma(columns, s, context)?;
                s.append(TK_RP, None)?;
                if let Some(conflict_clause) = conflict_clause {
                    s.append(TK_ON, None)?;
                    s.append(TK_CONFLICT, None)?;
                    conflict_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Check { expr, source } => {
                s.append(TK_CHECK, None)?;
                s.append(TK_LP, None)?;
                match source {
                    // Emit the expression exactly as the user wrote it so the
                    // spelling survives the canonical re-render (SQLite
                    // reports failed CHECKs with this text). Put the closing
                    // paren on a new line when a line comment might otherwise
                    // swallow it. Closed block comments are safe as-is.
                    Some(src) if !src.is_empty() => {
                        s.append(TK_ID, Some(src))?;
                        if src.contains("--") {
                            s.append(TK_ID, Some("\n"))?;
                        }
                    }
                    _ => expr.to_tokens(s, context)?,
                }
                s.append(TK_RP, None)
            }
            Self::ForeignKey {
                columns,
                clause,
                defer_clause,
            } => {
                s.append(TK_FOREIGN, None)?;
                s.append(TK_KEY, None)?;
                s.append(TK_LP, None)?;
                comma(columns, s, context)?;
                s.append(TK_RP, None)?;
                s.append(TK_REFERENCES, None)?;
                clause.to_tokens(s, context)?;
                if let Some(defer_clause) = defer_clause {
                    defer_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
        }
    }
}

impl_display_for_to_tokens!(SortOrder);
impl ToTokens for SortOrder {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(
            match self {
                Self::Asc => TK_ASC,
                Self::Desc => TK_DESC,
            },
            None,
        )
    }
}

impl_display_for_to_tokens!(NullsOrder);
impl ToTokens for NullsOrder {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_NULLS, None)?;
        s.append(
            match self {
                Self::First => TK_FIRST,
                Self::Last => TK_LAST,
            },
            None,
        )
    }
}

impl_display_for_to_tokens!(ForeignKeyClause);
impl ToTokens for ForeignKeyClause {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.tbl_name.to_tokens(s, context)?;
        if !self.columns.is_empty() {
            s.append(TK_LP, None)?;
            comma(&self.columns, s, context)?;
            s.append(TK_RP, None)?;
        }
        for arg in &self.args {
            arg.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(RefArg);
impl ToTokens for RefArg {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::OnDelete(ref action) => {
                s.append(TK_ON, None)?;
                s.append(TK_DELETE, None)?;
                action.to_tokens(s, context)
            }
            Self::OnInsert(ref action) => {
                s.append(TK_ON, None)?;
                s.append(TK_INSERT, None)?;
                action.to_tokens(s, context)
            }
            Self::OnUpdate(ref action) => {
                s.append(TK_ON, None)?;
                s.append(TK_UPDATE, None)?;
                action.to_tokens(s, context)
            }
            Self::Match(ref name) => {
                s.append(TK_MATCH, None)?;
                name.to_tokens(s, context)
            }
        }
    }
}

impl_display_for_to_tokens!(RefAct);
impl ToTokens for RefAct {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::SetNull => {
                s.append(TK_SET, None)?;
                s.append(TK_NULL, None)
            }
            Self::SetDefault => {
                s.append(TK_SET, None)?;
                s.append(TK_DEFAULT, None)
            }
            Self::Cascade => s.append(TK_CASCADE, None),
            Self::Restrict => s.append(TK_RESTRICT, None),
            Self::NoAction => {
                s.append(TK_NO, None)?;
                s.append(TK_ACTION, None)
            }
        }
    }
}

impl_display_for_to_tokens!(DeferSubclause);
impl ToTokens for DeferSubclause {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        if !self.deferrable {
            s.append(TK_NOT, None)?;
        }
        s.append(TK_DEFERRABLE, None)?;
        if let Some(init_deferred) = self.init_deferred {
            init_deferred.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(InitDeferredPred);
impl ToTokens for InitDeferredPred {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_INITIALLY, None)?;
        s.append(
            match self {
                Self::InitiallyDeferred => TK_DEFERRED,
                Self::InitiallyImmediate => TK_IMMEDIATE,
            },
            None,
        )
    }
}

impl_display_for_to_tokens!(IndexedColumn);
impl ToTokens for IndexedColumn {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.col_name.to_tokens(s, context)?;
        if let Some(ref collation_name) = self.collation_name {
            s.append(TK_COLLATE, None)?;
            collation_name.to_tokens(s, context)?;
        }
        if let Some(order) = self.order {
            order.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(Indexed);
impl ToTokens for Indexed {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::IndexedBy(ref name) => {
                s.append(TK_INDEXED, None)?;
                s.append(TK_BY, None)?;
                name.to_tokens(s, context)
            }
            Self::NotIndexed => {
                s.append(TK_NOT, None)?;
                s.append(TK_INDEXED, None)
            }
        }
    }
}

impl_display_for_to_tokens!(SortedColumn);
impl ToTokens for SortedColumn {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.expr.to_tokens(s, context)?;
        if let Some(ref order) = self.order {
            order.to_tokens(s, context)?;
        }
        if let Some(ref nulls) = self.nulls {
            nulls.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(Limit);
impl ToTokens for Limit {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_LIMIT, None)?;
        self.expr.to_tokens(s, context)?;
        if let Some(ref offset) = self.offset {
            s.append(TK_OFFSET, None)?;
            offset.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(InsertBody);
impl ToTokens for InsertBody {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Select(select, upsert) => {
                select.to_tokens(s, context)?;
                if let Some(upsert) = upsert {
                    upsert.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::DefaultValues => {
                s.append(TK_DEFAULT, None)?;
                s.append(TK_VALUES, None)
            }
        }
    }
}

impl_display_for_to_tokens!(Set);
impl ToTokens for Set {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        if self.col_names.len() == 1 {
            comma(&self.col_names, s, context)?;
        } else {
            s.append(TK_LP, None)?;
            comma(&self.col_names, s, context)?;
            s.append(TK_RP, None)?;
        }
        s.append(TK_EQ, None)?;
        self.expr.to_tokens(s, context)
    }
}

impl_display_for_to_tokens!(PragmaBody);
impl ToTokens for PragmaBody {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Equals(value) => {
                s.append(TK_EQ, None)?;
                value.to_tokens(s, context)
            }
            Self::Call(value) => {
                s.append(TK_LP, None)?;
                value.to_tokens(s, context)?;
                s.append(TK_RP, None)
            }
        }
    }
}

impl_display_for_to_tokens!(TriggerTime);
impl ToTokens for TriggerTime {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Before => s.append(TK_BEFORE, None),
            Self::After => s.append(TK_AFTER, None),
            Self::InsteadOf => {
                s.append(TK_INSTEAD, None)?;
                s.append(TK_OF, None)
            }
        }
    }
}

impl_display_for_to_tokens!(TriggerEvent);
impl ToTokens for TriggerEvent {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Delete => s.append(TK_DELETE, None),
            Self::Insert => s.append(TK_INSERT, None),
            Self::Update => s.append(TK_UPDATE, None),
            Self::UpdateOf(ref col_names) => {
                s.append(TK_UPDATE, None)?;
                s.append(TK_OF, None)?;
                comma(col_names, s, context)
            }
        }
    }
}

impl_display_for_to_tokens!(TriggerCmd);
impl ToTokens for TriggerCmd {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Update {
                or_conflict,
                tbl_name,
                sets,
                from,
                where_clause,
            } => {
                s.append(TK_UPDATE, None)?;
                if let Some(or_conflict) = or_conflict {
                    s.append(TK_OR, None)?;
                    or_conflict.to_tokens(s, context)?;
                }
                tbl_name.to_tokens(s, context)?;
                s.append(TK_SET, None)?;
                comma(sets, s, context)?;
                if let Some(from) = from {
                    s.append(TK_FROM, None)?;
                    from.to_tokens(s, context)?;
                }
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Insert {
                or_conflict,
                tbl_name,
                col_names,
                select,
                upsert,
                returning,
            } => {
                if let Some(ResolveType::Replace) = or_conflict {
                    s.append(TK_REPLACE, None)?;
                } else {
                    s.append(TK_INSERT, None)?;
                    if let Some(or_conflict) = or_conflict {
                        s.append(TK_OR, None)?;
                        or_conflict.to_tokens(s, context)?;
                    }
                }
                s.append(TK_INTO, None)?;
                tbl_name.to_tokens(s, context)?;
                if !col_names.is_empty() {
                    s.append(TK_LP, None)?;
                    comma(col_names, s, context)?;
                    s.append(TK_RP, None)?;
                }
                select.to_tokens(s, context)?;
                if let Some(upsert) = upsert {
                    upsert.to_tokens(s, context)?;
                }
                if !returning.is_empty() {
                    s.append(TK_RETURNING, None)?;
                    comma(returning, s, context)?;
                }
                Ok(())
            }
            Self::Delete {
                tbl_name,
                where_clause,
            } => {
                s.append(TK_DELETE, None)?;
                s.append(TK_FROM, None)?;
                tbl_name.to_tokens(s, context)?;
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Select(select) => select.to_tokens(s, context),
        }
    }
}

impl_display_for_to_tokens!(ResolveType);
impl ToTokens for ResolveType {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(
            match self {
                Self::Rollback => TK_ROLLBACK,
                Self::Abort => TK_ABORT,
                Self::Fail => TK_FAIL,
                Self::Ignore => TK_IGNORE,
                Self::Replace => TK_REPLACE,
            },
            None,
        )
    }
}

impl_display_for_to_tokens!(With);
impl ToTokens for With {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_WITH, None)?;
        if self.recursive {
            s.append(TK_RECURSIVE, None)?;
        }
        comma(&self.ctes, s, context)
    }
}

impl_display_for_to_tokens!(CommonTableExpr);
impl ToTokens for CommonTableExpr {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.tbl_name.to_tokens(s, context)?;
        if !self.columns.is_empty() {
            s.append(TK_LP, None)?;
            comma(&self.columns, s, context)?;
            s.append(TK_RP, None)?;
        }
        s.append(TK_AS, None)?;
        match self.materialized {
            Materialized::Any => {}
            Materialized::Yes => {
                s.append(TK_MATERIALIZED, None)?;
            }
            Materialized::No => {
                s.append(TK_NOT, None)?;
                s.append(TK_MATERIALIZED, None)?;
            }
        };
        s.append(TK_LP, None)?;
        self.select.to_tokens(s, context)?;
        s.append(TK_RP, None)
    }
}

/// Whether a type name printed bare reads back as itself: one or more words, each separated from the
/// next by exactly one space, and each either a plain type word (`crate::lexer::is_plain_type_word`)
/// or one whole well-formed quoted token (`"..."`, `'...'` or `` `...` ``, its own quote doubled
/// inside, so `"x""y"`). The parser keeps such a token whole and joins the words with one space, so
/// the same name comes back. `"a"b"` is not one: its quoted token `"a"` runs straight into `b"`.
fn type_name_reads_back_bare(b: &[u8]) -> bool {
    type_name_words(b).is_some_and(|words| {
        words.iter().all(|w| match w {
            TypeWord::Quoted(_) => true,
            TypeWord::Raw(t) => crate::lexer::is_plain_type_word(t),
        })
    })
}

/// One word of a type name, as [`type_name_words`] splits it.
enum TypeWord<'a> {
    /// One whole well-formed quoted token, quotes included; it may hold spaces.
    Quoted(&'a [u8]),
    /// Anything else, up to the next space.
    Raw(&'a [u8]),
}

/// Splits a type name into the words the parser joined with one space each. A word that starts with
/// a quote and is one whole well-formed quoted token (its own quote doubled inside, ending at a space
/// or at the end) is `Quoted`; any other word runs to the next space and is `Raw`. None for an empty
/// name or an empty word (two spaces in a row, or a leading or trailing space).
fn type_name_words(b: &[u8]) -> Option<Vec<TypeWord<'_>>> {
    if b.is_empty() {
        return None;
    }
    let mut words = Vec::new();
    let mut i = 0;
    loop {
        let start = i;
        if let Some(end) = quoted_type_word_end(b, start) {
            words.push(TypeWord::Quoted(&b[start..end]));
            i = end;
        } else {
            while i < b.len() && b[i] != b' ' {
                i += 1;
            }
            if i == start {
                return None;
            }
            words.push(TypeWord::Raw(&b[start..i]));
        }
        if i == b.len() {
            return Some(words);
        }
        i += 1; // the one space between words
        if i == b.len() {
            return None;
        }
    }
}

/// The end of the well-formed quoted token starting at `start`, if one starts there and ends at a
/// space or at the end of `b`.
fn quoted_type_word_end(b: &[u8], start: usize) -> Option<usize> {
    let q = *b.get(start)?;
    if !matches!(q, b'"' | b'\'' | b'`') {
        return None;
    }
    let mut i = start + 1;
    while i < b.len() {
        count_piece_step();
        if b[i] == q {
            if b.get(i + 1) == Some(&q) {
                i += 2;
                continue;
            }
            let end = i + 1;
            return (end == b.len() || b[end] == b' ').then_some(end);
        }
        i += 1;
    }
    None // unterminated
}

/// A type name holding a `]`, printed word by word as a28c82f07 printed it: a quoted word or a plain
/// one as itself, any other word bracket-quoted. It misreads a bracket's content that holds spaces
/// or quotes, so only mutant `type_pieces_cut_at_spaces` uses it; [`type_name_by_pieces`] prints.
fn type_name_by_words(b: &[u8]) -> Option<String> {
    let mut out = String::new();
    for (k, w) in type_name_words(b)?.iter().enumerate() {
        if k > 0 {
            out.push(' ');
        }
        match w {
            TypeWord::Quoted(t) => out.push_str(std::str::from_utf8(t).ok()?),
            TypeWord::Raw(t) if crate::lexer::is_plain_type_word(t) => {
                out.push_str(std::str::from_utf8(t).ok()?)
            }
            TypeWord::Raw(t) if !t.contains(&b']') => {
                out.push('[');
                out.push_str(std::str::from_utf8(t).ok()?);
                out.push(']');
            }
            TypeWord::Raw(_) => return None,
        }
    }
    Some(out)
}

/// For quote `q`: `ends[j]` is the index just past the closing `q` of a quoted token whose body is
/// read from `j` on (a doubled `qq` stays inside it), or None when it is never closed; so a token
/// opening at `s` ends at `ends[s + 1]`, as [`quoted_type_word_end`] scans it. One right-to-left
/// pass; `ends` has `b.len() + 2` entries.
fn quote_closing_ends(b: &[u8], q: u8) -> Vec<Option<usize>> {
    let n = b.len();
    let mut ends = vec![None; n + 2];
    for j in (0..n).rev() {
        count_piece_step();
        ends[j] = if b[j] != q {
            ends[j + 1]
        } else if b.get(j + 1) == Some(&q) {
            ends[j + 2]
        } else {
            Some(j + 1)
        };
    }
    ends
}

// Test builds: the scan steps `type_name_by_pieces` takes on this thread, so a test can bound its
// cost by counting at the layer that pays rather than by timing.
#[cfg(test)]
thread_local! {
    pub(crate) static PIECE_STEPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[inline]
fn count_piece_step() {
    #[cfg(test)]
    PIECE_STEPS.with(|c| c.set(c.get() + 1));
}

/// A printing of a type name holding a `]` that reads back as itself: the name cut into pieces joined
/// by one space each, every piece either one whole well-formed quoted token (printed as itself) or a
/// non-empty run holding no `]` (printed bare when it is a plain type word, else bracket-quoted with
/// its spaces and quotes). A bracket's content may hold spaces and quotes, so the cut points are
/// searched for, not taken at the spaces (engine-rev 2bdcd32c6 HIGH 1: `a  "]"` from `[a ] "]"`, and
/// `"x " ]"` from `["x] " ]"`, have no cut at the spaces alone). The parser's own tokens are always
/// such a cut, so one exists for every parsed name; the result is checked by reading it back.
fn type_name_by_pieces(name: &str) -> Option<String> {
    let b = name.as_bytes();
    let n = b.len();
    // back[e] = (s, quoted): the piece b[s..e] ends a cut of b[..e] that starts at 0.
    let mut back: Vec<Option<(usize, bool)>> = vec![None; n + 1];
    // Linear in the name (engine-rev 2bdcd32c6 HIGH 1, the third attack: scanning from every start
    // was quadratic, about 10^9 steps for a 100 KB name). A quoted piece's end comes from one
    // right-to-left pass per quote kind, and a run's scan is skipped for a start inside a run an
    // earlier start already scanned: its ends are a subset of that scan's, all already marked (and
    // `get_or_insert` keeps the earlier mark). Mutant `type_pieces_quadratic_scan` (test builds of
    // this crate only): every start scans again.
    let quadratic = fe_mutant("type_pieces_quadratic_scan");
    let closes = [b'"', b'\'', b'`'].map(|q| quote_closing_ends(b, q));
    let mut scanned_to = 0;
    for s in 0..n {
        count_piece_step();
        if s > 0 && !(b[s - 1] == b' ' && back[s - 1].is_some()) {
            continue;
        }
        let kind = match b[s] {
            b'"' => Some(0),
            b'\'' => Some(1),
            b'`' => Some(2),
            _ => None,
        };
        if let Some(e) = kind.and_then(|k| closes[k][s + 1]) {
            if e == n || b[e] == b' ' {
                back[e].get_or_insert((s, true));
            }
        }
        if s < scanned_to && !quadratic {
            continue;
        }
        let mut e = s;
        while e < n && b[e] != b']' {
            count_piece_step();
            e += 1;
            if e == n || b[e] == b' ' {
                back[e].get_or_insert((s, false));
            }
        }
        scanned_to = e;
    }
    let mut pieces = Vec::new();
    let mut e = n;
    loop {
        let (s, quoted) = back[e]?;
        let t = std::str::from_utf8(&b[s..e]).ok()?;
        pieces.push(if quoted || crate::lexer::is_plain_type_word(t.as_bytes()) {
            t.to_string()
        } else {
            format!("[{t}]")
        });
        if s == 0 {
            break;
        }
        e = s - 1;
    }
    pieces.reverse();
    let out = pieces.join(" ");
    (crate::parser::read_back_type_name(&out).as_deref() == Some(name)).then_some(out)
}

/// The parser's registered mutants (`FE_MUTANT`, the convention of turso_core's
/// `branch::store::fe_mutant`, which this crate cannot call): one names one deliberate defect, so a
/// red can be shown to fail on it from the same test binary. TEST BUILDS OF THIS CRATE ONLY: any
/// other build, turso_core's tests included, has no mutant to switch on. Read once per process.
fn fe_mutant(name: &str) -> bool {
    #[cfg(test)]
    {
        static ON: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
        ON.get_or_init(|| std::env::var("FE_MUTANT").ok()).as_deref() == Some(name)
    }
    #[cfg(not(test))]
    {
        let _ = name;
        false
    }
}

impl_display_for_to_tokens!(Type);
impl ToTokens for Type {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        // The name prints so that it reads back as itself (engine-rev-qaf-fix HIGH-4): bare when every
        // word is a plain type word, else bracket-quoted as written (`[x,y]`, `[not null]`), which the
        // parser reads back as the bracket's bytes; a name holding `]` prints piece by piece
        // (`type_name_by_pieces`). Only a name no parse produces (one with no such cut) is left to
        // the whole-name double quote, which does NOT read back as itself (the parser keeps a
        // quoted type token's quotes).
        // A word may also be a quoted token the parser kept whole, quotes and all (`"a]b"`, `'x]y'`):
        // it prints as itself, since quoting it again made every rewrite grow it (engine-rev-qaf-fix2
        // HIGH A: 52, 64, 84, ... 568 bytes). Only a WHOLE well-formed token counts: a bracket's
        // `"a"b"` merely starts and ends with a quote, and printed bare it is an unterminated literal
        // (engine-rev 2bdcd32c6 HIGH 1). Mutant `type_self_quoted_by_ends` (test builds only): the
        // first-and-last-byte check of 0e823a4ac.
        let bare = if fe_mutant("type_self_quoted_by_ends") {
            let b = self.name.as_bytes();
            let plain = !b.is_empty()
                && self.name.split(' ').all(|w| crate::lexer::is_plain_type_word(w.as_bytes()));
            plain
                || (b.len() >= 2 && matches!(b[0], b'"' | b'\'' | b'`') && b[b.len() - 1] == b[0])
        } else {
            type_name_reads_back_bare(self.name.as_bytes())
        };
        if bare {
            s.append(TK_ID, Some(&self.name))?;
        } else if !self.name.contains(']') {
            s.append(TK_ID, Some(&format!("[{}]", self.name)))?;
        } else if let Some(by_pieces) = if fe_mutant("type_bracket_name_double_quoted") {
            None
        } else if fe_mutant("type_pieces_cut_at_spaces") {
            type_name_by_words(self.name.as_bytes())
        } else {
            type_name_by_pieces(&self.name)
        } {
            // A `]` sits inside a quoted word (`"a]b" [x,y]`): piece by piece, each read back as
            // written. The whole-name double quote below keeps its quotes when read back, so it grew
            // the name on every rewrite (engine-rev 2bdcd32c6 HIGH 1, the attack on b6e14769f).
            // Mutants (test builds of this crate only): `type_bracket_name_double_quoted`, straight to
            // it; `type_pieces_cut_at_spaces`, a28c82f07's cut at every space.
            s.append(TK_ID, Some(&by_pieces))?;
        } else {
            s.append(TK_ID, Some(&format!("\"{}\"", self.name.replace('"', "\"\""))))?;
        }
        if let Some(ref size) = self.size {
            s.append(TK_LP, None)?;
            size.to_tokens(s, context)?;
            s.append(TK_RP, None)?;
        }
        for _ in 0..self.array_dimensions {
            s.append(TK_LBRACKET, None)?;
            s.append(TK_RBRACKET, None)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(TypeSize);
impl ToTokens for TypeSize {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::MaxSize(size) => size.to_tokens(s, context),
            Self::TypeSize(size1, size2) => {
                size1.to_tokens(s, context)?;
                s.append(TK_COMMA, None)?;
                size2.to_tokens(s, context)
            }
        }
    }
}

impl_display_for_to_tokens!(TransactionType);
impl ToTokens for TransactionType {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(
            match self {
                Self::Deferred => TK_DEFERRED,
                Self::Immediate => TK_IMMEDIATE,
                Self::Exclusive => TK_EXCLUSIVE,
                Self::Concurrent => TK_CONCURRENT,
            },
            None,
        )
    }
}

impl_display_for_to_tokens!(Upsert);
impl ToTokens for Upsert {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_ON, None)?;
        s.append(TK_CONFLICT, None)?;
        if let Some(ref index) = self.index {
            index.to_tokens(s, context)?;
        }
        self.do_clause.to_tokens(s, context)?;
        if let Some(ref next) = self.next {
            next.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(UpsertIndex);
impl ToTokens for UpsertIndex {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_LP, None)?;
        comma(&self.targets, s, context)?;
        s.append(TK_RP, None)?;
        if let Some(ref where_clause) = self.where_clause {
            s.append(TK_WHERE, None)?;
            where_clause.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(UpsertDo);
impl ToTokens for UpsertDo {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Set { sets, where_clause } => {
                s.append(TK_DO, None)?;
                s.append(TK_UPDATE, None)?;
                s.append(TK_SET, None)?;
                comma(sets, s, context)?;
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Nothing => {
                s.append(TK_DO, None)?;
                s.append(TK_NOTHING, None)
            }
        }
    }
}

impl_display_for_to_tokens!(FunctionTail);
impl ToTokens for FunctionTail {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        if let Some(ref filter_clause) = self.filter_clause {
            s.append(TK_FILTER, None)?;
            s.append(TK_LP, None)?;
            s.append(TK_WHERE, None)?;
            filter_clause.to_tokens(s, context)?;
            s.append(TK_RP, None)?;
        }
        if let Some(ref over_clause) = self.over_clause {
            s.append(TK_OVER, None)?;
            over_clause.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(Over);
impl ToTokens for Over {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Window(ref window) => window.to_tokens(s, context),
            Self::Name(ref name) => name.to_tokens(s, context),
        }
    }
}

impl_display_for_to_tokens!(WindowDef);
impl ToTokens for WindowDef {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.name.to_tokens(s, context)?;
        s.append(TK_AS, None)?;
        self.window.to_tokens(s, context)
    }
}

impl_display_for_to_tokens!(Window);
impl ToTokens for Window {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_LP, None)?;
        if let Some(ref base) = self.base {
            base.to_tokens(s, context)?;
        }
        if !self.partition_by.is_empty() {
            s.append(TK_PARTITION, None)?;
            s.append(TK_BY, None)?;
            comma(&self.partition_by, s, context)?;
        }
        if !self.order_by.is_empty() {
            s.append(TK_ORDER, None)?;
            s.append(TK_BY, None)?;
            comma(&self.order_by, s, context)?;
        }
        if let Some(ref frame_clause) = self.frame_clause {
            frame_clause.to_tokens(s, context)?;
        }
        s.append(TK_RP, None)
    }
}

impl_display_for_to_tokens!(FrameClause);
impl ToTokens for FrameClause {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.mode.to_tokens(s, context)?;
        if let Some(ref end) = self.end {
            s.append(TK_BETWEEN, None)?;
            self.start.to_tokens(s, context)?;
            s.append(TK_AND, None)?;
            end.to_tokens(s, context)?;
        } else {
            self.start.to_tokens(s, context)?;
        }
        if let Some(ref exclude) = self.exclude {
            s.append(TK_EXCLUDE, None)?;
            exclude.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(FrameMode);
impl ToTokens for FrameMode {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(
            match self {
                Self::Groups => TK_GROUPS,
                Self::Range => TK_RANGE,
                Self::Rows => TK_ROWS,
            },
            None,
        )
    }
}

impl_display_for_to_tokens!(FrameBound);
impl ToTokens for FrameBound {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::CurrentRow => {
                s.append(TK_CURRENT, None)?;
                s.append(TK_ROW, None)
            }
            Self::Following(value) => {
                value.to_tokens(s, context)?;
                s.append(TK_FOLLOWING, None)
            }
            Self::Preceding(value) => {
                value.to_tokens(s, context)?;
                s.append(TK_PRECEDING, None)
            }
            Self::UnboundedFollowing => {
                s.append(TK_UNBOUNDED, None)?;
                s.append(TK_FOLLOWING, None)
            }
            Self::UnboundedPreceding => {
                s.append(TK_UNBOUNDED, None)?;
                s.append(TK_PRECEDING, None)
            }
        }
    }
}

impl_display_for_to_tokens!(FrameExclude);
impl ToTokens for FrameExclude {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::NoOthers => {
                s.append(TK_NO, None)?;
                s.append(TK_OTHERS, None)
            }
            Self::CurrentRow => {
                s.append(TK_CURRENT, None)?;
                s.append(TK_ROW, None)
            }
            Self::Group => s.append(TK_GROUP, None),
            Self::Ties => s.append(TK_TIES, None),
        }
    }
}

/// Emit a parenthesized, comma-separated list of `name Type` pairs.
fn type_fields_to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
    fields: &[TypeField],
    s: &mut S,
    context: &C,
) -> Result<(), S::Error> {
    s.append(TK_LP, None)?;
    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            s.append(TK_COMMA, None)?;
        }
        field.name.to_tokens(s, context)?;
        field.field_type.to_tokens(s, context)?;
    }
    s.append(TK_RP, None)
}

fn comma<I, S: TokenStream + ?Sized, C: ToSqlContext>(
    items: I,
    s: &mut S,
    context: &C,
) -> Result<(), S::Error>
where
    I: IntoIterator,
    I::Item: ToTokens,
{
    s.comma(items, context)
}
