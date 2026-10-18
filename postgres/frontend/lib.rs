mod aliases;
mod catalog;
mod copy;
mod functions;
mod information_schema;
mod pg_settings;
pub mod phase;
mod result_types;
pub use result_types::{element_of, StatementTypes};
mod session;
mod sqlstate;
pub use sqlstate::sqlstate;

pub use session::PgConnection as Connection;
pub use session::{
    attach_schema_files, branch_call, check_table_keys, is_commit, message_rank, open_database,
    open_database_with_io, pg_bool, read_parse_message, single_statement, split_statements,
    ParseMessage, PgConnection, PgQueryRunner, ServerFiles, TxEvent, TxStmt,
    CONNECTION_BROKEN, DEFAULT_SEARCH_PATH, FIXED_SETTINGS,
};
#[cfg(feature = "failpoints")]
pub use session::fail_next_undo;
#[cfg(feature = "failpoints")]
pub use session::pretend_deferred_violations;
pub use turso_core::{
    Database, DatabaseOpts, Func, LimboError, Numeric, OpenFlags, PlatformIO, Result, StepResult,
};
pub use turso_pg_parser::translator::{PgBranchArg, PgBranchCall};
/// A value as PostgreSQL string-literal text, the one quoting function (for the CLI's catalog
/// queries; wire-rev-uw2 N6).
pub use turso_pg_parser::translator::sql_quoted;

pub mod vtab {
    pub use turso_core::VirtualTable;
}
