//! THE table from a condition to the SQLSTATE PostgreSQL sends for it: every code this frontend and the
//! wire server (turso_pg_server) put on the wire, in an error, a notice or a row, comes from here
//! (C44-M3; review c39799c5d..9e24b455d MED 1 found two notice codes and pg_input_error_info's
//! codes written as literals in this crate, outside the server's table). The server's guard test
//! every_sqlstate_the_server_sends_comes_from_the_table scans both crates' product sources for any
//! other five-character SQLSTATE literal.

use turso_parser::sql_error::SqlErrorKind;

/// The SQLSTATE of `kind`.
pub fn sqlstate(kind: SqlErrorKind) -> &'static str {
    use SqlErrorKind::*;
    match kind {
        SyntaxError => "42601",
        FeatureNotSupported => "0A000",
        InvalidTextRepresentation => "22P02",
        NumericValueOutOfRange => "22003",
        DivisionByZero => "22012",
        IntegrityConstraintViolation => "23000",
        UniqueViolation => "23505",
        NotNullViolation => "23502",
        CheckViolation => "23514",
        ForeignKeyViolation => "23503",
        InvalidColumnReference => "42P10",
        DatetimeFieldOverflow => "22008",
        InvalidParameterValue => "22023",
        UndefinedTable => "42P01",
        InvalidForeignKey => "42830",
        UndefinedColumn => "42703",
        UndefinedObject => "42704",
        UndefinedParameter => "42P02",
        InvalidSavepointSpecification => "3B001",
        AmbiguousColumn => "42702",
        UndefinedFunction => "42883",
        DuplicateAlias => "42712",
        StatementTooComplex => "54001",
        InternalError => "XX000",
        SerializationFailure => "40001",
        LockNotAvailable => "55P03",
        ReadOnlyTransaction => "25006",
        DiskFull => "53100",
        QueryCanceled => "57014",
        ConnectionFailure => "08006",
        ProtocolViolation => "08P01",
        NullValueNotAllowed => "22004",
        CharacterNotInRepertoire => "22021",
        InvalidBinaryRepresentation => "22P03",
        ActiveSqlTransaction => "25001",
        NoActiveSqlTransaction => "25P01",
        InFailedSqlTransaction => "25P02",
        InvalidCursorName => "34000",
        InvalidCatalogName => "3D000",
        DatatypeMismatch => "42804",
        ReservedName => "42939",
        DuplicateCursor => "42P03",
        DuplicateDatabase => "42P04",
        IndeterminateDatatype => "42P18",
        ObjectNotInPrerequisiteState => "55000",
        ObjectInUse => "55006",
        InsufficientPrivilege => "42501",
        InvalidName => "42602",
        InvalidSchemaName => "3F000",
        DependentObjectsStillExist => "2BP01",
        InvalidTableDefinition => "42P16",
        TooManyConnections => "53300",
        Warning => "01000",
        SuccessfulCompletion => "00000",
        StringDataRightTruncation => "22001",
        InvalidDatetimeFormat => "22007",
        BadCopyFileFormat => "22P04",
    }
}
