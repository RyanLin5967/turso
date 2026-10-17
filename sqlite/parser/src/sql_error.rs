//! Diagnostic metadata shared by the SQL parser, compiler and execution engine.
//!
//! A producer chooses the condition when it creates the error. Changing the human-readable
//! message cannot change that condition. A frontend supplies its protocol's code mapping.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlErrorKind {
    SyntaxError,
    FeatureNotSupported,
    InvalidTextRepresentation,
    NumericValueOutOfRange,
    DivisionByZero,
    IntegrityConstraintViolation,
    UniqueViolation,
    NotNullViolation,
    CheckViolation,
    ForeignKeyViolation,
    InvalidColumnReference,
    DatetimeFieldOverflow,
    InvalidParameterValue,
    UndefinedTable,
    InvalidForeignKey,
    UndefinedColumn,
    UndefinedObject,
    UndefinedParameter,
    InvalidSavepointSpecification,
    AmbiguousColumn,
    UndefinedFunction,
    DuplicateAlias,
    StatementTooComplex,
    InternalError,
    SerializationFailure,
    LockNotAvailable,
    ReadOnlyTransaction,
    DiskFull,
    QueryCanceled,
    // The wire server's own conditions (C44-M3: every SQLSTATE it sends comes from one table).
    ConnectionFailure,
    ProtocolViolation,
    NullValueNotAllowed,
    CharacterNotInRepertoire,
    InvalidBinaryRepresentation,
    ActiveSqlTransaction,
    NoActiveSqlTransaction,
    InFailedSqlTransaction,
    InvalidCursorName,
    InvalidCatalogName,
    DatatypeMismatch,
    ReservedName,
    DuplicateCursor,
    DuplicateDatabase,
    IndeterminateDatatype,
    ObjectNotInPrerequisiteState,
    ObjectInUse,
    TooManyConnections,
    Warning,
    SuccessfulCompletion,
    // pg_input_error_info's conditions (review c39799c5d..9e24b455d MED 1).
    StringDataRightTruncation,
    InvalidDatetimeFormat,
}

/// A diagnostic's text and optional, explicitly assigned SQL condition.
///
/// The surrounding error variant still decides engine behavior such as SQLite constraint
/// resolution and catchability. An unclassified message uses that variant's generic condition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlErrorMessage {
    text: String,
    kind: Option<SqlErrorKind>,
}

impl SqlErrorMessage {
    pub fn classified(kind: SqlErrorKind, text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            kind: Some(kind),
        }
    }

    pub fn kind(&self) -> Option<SqlErrorKind> {
        self.kind
    }

    pub fn into_string(self) -> String {
        self.text
    }

    /// Add diagnostic context without changing the producer's condition.
    pub fn with_text(mut self, text: impl Into<String>) -> Self {
        self.text = text.into();
        self
    }
}

impl From<String> for SqlErrorMessage {
    fn from(text: String) -> Self {
        Self { text, kind: None }
    }
}

impl From<&str> for SqlErrorMessage {
    fn from(text: &str) -> Self {
        text.to_owned().into()
    }
}

impl std::fmt::Display for SqlErrorMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.text, f)
    }
}

impl std::ops::Deref for SqlErrorMessage {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.text
    }
}

impl std::ops::DerefMut for SqlErrorMessage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.text
    }
}

impl AsRef<str> for SqlErrorMessage {
    fn as_ref(&self) -> &str {
        &self.text
    }
}

impl PartialEq<str> for SqlErrorMessage {
    fn eq(&self, other: &str) -> bool {
        self.text == other
    }
}

impl PartialEq<&str> for SqlErrorMessage {
    fn eq(&self, other: &&str) -> bool {
        self.text == *other
    }
}

impl PartialEq<String> for SqlErrorMessage {
    fn eq(&self, other: &String) -> bool {
        self.text == *other
    }
}
