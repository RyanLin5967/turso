//! Diagnostic metadata shared by the SQL parser, compiler and execution engine.
//!
//! A producer chooses the condition when it creates the error. Changing the human-readable
//! message cannot change that condition. The shared PostgreSQL mapping also serves input diagnostics.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlErrorKind {
    SyntaxError,
    FeatureNotSupported,
    InvalidTextRepresentation,
    NumericValueOutOfRange,
    StringDataRightTruncation,
    DuplicateTable,
    DuplicateSchema,
    InvalidSchemaName,
    DependentObjectsStillExist,
    IndeterminateDatatype,
    DivisionByZero,
    IntegrityConstraintViolation,
    UniqueViolation,
    NotNullViolation,
    CheckViolation,
    ForeignKeyViolation,
    InvalidColumnReference,
    DatetimeFieldOverflow,
    InvalidDatetimeFormat,
    InvalidParameterValue,
    UndefinedTable,
    InvalidForeignKey,
    UndefinedColumn,
    UndefinedObject,
    UndefinedParameter,
    InvalidSavepointSpecification,
    AmbiguousColumn,
    AmbiguousParameter,
    DatatypeMismatch,
    UndefinedFunction,
    DuplicateAlias,
    StatementTooComplex,
    InternalError,
    SerializationFailure,
    LockNotAvailable,
    ReadOnlyTransaction,
    DiskFull,
    QueryCanceled,
}

impl SqlErrorKind {
    /// PostgreSQL SQLSTATE for a producer-selected SQL condition.
    pub fn postgres_code(self) -> &'static str {
        use SqlErrorKind::*;
        match self {
            SyntaxError => "42601",
            FeatureNotSupported => "0A000",
            InvalidTextRepresentation => "22P02",
            NumericValueOutOfRange => "22003",
            StringDataRightTruncation => "22001",
            DuplicateTable => "42P07",
            DuplicateSchema => "42P06",
            InvalidSchemaName => "3F000",
            DependentObjectsStillExist => "2BP01",
            IndeterminateDatatype => "42P18",
            DivisionByZero => "22012",
            IntegrityConstraintViolation => "23000",
            UniqueViolation => "23505",
            NotNullViolation => "23502",
            CheckViolation => "23514",
            ForeignKeyViolation => "23503",
            InvalidColumnReference => "42P10",
            DatetimeFieldOverflow => "22008",
            InvalidDatetimeFormat => "22007",
            InvalidParameterValue => "22023",
            UndefinedTable => "42P01",
            InvalidForeignKey => "42830",
            UndefinedColumn => "42703",
            UndefinedObject => "42704",
            UndefinedParameter => "42P02",
            InvalidSavepointSpecification => "3B001",
            AmbiguousColumn => "42702",
            AmbiguousParameter => "42P08",
            DatatypeMismatch => "42804",
            UndefinedFunction => "42883",
            DuplicateAlias => "42712",
            StatementTooComplex => "54001",
            InternalError => "XX000",
            SerializationFailure => "40001",
            LockNotAvailable => "55P03",
            ReadOnlyTransaction => "25006",
            DiskFull => "53100",
            QueryCanceled => "57014",
        }
    }
}

/// A diagnostic's text and optional, explicitly assigned SQL condition.
///
/// The surrounding error variant still decides engine behavior such as SQLite constraint
/// resolution and catchability. An unclassified message uses that variant's generic condition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlErrorMessage {
    text: String,
    kind: Option<SqlErrorKind>,
    protocol_text: Option<String>,
}

impl SqlErrorMessage {
    pub fn classified(kind: SqlErrorKind, text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            kind: Some(kind),
            protocol_text: None,
        }
    }

    /// An explicit protocol diagnostic; the engine's existing display and result code stay intact.
    pub fn with_protocol_text(mut self, text: impl Into<String>) -> Self {
        self.protocol_text = Some(text.into());
        self
    }

    pub fn protocol_text(&self) -> Option<&str> {
        self.protocol_text.as_deref()
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
        Self {
            text,
            kind: None,
            protocol_text: None,
        }
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
