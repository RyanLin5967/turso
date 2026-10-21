//! Which PostgreSQL 18.6 functions and casts are immutable, for the parts of the frontend and the
//! translator that must decide what PostgreSQL's planner folds before anything runs (one list for
//! both: the fold probe and folds_at_plan in the frontend, the AND/OR simplification here).

/// PostgreSQL 18.6's functions that are stable or volatile in every overload (recorded,
/// pg18-nonimmutable-functions.list): the planner folds immutable calls only, so a statement
/// calling one raises its error at execution, after RowDescription (pg_database_size(99999::oid):
/// recorded, pg18-wirefuzz-pg_database_size_of_null_or_an_unknown_oid_answers_as_postgresql_does
/// .out #3, #4). BLIND SPOT: a name with immutable and other overloads (31 of them) is taken as
/// folded.
static NON_IMMUTABLE: std::sync::LazyLock<Vec<&'static str>> = std::sync::LazyLock::new(|| {
    include_str!("pg18-nonimmutable-functions.list")
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect()
});

/// PostgreSQL 18.6's base and range types whose input function is stable or volatile (pg_type
/// typinput's provolatile; recorded on iccad2, fresh initdb, 2026-10-19), sorted.
const STABLE_INPUT: [&str; 32] = [
    "aclitem", "date", "datemultirange", "daterange", "int4multirange", "int4range",
    "int8multirange", "int8range", "interval", "money", "nummultirange", "numrange", "regclass",
    "regcollation", "regconfig", "regdictionary", "regnamespace", "regoper", "regoperator",
    "regproc", "regprocedure", "regrole", "regtype", "time", "timestamp", "timestamptz", "timetz",
    "tsmultirange", "tsrange", "tstzmultirange", "tstzrange", "xml",
];

/// Whether the function named `name` (bare, lowercased) is immutable in PostgreSQL 18.6 as far as
/// [`NON_IMMUTABLE`] tells (a name it does not list).
pub fn is_immutable_call(name: &str) -> bool {
    NON_IMMUTABLE.binary_search(&name).is_err()
}

/// Whether a cast to the type named `name` reads through an immutable input function
/// ([`STABLE_INPUT`] does not list it).
pub fn has_immutable_input(name: &str) -> bool {
    STABLE_INPUT.binary_search(&name.to_ascii_lowercase().as_str()).is_err()
}
