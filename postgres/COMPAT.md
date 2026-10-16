# Turso Postgres Compatibility Reference

This document tracks PostgreSQL feature compatibility for the Turso Postgres
frontend. The feature list is based on the official
[PostgreSQL feature matrix](https://www.postgresql.org/about/featurematrix/),
with additional rows for baseline features the matrix does not enumerate.

Status legend:

| Status | Meaning |
|--------|---------|
| ✅ Supported | Feature works and is covered by tests |
| 🟡 Partial | Feature works with limitations (see notes) |
| ❌ Not supported | Feature is not implemented |

## Architecture

The frontend parses SQL with `pg_query` (libpg_query, the real PostgreSQL
grammar), so virtually all PostgreSQL *syntax* is accepted at parse time.
Support is decided by the translator (`postgres/parser/translator.rs`), which
converts the PostgreSQL AST into Turso's native AST, and by what the Turso
engine can execute. Components:

- `postgres/parser` — parse + translate PostgreSQL SQL to Turso AST
- `postgres/frontend` — pg_catalog emulation, COPY, schemas, session handling
- `postgres/server` — PostgreSQL wire protocol (v3) server, built on `pgwire`
- `postgres/cli` — `tursopg`, a psql-like REPL that can also host the server

Because the parser accepts the full PostgreSQL grammar, some clauses parse
and run but their semantics are silently dropped, producing wrong results or
lost information without any error. These are called out as "silently
ignored" in the notes of the tables below.

## Core SQL baseline

Basics not enumerated by the official feature matrix.

| Feature | Status | Notes |
|---------|--------|-------|
| SELECT (projections, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET) | ✅ Supported | |
| JOINs (INNER, LEFT, RIGHT, FULL, CROSS, NATURAL, USING) | ✅ Supported | A parameter compared with a column USING or NATURAL merges from columns of different types (int4 and int8) is refused 42P18, unless declared; PostgreSQL types it as their common type (int8) |
| UNION / UNION ALL / INTERSECT / EXCEPT | 🟡 Partial (fixes unbuilt) | Including ORDER BY/LIMIT on compounds, PostgreSQL's grouping (INTERSECT binds tighter; parentheses group), an arm's own ORDER BY/LIMIT/OFFSET/WITH and a WITH on the whole set operation (non-left-spine and clause-bearing arms run as subqueries), and a VALUES arm. INTERSECT ALL and EXCEPT ALL are rejected (0A000): the engine has neither. ✅ once the reds (570cbe2c8, 68e5afaf6) run green |
| Subqueries (FROM, IN, EXISTS, scalar) | ✅ Supported | `= ANY(array)` and `<> ALL(array)` work, including bound text-array parameters; other ANY/ALL array operators are rejected; `ALL`/row comparison subqueries error |
| FROM alias column lists (`t AS a (x, y)`) | 🟡 Partial | Rename the table's first columns, as in PostgreSQL (42P10 for a list longer than the table); on a table of the main schema only (read from the in-memory schema), elsewhere refused |
| INSERT (column lists, multi-row VALUES, DEFAULT, INSERT ... SELECT) | ✅ Supported | VALUES lists are checked as in PostgreSQL (one length, 42601 "VALUES lists must all be the same length"; against the column list, "INSERT has more target columns than expressions" / "more expressions than target columns"). A DEFAULT takes the column's default in every position, in some rows or all, with or without a column list. A positional VALUES list shorter than the table fills its leading columns, the rest taking their defaults, as in PostgreSQL; one longer is 42601 "INSERT has more expressions than target columns" |
| UPDATE (incl. FROM clause) | ✅ Supported | Including multi-column `SET (a, b) = (...)` |
| DELETE | 🟡 Partial (fixes unbuilt) | `USING` runs as `WHERE rowid IN (SELECT <target>.rowid FROM <target>, <using> WHERE <condition>)`: target and USING items in one namespace, so a bare name both have is ambiguous (42702) and the target named again is 42712. RETURNING resolves a bare name over the target and the USING items together, as PostgreSQL does: a name both have is 42702; a `*`, a USING relation's column (qualified, or a bare name only it has), a whole-row reference to a USING item, and a bare name the target has beside a USING item whose columns cannot be read are rejected (0A000); return the target's columns. A target declaring a column named rowid is handled (the rewrite uses the first of rowid, _rowid_, oid it does not declare; all three declared: 0A000). ✅ once the reds (736b058a7, 9181fb31f, 8cbc78699, 9e06059a4, ae70b3f9d) run green |
| CREATE TABLE | ✅ Supported | PK, NOT NULL, UNIQUE, DEFAULT, CHECK, FK (with ON DELETE/UPDATE actions, and DEFERRABLE [INITIALLY DEFERRED], checked at COMMIT; a multi-column MATCH FULL key is refused 0A000, the engine enforcing MATCH SIMPLE only; a key's parent columns must name a unique key in that key's own column order, where PostgreSQL accepts any order: `REFERENCES p(b, a)` over `UNIQUE (a, b)` is 42830 here); IF NOT EXISTS; tables are created STRICT. A single integer PRIMARY KEY column is the table's rowid: an INSERT of NULL into it takes a new key where PostgreSQL raises 23502 |
| CREATE TABLE AS / SELECT INTO | ✅ Supported | Schema derived from the SELECT; WITH NO DATA supported (lowered to LIMIT 0, so errors in an overridden LIMIT go unreported); explicit column list rejected; INTO on the first leaf of a compound SELECT (legal in PG) rejected; TEMP silently ignored; completes with `SELECT n` like PostgreSQL, though an IF NOT EXISTS skip tags `SELECT 0` instead of `CREATE TABLE AS` |
| ALTER TABLE | 🟡 Partial | ADD/DROP COLUMN, RENAME TABLE/COLUMN work; ADD [CONSTRAINT n] PRIMARY KEY / UNIQUE / FOREIGN KEY / CHECK works by rebuilding the table (a multi-column MATCH FULL key refused 0A000 before the rebuild; validated against its rows, atomic, indexes and triggers recreated; a table created by CREATE TABLE AS or outside schema public is refused); ADD CONSTRAINT ... USING INDEX, NOT VALID or DEFERRABLE rejected; ALTER COLUMN TYPE translates but fails at execution; SET/DROP DEFAULT, SET/DROP NOT NULL rejected |
| CREATE INDEX | ✅ Supported | UNIQUE, multi-column, partial (WHERE), expression indexes, IF NOT EXISTS |
| CREATE VIEW | ✅ Supported | Column aliases supported; TEMP silently ignored |
| COMMENT ON | 🟡 Partial | Accepted but discarded; comments are not persisted in `pg_description` |
| CREATE SCHEMA / DROP SCHEMA | 🟡 Partial | Schemas are ATTACHed databases; DROP ... CASCADE; `public` is special-cased. A `public` qualifier names the main database wherever it is read (a relation, an index, DROP, a `public.t.c` column reference, COPY), whatever the search path. Under `tursopg --server` both are refused with 0A000 for any schema but public (the branch server is single-schema: each session attaches the schema files present when it opens, so a schema created or dropped while sessions run would leave them disagreeing, and a branch would not cover it); a schema file that predates the server is still attached at session open |
| CREATE SEQUENCE / nextval / currval / setval | ✅ Supported | START, INCREMENT, MIN/MAXVALUE, CYCLE; CACHE accepted (no-op); pg_sequences view |
| CREATE DOMAIN | ✅ Supported | Base type + DEFAULT, NOT NULL, CHECK constraints enforced |
| CREATE TYPE ... AS ENUM | ✅ Supported | Values validated on write; other CREATE TYPE forms unsupported |
| TRUNCATE | 🟡 Partial | Lowered to DELETE of the first table only; CASCADE / RESTART IDENTITY dropped |
| BEGIN / COMMIT / ROLLBACK | 🟡 Partial | Isolation-level options accepted (the engine has one level); READ ONLY refused 0A000 (READ WRITE accepted); COMMIT / ROLLBACK AND CHAIN refused 0A000 in a live block (no chained transactions), 25P01 outside a block or in an implicit block, and in a failed block they end it (ROLLBACK) and chain a new block, as PostgreSQL does |
| Casts (`expr::type`, CAST) | ✅ Supported | Also `int4(x)`-style cast functions |
| Parameters (`$1`, `$2`, ...) | 🟡 Partial | Through the extended wire protocol (over the simple protocol a `$n` is 42P02, as nothing binds it); values in text format, and in binary for the types the V3 row lists. A parameter the client did not declare is typed from its context, as PostgreSQL infers it (the column it is compared with, assigned to or inserted into, an aggregate, a cast, LIMIT's bigint, ...). Where PostgreSQL would still type it, two cases are refused 42P18 "could not determine data type of parameter $n" unless the client declares the type: a parameter compared with, assigned to or inserted into a column of a relation in an attached schema (any schema but public, which the typing walk cannot read; also through a CTE's or a FROM alias's column list over `SELECT *` of such a relation), and one compared with an expression the walk does not type (a function it does not know) |
| Operators: `\|\|`, `%`, bitwise, ILIKE, SIMILAR TO, `~`/`~*`/`!~`/`!~*`, IS [NOT] DISTINCT FROM, BETWEEN | ✅ Supported | Regex operators lower to REGEXP; case-insensitive variants treated as sensitive |
| Logical operators (AND, OR, NOT) over integer operands | 🟡 Divergence | `SELECT 1 AND 0` and `SELECT NOT 1` are accepted (0 is false, any other integer true) and answer bool (16); PostgreSQL refuses them, 42804 "argument of AND must be type boolean". Pinned by tursopg.rs `wire_logical_operators_accept_integer_operands` (lead ruling, DECISIONS 9d8c1c38ef). Comparisons and logical operators over boolean operands announce bool, as PostgreSQL 18.6 does (recorded) |
| Dollar-quoted strings, escape strings (`E'...'`), bit/hex string literals | ✅ Supported | |
| generate_series | ✅ Supported | In FROM, with joins, and reading the row it is joined to (`FROM t, generate_series(1, t.x) AS g`); its column is named as in PostgreSQL (the alias, `AS g(x)`'s column, or `generate_series`). WITH ORDINALITY and a second column alias are rejected with an error. In the joined form `SELECT *` names the column `value`. A bare name of the series column resolves level by level as in PostgreSQL 18.6 (recorded 2026-10-15): another FROM item of the same SELECT with a column of that name makes it ambiguous (42702); a subquery's own relation with the name binds there; where no relation has it, it binds the series (E5-QUEUE G1a-G1d). Two series columns of one name make a bare reference ambiguous (42702) |
| pg_catalog emulation | 🟡 Partial | See Backend section |
| SET / SHOW | 🟡 Partial | From an allowlist, never passed to the engine; see Configuration Management |
| SHOW ALL | ❌ Not supported | Refused 0A000; PostgreSQL lists all settings (`PGDIV-SHOW-ALL`) |

## Backend

The pg_catalog tables emulated (live, reflecting real schema): `pg_class`,
`pg_namespace`, `pg_attribute`, `pg_type` (builtin + array + enum types),
`pg_index`, `pg_constraint`, `pg_attrdef`, `pg_tables`, `pg_sequences`,
`pg_database`, `pg_roles` (single hardcoded `turso` role), `pg_proc`, `pg_am`,
plus `pg_input_error_info`. Present but always empty: `pg_policy`,
`pg_trigger`, `pg_statistic_ext`, `pg_inherits`, `pg_rewrite`,
`pg_foreign_table`, `pg_partitioned_table`, `pg_collation`, `pg_description`,
`pg_publication*`. Catalog introspection functions: `format_type`,
`pg_get_constraintdef`, `pg_get_indexdef`, `pg_get_expr` (returns TursoPG's
stored SQL; relation oid and pretty printing do not change the output),
`pg_get_userbyid`, `pg_*_is_visible`, `pg_encoding_to_char`,
`pg_input_is_valid`, `to_char` (numeric), `now`/`clock_timestamp`/
`transaction_timestamp`/`statement_timestamp`.
Session information functions: `version()`, `current_database()` /
`current_catalog`, `current_schema` (call, bare-keyword, and FROM-position
forms), `pg_backend_pid()`, `quote_ident()`, `quote_literal()`;
`obj_description()`/`col_description()` always return NULL because COMMENT ON
is not persisted. DML against pg_catalog is rejected. `pg_typeof` is not
implemented.

| Feature | Status | Notes |
|---------|--------|-------|
| 64-bit large objects | ❌ Not supported | |
| Advisory locks | ❌ Not supported | |
| Custom background workers | ❌ Not supported | |
| Disk based FSM | ❌ Not supported | |
| Dynamic Background Workers | ❌ Not supported | |
| EXPLAIN (BUFFERS) support | ❌ Not supported | EXPLAIN is not supported at all |
| EXPLAIN (MEMORY) | ❌ Not supported | |
| EXPLAIN (SERIALIZE) support | ❌ Not supported | |
| EXPLAIN (WAL) support | ❌ Not supported | |
| "jsonlog" logging format | ❌ Not supported | |
| Loadable plugin infrastructure for monitoring the planner | ❌ Not supported | |
| Payload support for LISTEN/NOTIFY | ❌ Not supported | LISTEN/NOTIFY not supported |
| pg_stat_checkpointer system view | ❌ Not supported | |
| pg_stat_io - I/O metrics view | ❌ Not supported | |
| pg_wait_events system view | ❌ Not supported | |
| Server statistics in shared memory | ❌ Not supported | |
| SQL-standard information schema | 🟡 Partial | `columns`, `tables`, `table_constraints` and `key_column_usage` for schema public (constraint names as PostgreSQL derives them); no other view |
| Support for anonymous shared memory | ❌ Not supported | |
| XML, JSON and YAML output for EXPLAIN | ❌ Not supported | |

## Data Types, Functions, & Operators

Type mapping: serial/smallserial/bigserial (and serial2/4/8) become
`INTEGER NOT NULL DEFAULT nextval(...)` with an implicit sequence. boolean,
smallint, bigint, uuid, date, time, timestamp[tz], bytea, json, jsonb, inet,
cidr, macaddr, macaddr8 map to Turso custom types. varchar(n)/char(n) and
numeric(p,s) keep their type modifiers. char(n) is stored without its trailing
blanks and padded back on output; its comparisons ignore trailing blanks against
a column or a literal (by scan and by index seek alike), but not yet against a
bound parameter (`code = $1` compares as text), and comparing with a literal
longer than n raises "value too long" where PostgreSQL answers false (both are
the engine's: it encodes only literals, with the column's length check). interval, xml, tsvector/tsquery,
bit/varbit, geometric types degrade to TEXT; money to REAL; OID/reg* types to
INTEGER. Unknown type names pass through as custom types.

| Feature | Status | Notes |
|---------|--------|-------|
| Arrays of compound types | ❌ Not supported | |
| Array support | 🟡 Partial | `type[]` columns, ARRAY[...] literals (announced int4[] / text[] when their elements are integer / string literals), subscripts `a[i]`, slices `a[i:j]`, `@>`, `<@`, `&&`, array_length, array_append, array_agg; backed by native Turso arrays. A cast to an array type is kept for a string literal (`'{1,2}'::int[]`), a bound parameter and an ARRAY[...]; of any other expression it is refused 0A000 (the engine casts by the element's scalar type, which answered 0). A bound array parameter sent back in a result is the engine's record bytes, not its text (known red) |
| ENUM data type | ✅ Supported | CREATE TYPE ... AS ENUM; values validated on write; DROP TYPE [IF EXISTS] |
| GUID/UUID data type | ✅ Supported | uuid columns, gen_random_uuid(), input validation, text casts |
| macaddr8 data type | ✅ Supported | Along with inet, cidr, macaddr (round-trip tested) |
| Multiranges | ❌ Not supported | |
| NULLs in Array | ✅ Supported | `ARRAY[1, NULL, 3]` round-trips |
| Phrase search | ❌ Not supported | tsvector/tsquery degrade to TEXT; no full-text search |
| Range types | ❌ Not supported | |
| smallserial type | ✅ Supported | serial2/serial4/serial8 aliases too; serial implies NOT NULL + implicit sequence, not PRIMARY KEY |
| Type modifier support | ✅ Supported | varchar(n) length enforced ("value too long for varchar"); numeric(p,s) scale applied |
| UUIDv7 | ❌ Not supported | |
| XML data type | ❌ Not supported | xml columns degrade to TEXT; no XML functions |

## Indexing & Constraints

| Feature | Status | Notes |
|---------|--------|-------|
| Block-range (BRIN) indexes | ❌ Not supported | |
| B-tree bottom-up index deletion | ❌ Not supported | |
| B-tree deduplication | ❌ Not supported | |
| Concurrent GiST indexes | ❌ Not supported | |
| Covering Indexes for B-trees (INCLUDE) | ❌ Not supported | INCLUDE clause silently ignored |
| Covering indexes for GiST (INCLUDE) | ❌ Not supported | |
| Deferrable unique constraints | ❌ Not supported | |
| Exclusion constraints | ❌ Not supported | |
| GIN (Generalized Inverted Index) indexes | ❌ Not supported | `USING <method>` silently ignored; every index is a B-tree |
| GIN indexes partial match | ❌ Not supported | |
| GIN index performance and size improvements | ❌ Not supported | |
| GiST (Generalized Search Tree) indexes | ❌ Not supported | |
| Indexes on expressions | ✅ Supported | Partial (WHERE) indexes also supported |
| Index-only scans | ❌ Not supported | |
| Index-only scans on GiST | ❌ Not supported | |
| Index support for IS NULL | ❌ Not supported | |
| In-memory Bitmap Indexes | ❌ Not supported | |
| K-nearest neighbor GiST support | ❌ Not supported | |
| K-nearest neighbor SP-GiST support | ❌ Not supported | |
| Non-blocking CREATE INDEX | ❌ Not supported | CONCURRENTLY silently ignored |
| Parallel B-tree index scans | ❌ Not supported | |
| Parallelized CREATE INDEX for BRIN indexes | ❌ Not supported | |
| Parallelized CREATE INDEX for B-tree indexes | ❌ Not supported | |
| Parallelized CREATE INDEX for GIN indexes | ❌ Not supported | |
| Skip scan on multicolumn B-tree indexes | ❌ Not supported | |
| Space-Partitioned GiST (SP-GiST) indexes | ❌ Not supported | |
| SP-GiST indexes for range types | ❌ Not supported | |
| UNIQUE NULLS NOT DISTINCT | ❌ Not supported | Silently ignored |
| WAL support for hash indexes | ❌ Not supported | |

## SQL

| Feature | Status | Notes |
|---------|--------|-------|
| ANY_VALUE aggregate | ❌ Not supported | |
| DISTINCT ON | ❌ Not supported | Accepted but silently degrades to plain DISTINCT (wrong results) |
| FETCH FIRST .. WITH TIES | ❌ Not supported | `FETCH FIRST n ROWS ONLY` works (lowered to LIMIT); WITH TIES silently ignored |
| GROUPING SETS, CUBE and ROLLUP support | ❌ Not supported | Translation error |
| INSERT/UPDATE/DELETE RETURNING | ✅ Supported | Including `RETURNING *` and UPDATE ... FROM ... RETURNING; completes as `INSERT 0 n`, `UPDATE n`, `DELETE n` as in PostgreSQL, except behind a WITH (`SELECT n`) |
| LATERAL clause | 🟡 Partial | `CROSS JOIN LATERAL (SELECT e1, ...) AS o(n1, ...)` (one row of expressions over the outer row) is inlined when every `ei` is a scalar expression (no aggregate, window, volatile or set-returning function, or subquery) and its columns are read as `o.ni` (`*`, `o.*` and an unqualified `ni` are rejected); a LATERAL join with ON, USING or NATURAL, LEFT JOIN LATERAL, a comma LATERAL and every other LATERAL are rejected with an error |
| MERGE | ❌ Not supported | |
| MERGE ... RETURNING | ❌ Not supported | |
| Multirow VALUES | ✅ Supported | In INSERT and as standalone VALUES lists |
| ORDER BY / LIMIT on a standalone VALUES list | ❌ Not supported | Rejected: "ORDER BY clause is not allowed with VALUES clause" |
| Non-decimal integer literals | ✅ Supported | `0x`, `0o`, `0b` |
| ORDER BY NULLS FIRST/LAST | ✅ Supported | Honored in SELECT, window, and compound SELECT ORDER BY; rejected (matching SQLite) in CREATE INDEX |
| range_agg range type aggregation function | ❌ Not supported | |
| Recursive queries | 🟡 Partial | WITH RECURSIVE works with SQLite semantics (row-at-a-time recursive term, so e.g. DISTINCT in the recursive term over a multi-row anchor can differ from PG); SEARCH/CYCLE clauses are rejected |
| regexp_count, regexp_instr, regexp_like | ❌ Not supported | Regex *operators* (`~`, `~*`, SIMILAR TO) work |
| Return OLD and NEW values from modified rows | ❌ Not supported | |
| Row-wise comparison | ✅ Supported | Row constructors compare element-wise, as `(a,b) < (c,d)` does in PostgreSQL |
| SELECT ... FOR UPDATE/SHARE | ❌ Not supported | Accepted but silently ignored — no locking happens |
| SELECT FOR NO KEY UPDATE/SELECT FOR KEY SHARE lock modes | ❌ Not supported | Accepted but silently ignored — no locking happens |
| SQL standard interval handling | ❌ Not supported | interval degrades to TEXT; no interval arithmetic |
| SYSTEM_USER | ❌ Not supported | current_user/current_role return stub values |
| TABLE statement | ✅ Supported | |
| Underscores (_) for thousands separators | ✅ Supported | |
| unnest/array_agg | 🟡 Partial | array_agg works; unnest is not implemented |
| Upsert (INSERT ... ON CONFLICT DO ...) | ✅ Supported | DO NOTHING and DO UPDATE SET ... (with EXCLUDED and conflict targets). A bare column in DO UPDATE's SET values or its WHERE names the target's column here, where PostgreSQL refuses it (42702: the target and EXCLUDED both hold it, analyze.c adds both to the namespace); qualify it (`t.c`, `excluded.c`) to mean the same in both |
| Window functions | 🟡 Partial | Aggregate window functions (COUNT/SUM/AVG/MIN/MAX OVER), row_number, PARTITION BY/ORDER BY, frame clauses, and named WINDOW clauses work; rank, dense_rank, lag, lead, etc. are not implemented |
| WITHIN GROUP clause | ❌ Not supported | Silently dropped; ordered-set aggregates (percentile_cont) missing |
| WITH ORDINALITY clause | ❌ Not supported | Rejected with an error; column aliases on a table function other than generate_series are rejected too |
| WITH queries (Common Table Expressions) | ✅ Supported | Including WITH RECURSIVE; MATERIALIZED hints accepted |
| Writable WITH queries (Common Table Expressions) | ❌ Not supported | "CTE query is not a SELECT statement" |

## Data Definition Language (DDL)

| Feature | Status | Notes |
|---------|--------|-------|
| ALTER object IF EXISTS | 🟡 Partial | ALTER TABLE IF EXISTS works when the table exists, but a missing table still errors instead of being skipped |
| ALTER TABLE ... ADD UNIQUE/PRIMARY KEY USING INDEX | ❌ Not supported | ADD CONSTRAINT is rejected |
| ALTER TABLE ... SET ACCESS METHOD | ❌ Not supported | |
| ALTER TABLE ... SET LOGGED / UNLOGGED | ❌ Not supported | |
| Changing column types (ALTER TABLE .. ALTER COLUMN TYPE) | ❌ Not supported | Translates but breaks at execution ("no such column" on subsequent use) |
| CREATE ACCESS METHOD | ❌ Not supported | |
| CREATE TABLE ... (LIKE) | ❌ Not supported | Refused 0A000, no table created (PostgreSQL copies the source's columns); it was silently dropped, leaving a table without those columns, on which `ALTER TABLE ... DROP` panicked the engine |
| DROP object IF EXISTS | ✅ Supported | TABLE, INDEX, VIEW, MATERIALIZED VIEW, TYPE, DOMAIN, SEQUENCE, SCHEMA |
| ON COMMIT clause for CREATE TEMPORARY TABLE | ❌ Not supported | TEMP itself is silently ignored |
| REINDEX CONCURRENTLY | ❌ Not supported | REINDEX not supported at all |
| Stored generated columns | ❌ Not supported | Refused 0A000, no table created (the clause was dropped and the column read NULL) |
| Temporal constraints | ❌ Not supported | |
| Temporary tables (CREATE TEMP TABLE) | ❌ Not supported | TEMP silently ignored — the table is persistent |
| Typed tables | ❌ Not supported | |
| Virtual generated columns | ❌ Not supported | PostgreSQL 18's VIRTUAL is a syntax error in the vendored libpg_query grammar (42601); GENERATED ALWAYS AS (expr) without it is refused 0A000 |
| Identity columns (GENERATED ALWAYS / BY DEFAULT AS IDENTITY) | 🟡 Partial | Numbered by a sequence, as a serial column is (1, 2, ...); sequence options are refused 0A000; a GENERATED ALWAYS column takes an explicit value, which PostgreSQL refuses (428C9) |

## Performance

| Feature | Status | Notes |
|---------|--------|-------|
| Abbreviated keys | ❌ Not supported | |
| Asynchronous commit | ❌ Not supported | |
| Asynchronous I/O (AIO) | ❌ Not supported | |
| Automatic plan invalidation | ❌ Not supported | |
| Background checkpointer | ❌ Not supported | |
| Background writer | ❌ Not supported | |
| Base backup throttling | ❌ Not supported | |
| CREATE STATISTICS - most-common values (MCV) statistics | ❌ Not supported | |
| CREATE STATISTICS - multicolumn | ❌ Not supported | |
| CREATE STATISTICS - "OR" and "IN/ANY" statistics | ❌ Not supported | |
| Cross datatype hashing support | ❌ Not supported | |
| Distributed checkpointing | ❌ Not supported | |
| Foreign keys marked as NOT VALID | ❌ Not supported | |
| Foreign keys in a schema other than public (the child or the parent) | ❌ Not supported | Refused with 0A000 before anything is created, in CREATE TABLE and ALTER TABLE ADD CONSTRAINT: the key checks read public's catalog alone (wire review 16 item 11) |
| Frozen page map | ❌ Not supported | |
| Full text search | ❌ Not supported | tsvector/tsquery degrade to TEXT |
| Hash aggregation can use disk | ❌ Not supported | |
| Hashing support for DISTINCT/UNION/INTERSECT/EXCEPT | ❌ Not supported | |
| Hashing support for FULL OUTER JOIN, LEFT OUTER JOIN and RIGHT OUTER JOIN | ❌ Not supported | |
| Heap Only Tuples (HOT) | ❌ Not supported | |
| Improved performance for sorts exceeding working memory | ❌ Not supported | |
| Improved window function performance | ❌ Not supported | |
| Incremental sort | ❌ Not supported | |
| Incremental sort for SELECT DISTINCT | ❌ Not supported | |
| Incremental sort for window functions | ❌ Not supported | |
| Inlined WITH queries (Common Table Expressions) | ❌ Not supported | |
| Inlining of SQL functions | ❌ Not supported | |
| Just-in-Time (JIT) compilation for expression evaluation and tuple deforming | ❌ Not supported | |
| Load balancing for libpq / psql | ❌ Not supported | |
| LZ4 compression for TOAST tables | ❌ Not supported | |
| Multi-core scalability for read-only workloads | ❌ Not supported | |
| Multiple temporary tablespaces | ❌ Not supported | |
| Outer join reordering | ❌ Not supported | |
| Parallel bitmap heap scans | ❌ Not supported | |
| Parallel FULL and RIGHT joins | ❌ Not supported | |
| Parallel full table scans (sequential scans) | ❌ Not supported | |
| Parallel hash joins | ❌ Not supported | |
| Parallel JOIN, aggregate | ❌ Not supported | |
| Parallel merge joins | ❌ Not supported | |
| Parallel query | ❌ Not supported | |
| Parallel "SELECT DISTINCT" | ❌ Not supported | |
| Partial sort capability (top-n sorting) | ❌ Not supported | |
| Query pipelining | ❌ Not supported | |
| Reduced lock levels for ALTER TABLE commands | ❌ Not supported | |
| SELECT ... FOR UPDATE/SHARE NOWAIT | ❌ Not supported | Silently ignored |
| Set costs specific to TABLESPACEs | ❌ Not supported | |
| Shared row level locking | ❌ Not supported | |
| SIMD support for ARM | ❌ Not supported | |
| SIMD support for x86 | ❌ Not supported | |
| SKIP LOCKED clause | ❌ Not supported | Silently ignored |
| Synchronized sequential scanning | ❌ Not supported | |
| TABLESAMPLE clause | ❌ Not supported | Translation error |
| Tablespaces | ❌ Not supported | |
| Unlogged tables | ❌ Not supported | |
| WAL buffer auto-tuning | ❌ Not supported | |

## JSON

| Feature | Status | Notes |
|---------|--------|-------|
| Improved set of JSON functions and operators | ❌ Not supported | |
| JSONB data type | 🟡 Partial | jsonb columns supported (Turso custom type); `->` and `->>` operators work; `#>`, `#>>`, `?`, `?&`, `?\|`, `@@` fail to translate |
| JSONB-modifying operators and functions | ❌ Not supported | |
| JSONB subscripting | ❌ Not supported | |
| JSON data type | 🟡 Partial | Same as jsonb (see above) |
| SQL/JSON constructors | ❌ Not supported | |
| SQL/JSON: datetime() | ❌ Not supported | |
| SQL/JSON IS JSON | ❌ Not supported | |
| SQL/JSON JSON_TABLE | ❌ Not supported | |
| SQL/JSON path expressions | ❌ Not supported | |
| SQL/JSON query functions | ❌ Not supported | |

## Partitioning & Inheritance

| Feature | Status | Notes |
|---------|--------|-------|
| Accelerated partition pruning | ❌ Not supported | |
| Declarative table partitioning | ❌ Not supported | PARTITION BY / PARTITION OF silently dropped — the table is created unpartitioned |
| Default partition | ❌ Not supported | |
| Foreign key references for partitioned tables | ❌ Not supported | |
| Foreign table inheritance | ❌ Not supported | |
| Table inheritance (CREATE TABLE ... INHERITS) | ❌ Not supported | Refused 0A000, no table created (the clause was dropped, so the child lacked its parents' columns); PARTITION OF is refused the same way |
| Partitioning by a hash key | ❌ Not supported | |
| Partition pruning during query execution | ❌ Not supported | |
| Support for PRIMARY KEY, FOREIGN KEY, indexes, and triggers on partitioned tables | ❌ Not supported | |
| Table inheritance (INHERITS) | ❌ Not supported | INHERITS silently dropped |
| Table partitioning | ❌ Not supported | PARTITION BY / PARTITION OF silently dropped |
| UPDATE on a partition key | ❌ Not supported | |

## Views & Materialized Views

| Feature | Status | Notes |
|---------|--------|-------|
| Materialized views | ✅ Supported | Incrementally maintained (live, DBSP-based) — always fresh, unlike PostgreSQL snapshots; REFRESH MATERIALIZED VIEW is accepted as a no-op |
| Materialized views with concurrent refresh | 🟡 Partial | Moot: views are always fresh; REFRESH (CONCURRENTLY) is a no-op |
| SECURITY INVOKER views | ❌ Not supported | |
| Temporary VIEWs | ❌ Not supported | TEMP silently ignored; the view is persistent |
| Updatable views | ❌ Not supported | |
| Views over a table in a schema other than public | ❌ Not supported | CREATE VIEW is refused with the engine's message `view cannot reference table in attached database: s.u` (42601); PostgreSQL creates it |
| WITH CHECK clause | ❌ Not supported | |

## Replication

Replication is not supported.

## Backup, Restore, & Data Integrity

Backup and restore is not supported.

## Upgrade

Upgrade is not supported.

## Data Import & Export

| Feature | Status | Notes |
|---------|--------|-------|
| COPY table FROM 'file' (text format) | ✅ Supported | DELIMITER, NULL string, HEADER, column lists, backslash escapes, `\.` end-of-data marker; atomic (rolls back on malformed rows). The table is found as any statement's: `public.t` is the main database's whatever the search path, and an unqualified name goes through the search path (42P01 when no entry holds it) |
| COPY from/to STDIN/STDOUT | ❌ Not supported | Rejected with an error; no wire-level COPY sub-protocol |
| COPY HEADER MATCH | ❌ Not supported | Refused 0A000; PostgreSQL checks the header against the column names and uses 22P04 for a mismatch (`PGDIV-COPY-OPTION-REFUSED`) |
| COPY FROM ... WHERE | ❌ Not supported | |
| COPY ... ON_ERROR | ❌ Not supported | |
| COPY with arbitrary SELECT | ❌ Not supported | COPY TO is not supported at all |
| CSV support for COPY | ❌ Not supported | `FORMAT csv` and `FORMAT binary` rejected with an error |

## Configuration Management

| Feature | Status | Notes |
|---------|--------|-------|
| SET / SHOW configuration parameters | 🟡 Partial | From an allowlist, never passed to the engine as PRAGMAs. search_path is kept per session; SHOW gives it as PostgreSQL does (`"$user", public` before any SET), and a SET that changes it is followed by a ParameterStatus with the new path, as PostgreSQL reports it (GUC_REPORT). lock_timeout and client_min_messages act; statement_timeout, idle_in_transaction_session_timeout and transaction_timeout are accepted at 0 alone (0A000 otherwise); a pg_dump header's xmloption = content, default_tablespace = '' and default_table_access_method = heap are accepted, any other value of them 0A000. The client settings this server answers by (client_encoding UTF8, standard_conforming_strings on, bytea_output hex, DateStyle `ISO, MDY`, IntervalStyle postgres, TimeZone UTC, extra_float_digits 1) come from one table, which SHOW answers from and startup announces in ParameterStatus; SET accepts each only at a value this server behaves by and 0A000 at any other (DateStyle: ISO, MDY, US, NonEuro or DEFAULT; DMY, YMD and the SQL, Postgres and German styles refused). A setting PostgreSQL reads as one value takes one, as there (a list is 22023 `SET <name> takes only one argument`); DateStyle's list is read whole. Two accepted values SHOW does not echo: an equivalent zone name (GMT, Etc/UTC, Z, ...) leaves SHOW TimeZone at UTC, where PostgreSQL shows the name set, and extra_float_digits above 1 leaves SHOW at 1, where PostgreSQL shows the value (from 1 up both print floats in their shortest exact form there). search_path and foreign_keys are 0A000 inside a transaction block the client opened and as SET LOCAL: this server keeps a SET past the block whatever ends it, so a ROLLBACK could not undo them as PostgreSQL does (set them before BEGIN; in a multi-statement query's implicit block they act). Settings given in the startup packet are not applied. Planner and resource settings (work_mem, enable_*, ...) are accepted and change nothing; a custom (dotted) name is 0A000; a name PostgreSQL 18 knows that this server does not act on is 0A000 for SET and SHOW (the names are its settings table's, postgres/frontend/pg_settings.rs); any other name is 42704, as PostgreSQL answers. `pg_catalog.set_config(...)`, which a pg_dump header also calls, is not implemented |
| ALTER SYSTEM | ❌ Not supported | |
| Fractional input for "integer" values | ❌ Not supported | |
| Include directives for pg_hba.conf and pg_ident.conf | ❌ Not supported | |
| Per user/database server configuration settings | ❌ Not supported | |
| pg_config system view | ❌ Not supported | |
| Regular expression matching in pg_hba.conf and pg_ident.conf | ❌ Not supported | |

## Security

| Feature | Status | Notes |
|---------|--------|-------|
| Authentication (any method) | ❌ Not supported | The wire server trusts every connection; no password, MD5, SCRAM, or certificate checks |
| Channel binding for SCRAM authentication | ❌ Not supported | |
| Client can require SCRAM channel binding | ❌ Not supported | |
| Client-specified requirements for authentication | ❌ Not supported | |
| Column level permissions | ❌ Not supported | |
| Default permissions | ❌ Not supported | |
| Direct TLS negotiation ("sslnegotiation") | ❌ Not supported | |
| FIPS mode validation | ❌ Not supported | |
| GRANT/REVOKE ON ALL TABLES/SEQUENCES/FUNCTIONS | ❌ Not supported | GRANT/REVOKE not supported at all |
| GSSAPI client and server-side encryption | ❌ Not supported | |
| GSSAPI support | ❌ Not supported | |
| Kerberos credential delegation | ❌ Not supported | |
| krb5 authentication (without gssapi) | ❌ Not supported | |
| Large object access controls | ❌ Not supported | |
| LDAP server discovery | ❌ Not supported | |
| Multifactor authentication via valid client SSL/TLS certificate | ❌ Not supported | |
| Native LDAP authentication | ❌ Not supported | |
| Native RADIUS authentication | ❌ Not supported | |
| OAuth authentication / authorization | ❌ Not supported | |
| Per user/database connection limits | ❌ Not supported | |
| Predefined roles | ❌ Not supported | |
| Privileges for setting configuration parameters | ❌ Not supported | |
| ROLES | ❌ Not supported | pg_roles exposes a single hardcoded `turso` role |
| Row-level security | ❌ Not supported | |
| SCRAM-SHA-256 authentication | ❌ Not supported | |
| Search+bind mode operation for LDAP authentication | ❌ Not supported | |
| security_barrier option on views | ❌ Not supported | |
| Security Service Provider Interface (SSPI) | ❌ Not supported | |
| SHA-2 encryption for password hashing | ❌ Not supported | |
| SSL certificate validation in libpq | ❌ Not supported | |
| SSL client certificate authentication | ❌ Not supported | |
| SSPI authentication via GSSAPI | ❌ Not supported | |
| Support using the client's OS trusted CA | ❌ Not supported | |
| TLS v1.3 cipher suite allowlisting | ❌ Not supported | |

## Transactions and Visibility

| Feature | Status | Notes |
|---------|--------|-------|
| Cursors | ❌ Not supported | DECLARE/FETCH/MOVE not translated |
| Savepoints | ✅ Supported | SAVEPOINT, RELEASE, ROLLBACK TO |
| Serializable Snapshot Isolation | ❌ Not supported | BEGIN ISOLATION LEVEL ... accepted but the level is ignored |
| Two-phase commit | ❌ Not supported | PREPARE TRANSACTION rejected |
| Updatable cursors | ❌ Not supported | |

## VACUUM and Maintenance

| Feature | Status | Notes |
|---------|--------|-------|
| VACUUM / ANALYZE statements | ❌ Not supported | Rejected with an error |
| Inserted data can trigger autovacuum | ❌ Not supported | |
| Integrated autovacuum daemon | ❌ Not supported | |
| Page freezing optimizations | ❌ Not supported | |
| Parallelized VACUUM for indexes | ❌ Not supported | |
| Parallel vacuumdb jobs | ❌ Not supported | |
| Radix tree memory structure for vacuum | ❌ Not supported | |
| Vacuum "emergency mode" | ❌ Not supported | |
| Visibility map for vacuuming | ❌ Not supported | |

## Foreign Data Wrappers

| Feature | Status | Notes |
|---------|--------|-------|
| Certificate authentication with postgres_fdw | ❌ Not supported | |
| CREATE FOREIGN TABLE ... LIKE | ❌ Not supported | |
| Foreign data wrapper query parallelism | ❌ Not supported | |
| Foreign data wrappers | ❌ Not supported | |
| Foreign tables | ❌ Not supported | |
| IMPORT FOREIGN SCHEMA | ❌ Not supported | |
| Import foreign table partitions | ❌ Not supported | |
| Parallel query execution on remote databases | ❌ Not supported | |
| postgres_fdw parallel commit | ❌ Not supported | |
| postgres_fdw pushdown | ❌ Not supported | |
| postgres_fdw SCRAM authentication passthrough | ❌ Not supported | |
| PostgreSQL Foreign Data Wrapper | ❌ Not supported | |
| Writable Foreign Data Wrappers | ❌ Not supported | |

## Custom Functions, Stored Procedures, & Triggers

| Feature | Status | Notes |
|---------|--------|-------|
| ALTER TABLE ENABLE/DISABLE TRIGGER | ❌ Not supported | |
| ALTER TABLE / ENABLE REPLICA TRIGGER/RULE | ❌ Not supported | |
| BEGIN ATOMIC function bodies | ❌ Not supported | |
| CALL syntax for executing procedures | ❌ Not supported | |
| CREATE FUNCTION | ❌ Not supported | Rejected with an error |
| Column level triggers | ❌ Not supported | |
| CREATE PROCEDURE syntax for SQL stored procedures | ❌ Not supported | |
| Event triggers | ❌ Not supported | |
| FILTER clause for aggregate functions | ✅ Supported | |
| ORDER BY support within aggregates | ❌ Not supported | Silently dropped |
| Per function GUC settings | ❌ Not supported | |
| Per function statistics | ❌ Not supported | |
| RETURN QUERY EXECUTE | ❌ Not supported | |
| RETURNS TABLE | ❌ Not supported | |
| Statement level triggers | ❌ Not supported | CREATE TRIGGER not translated |
| Statement level TRUNCATE triggers | ❌ Not supported | |
| Triggers on views | ❌ Not supported | |
| Variadic functions | ❌ Not supported | |
| WHEN clause for CREATE TRIGGER | ❌ Not supported | |

## Procedural Languages

| Feature | Status | Notes |
|---------|--------|-------|
| Procedural languages (PL/pgSQL, PL/Python, ...) | ❌ Not supported | CREATE FUNCTION and DO are rejected with an error |
| CASE in pl/pgsql | ❌ Not supported | |
| CONTINUE statement for PL/pgSQL | ❌ Not supported | |
| CREATE TRANSFORM | ❌ Not supported | |
| DO statement for pl/perl | ❌ Not supported | |
| DO statement for pl/pgsql | ❌ Not supported | |
| EXCEPTION support in PL/pgSQL | ❌ Not supported | |
| EXECUTE USING in PL/pgSQL | ❌ Not supported | |
| FOREACH IN ARRAY in pl/pgsql | ❌ Not supported | |
| IN/OUT/INOUT parameters for pl/pgsql and PL/SQL | ❌ Not supported | |
| Named parameters | ❌ Not supported | |
| Non-superuser language creation | ❌ Not supported | |
| pl/pgsql installed by default | ❌ Not supported | |
| Polymorphic functions | ❌ Not supported | |
| Python 3 support for pl/python | ❌ Not supported | |
| Qualified function parameters | ❌ Not supported | |
| Query parallelism for RETURN QUERY | ❌ Not supported | |
| RETURN QUERY in pl/pgsql | ❌ Not supported | |
| ROWS and COST specification for functions | ❌ Not supported | |
| Scrollable and updatable cursor support for pl/pgsql | ❌ Not supported | |
| SQLERRM/SQLSTATE for pl/pgsql | ❌ Not supported | |
| Unicode object support in PL/python | ❌ Not supported | |
| User defined exceptions | ❌ Not supported | |
| Validator function for pl/perl | ❌ Not supported | |

## Extensions

| Feature | Status | Notes |
|---------|--------|-------|
| CREATE EXTENSION .. CASCADE | ❌ Not supported | |
| Extension installation | ❌ Not supported | CREATE EXTENSION not translated |
| Trusted extensions | ❌ Not supported | |

## Internationalisation

| Feature | Status | Notes |
|---------|--------|-------|
| Built-in, platform independent immutable collation | ❌ Not supported | |
| casefold | ❌ Not supported | |
| Column-level collation support | ❌ Not supported | COLLATE clauses silently stripped |
| Database level collation | ❌ Not supported | |
| Default ICU collations for clusters/databases | ❌ Not supported | |
| EUC_JIS_2004 / SHIFT_JIS_2004 support | ❌ Not supported | |
| ICU collations | ❌ Not supported | |
| LIKE comparisons for nondeterministic collations | ❌ Not supported | |
| Multibyte encoding support, incl. UTF8 | 🟡 Partial | UTF-8 only (Turso native encoding); no other server encodings |
| Multiple language support | ❌ Not supported | |
| Nondeterministic collations | ❌ Not supported | |
| pg_unicode_fast collation | ❌ Not supported | |
| Unicode string literals and identifiers | ❌ Not supported | |
| UTF8 support on Windows | ❌ Not supported | |

## Client Applications

| Feature | Status | Notes |
|---------|--------|-------|
| psql-style REPL | 🟡 Partial | `tursopg` (not psql itself); supports `\d[+]`, `\dt[+]`, `\di`, `\dv`, `\dn`, `\dT`, `\du`/`\dg`, `\df`, `\l`, `\x`, `\timing`, `\echo`, `\conninfo`, `\?`, `\q`, and interactive `quit`/`exit`; no `\copy`, `\i`, `\e`, `\set`, `\pset`, `\g`, `\watch` |
| pgbench | ❌ Not supported | |
| pg_combinebackup | ❌ Not supported | |
| pg_createsubscriber | ❌ Not supported | |
| pg_prewarm | ❌ Not supported | |
| pg_rewind | ❌ Not supported | |
| pg_standby | ❌ Not supported | |
| pg_upgrade | ❌ Not supported | |
| pg_waldump | ❌ Not supported | |
| pg_walsummary | ❌ Not supported | |
| pg_xlogdump | ❌ Not supported | |
| psql \bind | ❌ Not supported | |
| psql \dconfig | ❌ Not supported | |
| psql pipeline queries | ❌ Not supported | |
| psql named prepared statements | ❌ Not supported | SQL-level PREPARE/EXECUTE/DEALLOCATE are not translated |
| Version aware psql | ❌ Not supported | |

## Additional Modules (contrib)

| Feature | Status | Notes |
|---------|--------|-------|
| adminpack | ❌ Not supported | |
| auth_delay | ❌ Not supported | |
| auto_explain | ❌ Not supported | |
| btree_gin | ❌ Not supported | |
| btree_gist | ❌ Not supported | |
| citext | ❌ Not supported | |
| dblink | ❌ Not supported | |
| dblink asynchronous notification support | ❌ Not supported | |
| file_fdw | ❌ Not supported | |
| fuzzystrmatch | ❌ Not supported | |
| hstore | ❌ Not supported | |
| intarray | ❌ Not supported | |
| isn (ISBN) | ❌ Not supported | |
| KNN support for CUBE | ❌ Not supported | |
| ltree | ❌ Not supported | |
| pageinspect | ❌ Not supported | |
| passwordcheck | ❌ Not supported | |
| pg_buffercache | ❌ Not supported | |
| pg_freespacemap | ❌ Not supported | |
| pg_logicalinspect | ❌ Not supported | |
| pg_overexplain | ❌ Not supported | |
| pg_stat_statements | ❌ Not supported | |
| pgstattuple | ❌ Not supported | |
| pg_trgm | ❌ Not supported | |
| pg_trgm regular expressions indexing | ❌ Not supported | |
| pg_walinspect | ❌ Not supported | |
| seg | ❌ Not supported | |
| sepgsql | ❌ Not supported | |
| sslinfo | ❌ Not supported | |
| tablefunc | ❌ Not supported | |
| tcn | ❌ Not supported | |
| tsearch2 compatibility wrapper | ❌ Not supported | |
| unaccent | ❌ Not supported | |
| uuid-ossp | ❌ Not supported | gen_random_uuid() (core) is available |
| xml2 | ❌ Not supported | |

## Network

| Feature | Status | Notes |
|---------|--------|-------|
| Full SSL support | ❌ Not supported | SSLRequest answered with "SSL not available"; plaintext only |
| IPv6 support | 🟡 Partial | Server binds whatever address it is given, including IPv6 literals; no dual-stack handling |
| V2 client protocol | ❌ Not supported | |
| V3 client protocol | 🟡 Partial | Via pgwire: simple query and extended query (Parse/Bind/Describe/Execute/Sync/Close/Flush) protocols; trust auth only; parameters in text format for every type, in binary for int2/int4/int8, float4/float8, bool, bytea, jsonb and the text types (any other binary parameter: 0A000); Execute row limits honoured (PortalSuspended). Bind is checked in PostgreSQL's order (parameter-format count 08P01, value count 08P01, failed block 25P02, each sent value's format code 22023); every statement's value count is checked at Bind, its parameters counted as PostgreSQL counts them (a statement it analyses at Parse: the declared ones and every `$n` its lexer reads, never one in a string, a quoted name, a dollar quote, a comment or an identifier; a utility statement such as SET, COPY or ALTER: the declared ones alone), except a `$n` above 65535, left to the prepare's 42601 (E5-QUEUE R2(a) closed by wire review 17 item 2); the result-format count (PortalSetResultFormat's 08P01) is checked at Describe or Execute, once the column count is known (E5-QUEUE R1); a result format code other than 0 or 1 is refused (22023) when a row is formatted, as in PostgreSQL. A prepared statement's parameter and result types are fixed at its first Describe or Execute (PostgreSQL fixes them at Parse); a later Describe or Execute that finds others (after `turso_branch_switch`, a DDL or a search_path change) is refused 0A000, "cached plan must not change result type" for the result, where PostgreSQL refuses a changed result type the same way but re-plans a changed parameter type with the fixed one, so a parameter whose new type it can coerce runs there and is refused here; one it cannot coerce is refused there at Bind, before BindComplete, with the re-analysis's own error (42883 for an operator with no match; PostgreSQL's exec_bind_message revalidates the plan in GetCachedPlan before it sends BindComplete, read in its source, the code unrecorded), and here at the Describe, or at the Execute after BindComplete, 0A000 "cached plan must not change the type of parameter $n". No COPY sub-protocol, CancelRequest, or NotificationResponse |

## Platforms

Not applicable: the Turso Postgres frontend is Rust and builds on every
platform Turso supports; PostgreSQL's platform/compiler feature entries do
not carry over.

## PostgreSQL 18.6 recorded divergences

These are the commands the `tests/pg18` replays register: each `recorded::replay` divergence in `postgres/cli/tests/wire_sessions.rs` cites one ID below, and a command that differs only because an earlier one did takes that earlier cause's ID. ❌ = we answer differently from PostgreSQL and should not; 🟡 = an intended refusal, a message-text difference with the same SQLSTATE, or a difference that comes from the recording's cluster or harness. Source: run `c33-replay-wire-05b866917`, 412 divergences in 86 recordings.

| ID | Divergence | Status | Owner | Recordings |
|---|---|---|---|---|
| `PGDIV-INT-RANGE` | PG raises 22003 (integer/bigint/smallint out of range) on int2/int4/int8 overflow in arithmetic, casts, abs() and INSERT; we return a float (`9.223372036854776e+18`), a clamped value (`9223372036854775807`), or the out-of-range integer (`2147483648`), and an overflow inside a block leaves it open so COMMIT commits what PG rolls back. Silent wrong answer. | ❌ | engine | `pg18-c33-intrange.out` #4-#19; `pg18-c33-engineerr.out` #8-#11, #14-#17, #19, #20, #23, #25-#30 |
| `PGDIV-DIV-ZERO-NULL` | PG raises 22012 division by zero for `1/0`, `1 % 0` and NUMERIC `x / 0`; we return one NULL row (NUMERIC typed 701). Silent wrong answer. | ❌ | engine | `pg18-audit3-a_bare_numeric_division_by_zero_is_22012.out` #4; `pg18-audit3-integer_division_by_zero_is_22012.out` #2; `pg18-audit3-integer_modulo_by_zero_is_22012.out` #2 |
| `PGDIV-REAL-RANGE` | PG raises 22003 out of range for type real when a value overflows or underflows float4 (insert, CAST, `::real`); we store or return `Infinity`, `-Infinity` or `0`. Silent wrong answer. | ❌ | engine | `pg18-c33.out` #25-#31; `pg18-float.out` #21, #22 |
| `PGDIV-NUMERIC-AS-FLOAT` | PG answers exact numeric (1700) for integer literals past int8 (`9223372036854775808`, `0x_7FFF_FFFF_FFFF_FFFF_F`) and for a numeric parameter `1e-400`; we answer a rounded float8 (`1.4757395258967641e+20`) or `0`. Silent wrong answer. | ❌ | engine | `pg18-audit1-lib-parameters_decode_at_postgresqls_edges.out` #12; `pg18-num.out` #4; `pg18-c33.out` #13, #17 |
| `PGDIV-NEG-ZERO` | PG answers `0.0`/`0` for numeric `-0.0` and `'-0'::numeric`, and `-0` for `-0.0::float8`; we answer `-0` typed 701 for the numeric forms and `0` for the float8 form. Silent wrong answer. | ❌ | engine | `pg18-c33.out` #19, #20, #22; `pg18-float.out` #27 |
| `PGDIV-SUM-INT8-NUMERIC` | PG `sum(bigint)` is numeric and answers `9223372036854775808`; we raise 22003 `integer overflow` (aborting an open block). | ❌ | engine | `pg18-c33-intrange.out` #20; `pg18-c33-engineerr.out` #7; `pg18.out` #14, #15 |
| `PGDIV-AVG-SCALE` | PG `avg(int)` answers `1.5000000000000000`; we answer `1.5` (type 1700 agrees). | ❌ | engine | `pg18-audit1-ws-aggregates_have_postgres_types_on_both_protocols.out` #6, #7; `pg18-audit2-aggregates_report_numeric_types.out` #4 |
| `PGDIV-BOOL-TEXT` | PG answers bool (16) `t`/`f` for `EXISTS(...)` in `DELETE ... RETURNING` and for `pg_attribute.attnotnull`; we answer `1`/`0` typed 25 or 23. Silent wrong answer. | ❌ | wire | `pg18-audit1-ws-delete_using_returning_resolves_a_bare_name_over_both.out` #12, #22; `pg18-audit3-primary_key_columns_are_reported_not_null.out` #7; `pg18.out` #37, #39, #41 |
| `PGDIV-ARRAY-RESULT-BLOB` | PG returns a text-format bound `text[]`/`int[]` parameter as its array literal (`{a,b}`, `{NULL}`); we return our internal array encoding rendered as bytea hex (`\x030f0f6162`). Type oid agrees. | ❌ | wire | `pg18-arrres.out` #1-#3; `pg18-r15.out` #33, #34, #37 |
| `PGDIV-DESCRIBE-PARAM-TYPE` | PG Describe types the parameter from the compared column: 1028 `oid[]`, 19 `name` (`pg_class.relname`); we report 1007 `int4[]` and 25 `text`. | ❌ | wire | `pg18-arrays.out` #5; `pg18-audit1-ws-a_three_part_reference_resolves_through_its_schema.out` #3, #4 |
| `PGDIV-DERIVED-COLUMN-BYTEA` | PG types a derived-table `count(*)` column 20 and a CTE integer column under `UNION ALL` 23; we send 17 (bytea) in RowDescription (values agree). | ❌ | wire | `pg18-audit1-ws-untyped_contexts_type_their_parameters.out` #18, #19; `pg18-audit1-ws-set_operations_keep_their_grouping_and_clauses.out` #13, #14 |
| `PGDIV-EXPR-TYPE-TEXT` | PG types window aggregates/`row_number()` 20, scalar subqueries, CASE, `length()` 23, `pg_database_size()` 20, `::varchar(3)` 1043; we fall back to 25 (text) in RowDescription (values agree). | ❌ | wire | `pg18-audit1-ws-untyped_contexts_type_their_parameters.out` #26; `pg18-audit1-ws-a_correlated_generate_series_column_is_scoped_to_its_select.out` #16; `pg18-audit2-char_n_is_blank_padded_as_in_postgres.out` #5; `pg18-audit2-pg_indexes_and_pg_database_size_answer.out` #5; `pg18-colnames.out` #5, #8; `pg18-gs.out` #8, #9; `pg18-nullorder.out` #9 |
| `PGDIV-NUMERIC-OID-FLOAT8` | PG types NUMERIC(p,s) columns and numeric literals (`1.5`) 1700; we send 701 (float8) in RowDescription (text agrees). | ❌ | wire | `pg18-audit2-alter_table_add_constraint_is_validated_and_atomic.out` #7, #10; `pg18-audit2-alter_table_add_constraint_takes_effect_and_keeps_the_rows.out` #6, #10; `pg18-colnames.out` #7 |
| `PGDIV-CATALOG-NAME-TYPES` | PG types catalog identifier columns 19 (`name`), `is_nullable` 1043, `current_schemas()` 1003, `partstrat` 18, `array_position` 23 in `pg_indexes`, `information_schema` and the pgbench probe; we send 25 (rows agree). | ❌ | wire | `pg18-audit2-pg_indexes_and_pg_database_size_answer.out` #3, #4; `pg18-audit2-information_schema_describes_tables_columns_and_keys.out` #3-#5; `pg18-audit2-pgbench_table_probe_runs.out` #2, #3; `pg18-audit3-primary_key_columns_are_reported_not_null.out` #6 |
| `PGDIV-INFOSCHEMA-NOTNULL-CHECK` | PG 18 lists NOT NULL constraints in `information_schema.table_constraints` as `CHECK` (4 rows, varchar 1043); we list only FOREIGN KEY and PRIMARY KEY (2 rows, text 25). | ❌ | wire | `pg18-audit2-information_schema_describes_tables_columns_and_keys.out` #6 |
| `PGDIV-ERROR-BEFORE-ROWDESC` | PG sends RowDescription (1700) before an execution-time error (22012 numeric division by zero, 22023 infinite `generate_series` bound); we send only the error (prefixed `Extension error: ` for generate_series). | ❌ | wire | `pg18-audit3-a_numeric_division_by_zero_is_22012.out` #4; `pg18-gsinf.out` #1, #3, #4 |
| `PGDIV-CHAR-OVERFLOW-CODE` | PG raises 22001 `value too long for type character(6)`; we raise XX000 `value too long for type character`. | ❌ | wire | `pg18-audit2-char_n_is_blank_padded_as_in_postgres.out` #6 |
| `PGDIV-RELATION-EXISTS-CODE` | PG raises 42P07 `relation "p" already exists`; we raise 42601 `Parse error: table p already exists`. | ❌ | wire | `pg18-audit2-a_refused_create_table_leaves_no_sequence.out` #5 |
| `PGDIV-WRONG-KIND-CODE` | PG raises 42809 `"v" is not a table` (a DROP TABLE naming a view, or DROP VIEW a table); we raise the same text as 42601 with the `Parse error: ` prefix. | ❌ | wire | `pg18-c53-dropnotice.out` #14 |
| `PGDIV-COLUMN-ALIAS-COUNT-CODE` | PG raises 42P10 `table "g" has 1 columns available but 2 columns specified`; we raise 42601 `too many column aliases specified for function generate_series`. | ❌ | wire | `pg18-audit1-ws-generate_series_may_read_the_row_it_joins.out` #7 |
| `PGDIV-PARAM-OVER-LIMIT-CODE` | PG refuses Describe of `id = $65536` at Parse with 42P18 `could not determine data type of parameter $1`; we send ParseComplete then 42P02 `there is no parameter $65536`. | ❌ | wire | `pg18-audit3-a_parameter_number_past_the_limit_is_refused.out` #8 |
| `PGDIV-GS-MIXED-TYPES` | PG raises 42883 `function generate_series(integer, double precision, integer) does not exist`; we accept the call and raise 22023 `stop value cannot be infinity`. | ❌ | wire | `pg18-gsinf.out` #2 |
| `PGDIV-SQLITE-FUNCTION-LEAK` | PG raises 42883 `function typeof(text) does not exist` at Parse; SQLite `typeof()` resolves for us and Describe fails 42P18. | ❌ | wire | `pg18-audit1-ws-untyped_contexts_type_their_parameters.out` #44 |
| `PGDIV-FUNC-ALIAS-CORRELATED` | PG resolves a bare function alias `g` inside a correlated `EXISTS` (`w.k = g`) and answers 3 rows; we raise 42703 `no such column: g`. | ❌ | wire | `pg18-audit1-ws-a_correlated_generate_series_column_is_scoped_to_its_select.out` #12 |
| `PGDIV-FULL-JOIN-USING` | PG answers `FULL JOIN ... USING (i)` and `NATURAL FULL JOIN` (4 rows); we raise 42601 `FULL OUTER JOIN requires an equality condition in the ON clause`. | ❌ | wire | `pg18-join2.out` #8, #10, #12 |
| `PGDIV-ROW-CONSTRUCTOR` | PG answers `SELECT ARRAY[1], ROW(1, 2), EXISTS (...), ...` (types 1007, 2249, 16, ...); we raise 42601 `row value misused`. | ❌ | wire | `pg18-colnames.out` #6 |
| `PGDIV-VIRTUAL-GENERATED` | PG creates `b int GENERATED ALWAYS AS (a * 2) VIRTUAL`; we raise 42601 syntax error at `virtual` (a refusal should be 0A000); later commands on the table fail 42P01. | ❌ | wire | `pg18-ident.out` #6, #7, #10 |
| `PGDIV-PG-TYPEOF` | PG answers `pg_typeof(random())::text` = `double precision`; we raise 42883 `no such function: pg_typeof`. | ❌ | wire | `pg18-random.out` #1 |
| `PGDIV-PG-SETTINGS` | PG answers `SELECT ... FROM pg_settings` (5 rows); we raise 42601 `no such object: pg_settings`. | ❌ | wire | `pg18-audit2-show_answers_from_the_settings_set_keeps.out` #15 |
| `PGDIV-PARSE-PREFIX` | Same SQLSTATE and text as PG, but our message starts with `Parse error: ` (or `Parse error: Invalid statement: `). | 🟡 | wire | `pg18-audit1-ws-a_dump_headers_settings_are_accepted.out` #20, #21; `pg18-audit1-ws-set_honours_or_refuses_each_parameter.out` #17, #18; `pg18-audit2-a_refused_create_table_leaves_no_sequence.out` #3; `pg18-audit1-ws-delete_using_reads_one_namespace.out` #8; `pg18-audit1-ws-a_correlated_generate_series_column_is_scoped_to_its_select.out` #20; `pg18-audit1-ws-delete_using_returning_resolves_a_bare_name_over_both.out` #6; `pg18-audit2-show_answers_from_the_settings_set_keeps.out` #11-#13; `pg18-audit3-a_branch_call_parameter_past_the_limit_is_refused.out` #6, #9, #12, #15, #18, #21; `pg18-audit3-a_mistyped_commit_or_rollback_aborts_the_block.out` #6, #11; `pg18-audit3-a_parameter_number_past_the_limit_is_refused.out` #4, #7; `pg18-audit3-a_parameter_number_the_scanner_would_wrap_is_refused.out` #6, #9, #12; `pg18-audit3-copy_refuses_an_option_it_does_not_implement.out` #15, #17; `pg18-audit3-engine_errors_carry_postgres_sqlstates.out` #14; `pg18-infcast.out` #3; `pg18-c33.out` #32; `pg18-insdef.out` #3, #4, #8; `pg18-join1b.out` #2; `pg18-short.out` #6; `pg18.out` #20, #21, #42 |
| `PGDIV-CONSTRAINT-MESSAGE` | Same SQLSTATE (23502/23503/23505/23514); PG names the relation and constraint (`duplicate key value violates unique constraint "t_pkey"`, `... violates foreign key constraint "c_pid_fkey"`, `could not create unique index "d_v_key"`); we send SQLite wording (`UNIQUE constraint failed: t.id`, `FOREIGN KEY constraint failed`, `deferred foreign key constraint failed on commit`, `CHECK constraint failed: v > 0`). | 🟡 | wire | `pg18-audit2-a_failed_commit_leaves_the_session_idle.out` #5; `pg18-audit2-an_error_inside_a_transaction_aborts_it_as_in_postgres.out` #5; `pg18-audit2-rollback_to_savepoint_recovers_a_failed_transaction.out` #6; `pg18-audit2-foreign_keys_are_enforced_on_the_trunk_and_on_a_branch.out` #5, #6; `pg18-audit2-alter_table_add_constraint_is_validated_and_atomic.out` #8; `pg18-audit3-a_deferred_foreign_key_is_checked_at_commit.out` #7, #15; `pg18-audit3-a_multi_statement_query_is_one_implicit_transaction.out` #4, #10; `pg18-audit2-alter_table_add_constraint_takes_effect_and_keeps_the_rows.out` #11-#14; `pg18-audit3-an_index_on_a_qualified_table_is_that_tables.out` #12; `pg18-audit3-primary_key_columns_are_reported_not_null.out` #5; `pg18-audit3-engine_errors_carry_postgres_sqlstates.out` #8-#11; `pg18-audit2-alter_table_add_constraint_works_in_every_transaction_state.out` #7, #14, #22, #30, #35, #42, #50, #58, #63, #70, #78, #86, #91, #98, #106, #114, #117 |
| `PGDIV-SQLITE-NAME-MESSAGE` | Same SQLSTATE (42P01/42703/42702); PG says `relation "x" does not exist`, `column "x" does not exist`, `column reference "x" is ambiguous`; we say `Parse error: no such table: x` (`main.t` for `public.t`), `no such column`, `ambiguous column name`. | 🟡 | wire | `pg18-audit1-ws-delete_using_reads_one_namespace.out` #6; `pg18-audit3-an_explicit_public_qualifier_names_public.out` #15; `pg18-audit3-engine_errors_carry_postgres_sqlstates.out` #12, #13; `pg18-gs.out` #7, #10; `pg18-r14.out` #10; `pg18-r15.out` #27; `pg18-c53-dropnotice.out` #24 |
| `PGDIV-COPY-CSV-OPTION-TEXT` | Same 0A000; PG says `COPY QUOTE requires CSV mode` / `COPY FORCE_NOT_NULL requires CSV mode`; we say `COPY option "quote" is not supported`. | 🟡 | wire | `pg18-audit3-copy_refuses_an_option_it_does_not_implement.out` #3, #5 |
| `PGDIV-SET-REFUSED` | PG accepts `SET` of xmloption, transaction_timeout, geqo_threshold, client_encoding LATIN1, standard_conforming_strings off, DateStyle (and SHOW geqo_threshold), and rejects bad default_tablespace / default_table_access_method values with 22023; we refuse all of them with 0A000. | 🟡 | ours-only, wire | `pg18-audit1-ws-a_dump_headers_settings_are_accepted.out` #14-#19; `pg18-audit1-ws-set_honours_or_refuses_each_parameter.out` #15, #16; `pg18-audit2-set_reads_every_value_it_is_given.out` #3 |
| `PGDIV-READ-ONLY-TX` | PG honours `BEGIN READ ONLY`, `START TRANSACTION READ ONLY`, `SET [LOCAL] transaction_read_only`, `default_transaction_read_only` (then 25006 on INSERT); we refuse with 0A000, which aborts an open block (later 25P02/25P01 differences cascade). | 🟡 | ours-only | `pg18-audit1-ws-a_chained_or_read_only_transaction_is_refused.out` #12-#15; `pg18-audit1-ws-a_read_only_begin_inside_a_block_is_refused.out` #5, #6, #8-#10; `pg18-audit1-ws-set_honours_or_refuses_each_parameter.out` #5, #6, #9, #10 |
| `PGDIV-COMMIT-AND-CHAIN` | PG commits and opens a new block on `COMMIT AND CHAIN`; we refuse 0A000 and abort the block, so its rows are rolled back. | 🟡 | ours-only | `pg18-audit1-ws-a_chained_or_read_only_transaction_is_refused.out` #9, #11 |
| `PGDIV-DELETE-USING-RETURNING` | PG lets `DELETE ... USING ... RETURNING` return USING-relation columns, `j.*`, `*` or a whole-row `k`; we refuse 0A000, so the rows PG deleted remain and later counts and deletes differ. | 🟡 | ours-only | `pg18-audit1-ws-delete_using_returning_resolves_a_bare_name_over_both.out` #8-#11, #13, #16, #19; `pg18-audit1-ws-delete_using_returning_a_using_column_is_refused.out` #6-#12, #15, #18 |
| `PGDIV-COPY-OPTION-REFUSED` | PG accepts COPY options DEFAULT, ON_ERROR, ENCODING (and checks HEADER MATCH, 22P04); we refuse them 0A000, so later row counts differ. | 🟡 | ours-only | `pg18-audit3-copy_refuses_an_option_it_does_not_implement.out` #7-#14, #16, #18, #20 |
| `PGDIV-WITH-ORDINALITY` | PG answers `generate_series(1, 2) WITH ORDINALITY` (int4, int8); we refuse 0A000. | 🟡 | ours-only | `pg18-audit1-ws-generate_series_may_read_the_row_it_joins.out` #6; `pg18-audit3-engine_errors_carry_postgres_sqlstates.out` #15 |
| `PGDIV-NAN` | PG answers `'NaN'::float8`, `'nan'::numeric` and NaN comparisons; we refuse 0A000 (the engine holds no NaN). | 🟡 | ours-only | `pg18-infcast.out` #2; `pg18-inf.out` #1; `pg18-colnames.out` #4 |
| `PGDIV-INHERITS` | PG creates `CREATE TABLE ... INHERITS (ipa)` and shows child rows through the parent; we refuse 0A000, and later commands on the child fail 42P01. | 🟡 | ours-only | `pg18-inherits.out` #3-#7 |
| `PGDIV-SHOW-ALL` | PG answers `SHOW ALL` with every setting (including cluster paths and port); we refuse 0A000. | 🟡 | ours-only | `pg18-audit2-show_answers_from_the_settings_set_keeps.out` #14 |
| `PGDIV-VACUUM` | PG raises 25001 `VACUUM cannot run inside a transaction block` in a multi-statement query; we raise 0A000 `VACUUM is not supported`. | 🟡 | ours-only | `pg18-audit3-a_multi_statement_query_is_one_implicit_transaction.out` #8 |
| `PGDIV-TURSO-FUNCTIONS` | PG has no `turso_branch_*` functions and raises 42883 `function ... does not exist`; we run our function and raise its own error (42P02, 3D000, 25001). | 🟡 | ours-only | `pg18-audit3-a_branch_call_parameter_past_the_limit_is_refused.out` #7; `pg18-audit3-a_parameter_number_the_scanner_would_wrap_is_refused.out` #5; `pg18-audit3-a_multi_statement_query_is_one_implicit_transaction.out` #6 |
| `PGDIV-PUBLIC-SCHEMA-NOOP` | The recording resets public with `DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public`. We drop tables, but CREATE SCHEMA public is unconditionally refused 42601 instead of recreating it. Frontend defect (C56). | ❌ | wire | `pg18-audit1-lib-parameters_decode_at_postgresqls_edges.out` #1; `pg18-audit1-lib-a_bytea_parameter_with_a_non_ascii_digit_is_refused.out` #1; `pg18-audit1-ws-a_dump_headers_settings_are_accepted.out` #1; `pg18-audit1-ws-a_chained_or_read_only_transaction_is_refused.out` #1; `pg18-audit1-ws-a_bytea_parameter_with_a_non_ascii_digit_is_refused.out` #1; `pg18-audit1-ws-a_parameter_over_the_simple_protocol_is_refused.out` #1; `pg18-audit1-ws-a_read_only_begin_inside_a_block_is_refused.out` #1; `pg18-audit1-ws-a_three_part_reference_resolves_through_its_schema.out` #1; `pg18-audit1-ws-a_parameter_is_typed_by_every_kind_of_from_item.out` #1; `pg18-audit1-ws-aggregates_have_postgres_types_on_both_protocols.out` #1; `pg18-audit1-ws-generate_series_may_read_the_row_it_joins.out` #1; `pg18-audit1-ws-checkpoint_inside_a_block_keeps_the_block.out` #1; `pg18-audit1-ws-set_honours_or_refuses_each_parameter.out` #1; `pg18-audit1-ws-untyped_contexts_type_their_parameters.out` #1; `pg18-audit1-ws-delete_using_reads_one_namespace.out` #1; `pg18-audit1-ws-a_correlated_generate_series_column_is_scoped_to_its_select.out` #1; `pg18-audit1-ws-delete_using_deletes_only_the_joined_rows.out` #1; `pg18-audit1-ws-delete_using_returning_resolves_a_bare_name_over_both.out` #1; `pg18-audit1-ws-delete_using_returning_a_using_column_is_refused.out` #1; `pg18-audit1-ws-set_operations_keep_their_grouping_and_clauses.out` #1 |
| `PGDIV-SCHEMA-REFUSED` | PostgreSQL accepts a second schema; the branch server intentionally refuses schemas other than public with 0A000. | 🟡 | ours-only | `pg18-audit3-a_bare_numeric_division_by_zero_is_22012.out` #1; `pg18-audit3-a_branch_call_parameter_past_the_limit_is_refused.out` #1; `pg18-audit3-a_deferred_foreign_key_is_checked_at_commit.out` #1; `pg18-audit3-a_from_functions_arguments_read_the_items_before_it.out` #1; `pg18-audit3-a_mistyped_commit_or_rollback_aborts_the_block.out` #1; `pg18-audit3-a_multi_statement_query_is_one_implicit_transaction.out` #1; `pg18-audit3-a_numeric_division_by_zero_is_22012.out` #1; `pg18-audit3-a_parameter_is_typed_from_the_relation_the_engine_writes.out` #1, #4; `pg18-audit3-a_parameter_number_past_the_limit_is_refused.out` #1; `pg18-audit3-a_parameter_number_the_scanner_would_wrap_is_refused.out` #1; `pg18-audit3-a_view_over_public_t_types_its_parameters.out` #1; `pg18-audit3-an_explicit_public_qualifier_names_public.out` #1, #3; `pg18-audit3-an_index_on_a_qualified_table_is_that_tables.out` #1, #3; `pg18-audit3-an_undeclared_parameter_has_its_inferred_type_at_describe_and_bind.out` #1; `pg18-audit3-an_unqualified_relation_is_resolved_as_the_engine_resolves_it.out` #1, #4; `pg18-audit3-copy_from_runs_inside_the_block_it_is_in.out` #1; `pg18-audit3-copy_refuses_an_option_it_does_not_implement.out` #1; `pg18-audit3-engine_errors_carry_postgres_sqlstates.out` #1; `pg18-audit3-every_parameter_in_the_text_is_a_parameter.out` #1; `pg18-audit3-having_without_group_by_filters_the_aggregate_row.out` #1; `pg18-audit3-integer_division_by_zero_is_22012.out` #1; `pg18-audit3-integer_modulo_by_zero_is_22012.out` #1; `pg18-audit3-primary_key_columns_are_reported_not_null.out` #1; `pg18.out` #6; `pg18-c53-dropnotice.out` #9 |
| `PGDIV-MISSING-SCHEMA-CODE` | Commands on the refused schema then answer XX000 `no such database: s`; a missing schema must use 3F000. The cascade is a frontend defect (C56). | ❌ | wire | `pg18-audit3-a_parameter_is_typed_from_the_relation_the_engine_writes.out` #5, #6, #7, #8, #9, #10, #11, #12, #15; `pg18-audit3-an_explicit_public_qualifier_names_public.out` #4, #5, #11, #16; `pg18-audit3-an_index_on_a_qualified_table_is_that_tables.out` #4, #5, #6, #7, #13; `pg18-audit3-an_unqualified_relation_is_resolved_as_the_engine_resolves_it.out` #5; `pg18.out` #7 |
| `PGDIV-CLUSTER-SETTINGS` | The recording cluster set DateStyle `ISO, YMD` and TimeZone `America/Toronto` in its config file; we answer our defaults `ISO, MDY` and `UTC` (tag also differs, PGDIV-SHOW-TAG). | 🟡 | cluster | `pg18-audit2-show_answers_from_the_settings_set_keeps.out` #6, #8; `pg18-c53-dropnotice.out` #17 |
| `PGDIV-RANDOM-VALUE` | The recording compares one `random()` draw (`0.812971034057977`), which no replay can reproduce; type 701 agrees. Harness, not server. | 🟡 | harness | `pg18-random.out` #2 |


| `PGDIV-PARSE-REFUSAL-STEP` | PG refuses at Parse (no ParseComplete) for parameter-number errors (42601/42P02/42P18), unknown relations/functions and invalid FROM references; we send ParseComplete (and BindComplete) first, then the error, usually with a `Parse error: ` prefix and SQLite wording (`missing FROM-clause entry`, `no such table`). | ❌ | wire | `pg18-audit1-ws-a_three_part_reference_resolves_through_its_schema.out` #5; `pg18-audit1-ws-untyped_contexts_type_their_parameters.out` #45; `pg18-audit3-a_branch_call_parameter_past_the_limit_is_refused.out` #4, #10, #13, #16, #19; `pg18-audit3-a_parameter_is_typed_from_the_relation_the_engine_writes.out` #18; `pg18-audit3-a_parameter_number_past_the_limit_is_refused.out` #5; `pg18-audit3-a_parameter_number_the_scanner_would_wrap_is_refused.out` #4, #7, #10, #13; `pg18-audit3-an_unqualified_relation_is_resolved_as_the_engine_resolves_it.out` #6; `pg18.out` #25, #31, #32 |
