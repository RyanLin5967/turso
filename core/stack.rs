macro_rules! trace_stack {
    () => {
        let _stack = $crate::stack::trace_scope(concat!("line:", line!()));
    };
    ($label:expr $(,)?) => {
        let _stack = $crate::stack::trace_scope($label);
    };
    ($label:expr, $detail:expr $(,)?) => {
        let _stack = $crate::stack::trace_scope_with_detail($label, $detail);
    };
}
pub(crate) use trace_stack;

#[cfg(feature = "stacker")]
pub(crate) struct TraceGuard {
    label: &'static str,
    detail: Option<&'static str>,
}

#[cfg(feature = "stacker")]
impl TraceGuard {
    fn emit(&self, phase: &'static str) {
        if std::env::var_os("TURSO_TRACE_STACK").is_none() {
            return;
        }

        tracing::debug!(
            target: "turso_stack",
            label = self.label,
            detail = self.detail,
            phase,
            remaining_stack = stacker::remaining_stack(),
            "stacker remaining stack"
        );
    }
}

#[cfg(feature = "stacker")]
impl Drop for TraceGuard {
    fn drop(&mut self) {
        self.emit("exit");
    }
}

#[cfg(feature = "stacker")]
pub(crate) fn trace_scope(label: &'static str) -> TraceGuard {
    let guard = TraceGuard {
        label,
        detail: None,
    };
    guard.emit("enter");
    guard
}

#[cfg(feature = "stacker")]
pub(crate) fn trace_scope_with_detail(label: &'static str, detail: &'static str) -> TraceGuard {
    let guard = TraceGuard {
        label,
        detail: Some(detail),
    };
    guard.emit("enter");
    guard
}

#[cfg(feature = "stacker")]
pub(crate) fn trace_remaining(label: &'static str) {
    if std::env::var_os("TURSO_TRACE_STACK").is_none() {
        return;
    }

    tracing::debug!(
        target: "turso_stack",
        label,
        phase = "sample",
        remaining_stack = stacker::remaining_stack(),
        "stacker remaining stack"
    );
}

#[cfg(not(feature = "stacker"))]
#[inline]
pub(crate) fn trace_scope(_label: &'static str) {}

#[cfg(not(feature = "stacker"))]
#[inline]
pub(crate) fn trace_scope_with_detail(_label: &'static str, _detail: &'static str) {}

#[cfg(not(feature = "stacker"))]
#[inline]
pub(crate) fn trace_remaining(_label: &'static str) {}

/// One recursive step of a nested query (a view's expansion, a subquery's planning, optimizing or
/// emission) in progress: `turso_stack_guard::NestGuard`, the one primitive the parser, the engine
/// and the PostgreSQL translator share. A refusal is `LimboError::StackDepthLimitExceeded`,
/// PostgreSQL's 54001. Leaving it, on every exit, pops it.
pub(crate) struct NestGuard(#[allow(dead_code)] turso_stack_guard::NestGuard);

impl NestGuard {
    /// `what` names the step for a reader of the call site; PostgreSQL's message names none.
    #[inline]
    #[track_caller]
    pub(crate) fn enter(_what: &'static str) -> crate::Result<Self> {
        turso_stack_guard::NestGuard::enter()
            .map(Self)
            .map_err(|_| crate::LimboError::StackDepthLimitExceeded)
    }
}

/// A statement stepping on this thread (`Program::step`): a statement prepared meanwhile (a schema
/// reparse, a callback running SQL, a trigger compiled at its first fire) is checked from its
/// outermost step.
pub(crate) use turso_stack_guard::Executing;

#[cfg(test)]
mod tests {
    use turso_stack_guard::test_hooks;

    /// The stack between two consecutive nested checks, over views, FROM subqueries, WHERE and
    /// scalar subqueries and a mix of them, stays under LEVEL_COST (this test, in
    /// debug on an 8 MiB thread, measured 73,584 B at most). A lower bound on a level's cost.
    #[test]
    fn two_consecutive_nested_checks_are_less_than_one_level_cost_apart() {
        std::thread::Builder::new()
            .stack_size(8 << 20)
            .spawn(|| {
                let io: std::sync::Arc<dyn crate::IO> = std::sync::Arc::new(crate::MemoryIO::new());
                let db = crate::Database::open_file(io, ":memory:", std::sync::Arc::new(crate::SqliteDialect)).unwrap();
                let conn = db.connect().unwrap();
                conn.execute("CREATE TABLE t(a INTEGER)").unwrap();
                conn.execute("INSERT INTO t VALUES (1)").unwrap();
                conn.execute("CREATE VIEW v0 AS SELECT a FROM t").unwrap();
                for i in 1..12 {
                    conn.execute(format!("CREATE VIEW v{i} AS SELECT a FROM v{}", i - 1)).unwrap();
                }
                let mut from = "SELECT a FROM t".to_string();
                let mut scalar = "SELECT a FROM t".to_string();
                let mut mixed = "SELECT a FROM v3".to_string();
                for _ in 0..12 {
                    from = format!("SELECT a FROM ({from})");
                    scalar = format!("SELECT a FROM t WHERE a = ({scalar})");
                    mixed = format!("SELECT a FROM ({mixed}) WHERE a IN (SELECT a FROM v2 WHERE a = ({mixed}))");
                }
                test_hooks::arm(true);
                test_hooks::take_worst();
                let mut over = Vec::new();
                for sql in ["SELECT a FROM v11".to_string(), from, scalar, mixed] {
                    conn.prepare(&sql).unwrap().run_collect_rows().unwrap();
                    let (worst, at) = test_hooks::take_worst_at();
                    println!("FE_LEVEL_GAP {worst} B {:?}: {}", at.map(|(o, i)| format!("{o} -> {i}")), &sql[..sql.len().min(40)]);
                    if worst == 0 || worst >= turso_stack_guard::LEVEL_COST {
                        let at = at.map(|(o, i)| format!("{o} -> {i}"));
                        over.push(format!("{worst} B between two checks {at:?}: {}", &sql[..sql.len().min(60)]));
                    }
                }
                test_hooks::arm(false);
                assert!(over.is_empty(), "CLAIM: checks further apart than LEVEL_COST {}: {over:?}", turso_stack_guard::LEVEL_COST);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// Under INSERT, UPDATE and DELETE the first subquery's planning and emission once held no
    /// outer guard, so its guard was a statement's outermost step, exempt from the check: one level
    /// ran unchecked on whatever stack was left. Each statement's prepare must enter exactly its
    /// two outermost steps (the parser's `parse_stmt` and `translate_inner`), so every subquery
    /// step is nested and checked. Mutant `nest_statement_unguarded` (no guard in translate_inner)
    /// must turn it red.
    #[test]
    fn a_dml_statements_first_subquery_is_a_nested_step() {
        let io: std::sync::Arc<dyn crate::IO> = std::sync::Arc::new(crate::MemoryIO::new());
        let db = crate::Database::open_file(io, ":memory:", std::sync::Arc::new(crate::SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(a INTEGER)").unwrap();
        let mut bad = Vec::new();
        test_hooks::arm(true);
        for sql in [
            "SELECT (SELECT 1)",
            "INSERT INTO t SELECT (SELECT 1)",
            "INSERT INTO t VALUES ((SELECT 1))",
            "UPDATE t SET a = (SELECT 1)",
            "UPDATE t SET a = 1 WHERE a IN (SELECT 1)",
            "DELETE FROM t WHERE a IN (SELECT 1)",
            "DELETE FROM t WHERE a = (SELECT (SELECT 1))",
        ] {
            test_hooks::take_outermost();
            let prepared = conn.prepare(sql).map(|_| ());
            let outermost = test_hooks::take_outermost();
            if prepared.is_err() || outermost != 2 {
                bad.push(format!("{sql}: {outermost} outermost steps ({prepared:?})"));
            }
        }
        test_hooks::arm(false);
        assert!(bad.is_empty(), "CLAIM: a statement's subquery step was entered as an outermost, unchecked step: {bad:?}");
    }

    /// A connection with no temp database still answers for its temp schema where the stack
    /// guard refuses every step: both callers that build it (`Connection::empty_temp_schema` and
    /// the resolver's lookup of `temp`) cannot return an error, so building it parses nothing (it
    /// parsed the built-in definitions, and a refusal there was an `.expect` that aborted the
    /// process).
    #[test]
    fn the_temp_schema_is_built_where_the_stack_guard_refuses_every_step() {
        #[inline(never)]
        fn below_margin(conn: &std::sync::Arc<crate::Connection>) -> (usize, usize) {
            if turso_stack_guard::remaining_stack() >= turso_stack_guard::margin() {
                let mut frame = [0u8; 1 << 10];
                std::hint::black_box(&mut frame);
                return below_margin(conn);
            }
            let _executing = turso_stack_guard::Executing::enter();
            assert!(turso_stack_guard::NestGuard::enter().is_err(), "premise: a step here is refused");
            let built = conn.empty_temp_schema().tables.len();
            let syms = conn.syms.read();
            let schema = conn.schema.read().clone();
            let resolver = crate::translate::emitter::Resolver::new(
                &schema,
                conn.database_schemas(),
                &conn.temp.database,
                conn.attached_databases(),
                &syms,
                conn.experimental_custom_types_enabled(),
                conn.get_dqs_dml().into(),
                conn.dialect(),
                conn.db.empty_schemas.dialect.clone(),
                &None,
            );
            let resolved = resolver.with_schema(crate::TEMP_DB_ID, |temp| temp.tables.len());
            (built, resolved)
        }
        std::thread::Builder::new()
            .stack_size(2 << 20)
            .spawn(|| {
                let io: std::sync::Arc<dyn crate::IO> = std::sync::Arc::new(crate::MemoryIO::new());
                let db = crate::Database::open_file(io, ":memory:", std::sync::Arc::new(crate::SqliteDialect)).unwrap();
                let conn = db.connect().unwrap();
                assert!(conn.temp.database.read().is_none(), "premise: the connection has no temp database");
                let (built, resolved) = below_margin(&conn);
                assert!(built > 0 && resolved > 0, "the temp schema lost its built-in tables: {built}, {resolved}");
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// A real re-entry: a CREATE TABLE's schema reparse prepares a statement inside
    /// `Program::step` (op_parse_schema), and that statement's outermost step is seen as entered
    /// while a statement steps; a flat SELECT is not.
    #[test]
    fn a_schema_reparse_inside_a_step_is_a_reentered_prepare() {
        let io: std::sync::Arc<dyn crate::IO> = std::sync::Arc::new(crate::MemoryIO::new());
        let db = crate::Database::open_file(io, ":memory:", std::sync::Arc::new(crate::SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        test_hooks::arm(true);
        test_hooks::take_reentered();
        conn.execute("CREATE TABLE r(a INTEGER)").unwrap();
        let reparse = test_hooks::take_reentered();
        conn.execute("SELECT a FROM r").unwrap();
        let flat = test_hooks::take_reentered();
        test_hooks::arm(false);
        assert!(reparse > 0, "the schema reparse's prepare was not seen inside a step");
        assert_eq!(flat, 0, "a flat SELECT counted as re-entered");
    }
}
