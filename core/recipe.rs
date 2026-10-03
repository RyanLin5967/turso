//! BASE control stub (lane k1-recipe-build, artie-research
//! `frontier/round14/k1-recipe-build/PREREG.md` §3): the recipe harness's API with no engine
//! change, so the unmodified r13-c8 engine (01104be62) runs the same harness as the EAGER arm.
//! Every counter reads 0 here; the BASE-vs-EAGER comparison uses only the counters both builds
//! have (owned pages, arena slots, PAGE_IO, result hashes).

pub mod counter {
    pub const PAGE_FETCH: usize = 0;
    pub const RECIPE_EVALS: usize = 1;
    pub const STALE_READS: usize = 2;
    pub const INSTALLED: usize = 3;
    pub const FALLBACKS: usize = 4;
    pub const BRANCH_DIRTY: usize = 5;
    pub const TRUNK_DIRTY: usize = 6;
    pub const COMPILES: usize = 7;
    pub const CACHE_HITS: usize = 8;
    pub const EMPTY_MATCH: usize = 9;
}

pub fn recipe_io() -> [u64; 10] {
    [0; 10]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mutant {
    None,
}

pub fn mutant() -> Mutant {
    Mutant::None
}

impl crate::Connection {
    /// The base engine has no recipe backfill: only `false` is accepted.
    pub fn set_recipe_backfill(&self, value: bool) {
        assert!(!value, "BASE build: recipe backfill does not exist here");
    }
}

#[doc(hidden)]
pub fn debug_recipe_count(
    _conn: &std::sync::Arc<crate::Connection>,
    _table: &str,
) -> (Option<usize>, Option<usize>) {
    (None, None)
}
