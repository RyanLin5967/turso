//! Recipe backfill harness (lane k1-recipe-build; artie-research
//! `frontier/round14/k1-recipe-build/PREREG.md`). Integer counters only, except the `ns_*` fields,
//! which are secondary (PREREG §10) and unlocked unless the caller holds the fleet lock.
//!
//!   branch_recipe diff  --seeds S [--start S0] --ops K --dir DIR [--no-sqlite]
//!   branch_recipe bench --workload sd|dc|repair|fr --arm eager|recipe|lazy --n N --v V
//!                       --shape fan|tree|chain --db PATH [--points P] [--sample B] [--seed X]
//!
//! `diff` is KC1: one seeded op stream over a tree of branches runs on a RECIPE database and an
//! EAGER database of this binary (and on stock SQLite, one file per branch copied at fork). After
//! every op the touched branch's full content and changes() must agree; every 25 ops every live
//! branch is compared. A RECIPE-vs-EAGER difference prints `MISMATCH` and the run exits 3. With
//! `K1_RECIPE_MUTANT=M1..M8` set, the run must find a difference (the fire-check).
//!
//! `bench` loads the CH-benCHmark customer table (PREREG §5) on the trunk, then forks V branches in
//! the given shape and runs one workload step per branch, printing one `BR` line per branch with
//! its owned pages and the per-statement counters, a `PT` line per sampled branch for point reads,
//! and a `SUMMARY` line.

use std::collections::{BTreeMap, HashSet};
use std::io::Write;
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use turso_core::branch::{Branch, BranchDurability};
use turso_core::recipe::{counter, recipe_io};
use turso_core::{
    Connection, Database, DatabaseOpts, Numeric, OpenFlags, PlatformIO, SqliteDialect, Value,
    ValueRef, IO,
};

fn die(msg: &str) -> ! {
    eprintln!("branch_recipe: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    let _ = std::io::stdout().flush();
    std::process::exit(1)
}

#[derive(Clone)]
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        // splitmix64
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        lo + self.below((hi - lo + 1) as u64) as i64
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len() as u64) as usize]
    }
    fn alpha(&mut self, len: usize) -> String {
        (0..len)
            .map(|_| (b'a' + self.below(26) as u8) as char)
            .collect()
    }
}

fn open_db(path: &Path, durability: BranchDurability) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new().with_branch_durability(durability),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap_or_else(|e| not_a_result(&format!("open {} failed: {e}", path.display())))
}

fn rows(conn: &Arc<Connection>, sql: &str) -> turso_core::Result<Vec<Vec<Value>>> {
    conn.prepare(sql)?.run_collect_rows()
}

fn int(conn: &Arc<Connection>, sql: &str) -> i64 {
    let r = rows(conn, sql).unwrap_or_else(|e| not_a_result(&format!("{sql}: {e}")));
    r.first()
        .and_then(|r| r.first())
        .and_then(|v| v.as_int())
        .unwrap_or_else(|| not_a_result(&format!("{sql}: no integer in {r:?}")))
}

fn exec(conn: &Arc<Connection>, sql: &str) {
    conn.execute(sql)
        .unwrap_or_else(|e| not_a_result(&format!("{sql}: {e}")));
}

/// A value in a form both engines' results compare in: storage class plus exact bits.
#[derive(Debug, Clone, PartialEq)]
enum Cv {
    Null,
    Int(i64),
    Real(u64),
    Text(String),
    Blob(Vec<u8>),
}

fn cv_turso(v: &Value) -> Cv {
    match v.as_ref() {
        ValueRef::Null => Cv::Null,
        ValueRef::Numeric(Numeric::Integer(i)) => Cv::Int(i),
        ValueRef::Numeric(Numeric::Float(f)) => Cv::Real(f64::from(f).to_bits()),
        ValueRef::Text(t) => Cv::Text(t.as_str().to_string()),
        ValueRef::Blob(b) => Cv::Blob(b.to_vec()),
    }
}

fn cv_sqlite(v: rusqlite::types::Value) -> Cv {
    match v {
        rusqlite::types::Value::Null => Cv::Null,
        rusqlite::types::Value::Integer(i) => Cv::Int(i),
        rusqlite::types::Value::Real(f) => Cv::Real(f.to_bits()),
        rusqlite::types::Value::Text(s) => Cv::Text(s),
        rusqlite::types::Value::Blob(b) => Cv::Blob(b),
    }
}

fn turso_rows(conn: &Arc<Connection>, sql: &str) -> Result<Vec<Vec<Cv>>, String> {
    rows(conn, sql)
        .map(|rs| rs.iter().map(|r| r.iter().map(cv_turso).collect()).collect())
        .map_err(|e| e.to_string())
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<Cv>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    let mut out = Vec::new();
    let mut rs = stmt.query([]).map_err(|e| e.to_string())?;
    while let Some(row) = rs.next().map_err(|e| e.to_string())? {
        let mut r = Vec::with_capacity(n);
        for i in 0..n {
            r.push(cv_sqlite(row.get::<_, rusqlite::types::Value>(i).map_err(|e| e.to_string())?));
        }
        out.push(r);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// diff (KC1)
// ---------------------------------------------------------------------------------------------

#[derive(Clone)]
struct Col {
    name: String,
    ty: &'static str,
}

struct Node {
    parent: Option<usize>,
    alive: bool,
    /// None for the trunk, whose connections live in `Diff`.
    br_r: Option<Branch>,
    br_e: Option<Branch>,
    c_r: Arc<Connection>,
    c_e: Arc<Connection>,
    sq: Option<rusqlite::Connection>,
    sq_path: PathBuf,
    cols: Vec<Col>,
    indexed: HashSet<String>,
}

struct DiffStats {
    ops: u64,
    compares: u64,
    rows_compared: u64,
    mismatches: u64,
    sqlite_divergences: u64,
    refusals_checked: u64,
    recipe_tables_seen: u64,
}

struct Diff {
    rng: Rng,
    nodes: Vec<Node>,
    next_col: u64,
    use_sqlite: bool,
    dir: PathBuf,
    stats: DiffStats,
    seed: u64,
    log: Vec<String>,
}

const BASE_COLS: [(&str, &str); 6] = [
    ("a", "INTEGER"),
    ("b", "REAL"),
    ("c", "TEXT"),
    ("d", "NUMERIC"),
    ("e", ""),
    ("f", "VARCHAR(8)"),
];
const ADD_TYPES: [&str; 8] = [
    "INTEGER",
    "REAL",
    "TEXT",
    "NUMERIC",
    "VARCHAR(8)",
    "DECIMAL(10,2)",
    "BLOB",
    "",
];

impl Diff {
    fn lit(&mut self) -> String {
        match self.rng.below(8) {
            0 => "NULL".into(),
            1 | 2 => self.rng.range(-20, 20).to_string(),
            3 => format!("{}.{}", self.rng.range(-50, 50), self.rng.below(100)),
            4 => {
                let len = self.rng.below(4) as usize + 1;
                format!("'{}'", self.rng.alpha(len))
            }
            5 => format!("'{}'", self.rng.range(-9, 9)),
            6 => self.rng.range(1, 9000).to_string(),
            _ => format!("{}.5", self.rng.range(0, 9)),
        }
    }

    fn col_of(&mut self, node: usize) -> String {
        let cols = &self.nodes[node].cols;
        cols[self.rng.below(cols.len() as u64) as usize].name.clone()
    }

    fn term(&mut self, node: usize) -> String {
        if self.rng.chance(60) {
            self.col_of(node)
        } else {
            self.lit()
        }
    }

    fn expr(&mut self, node: usize, depth: u32) -> String {
        let k = if depth == 0 { 0 } else { self.rng.below(12) };
        match k {
            0..=2 => self.term(node),
            3 => format!("({} + {})", self.expr(node, depth - 1), self.term(node)),
            4 => format!("({} - {})", self.term(node), self.expr(node, depth - 1)),
            5 => format!("({} * {})", self.term(node), self.term(node)),
            6 => format!("({} || {})", self.term(node), self.term(node)),
            7 => format!(
                "CASE WHEN {} THEN {} WHEN {} THEN {} ELSE {} END",
                self.pred(node, 0),
                self.lit(),
                self.pred(node, 0),
                self.expr(node, depth - 1),
                self.term(node)
            ),
            8 => format!("coalesce({}, {})", self.term(node), self.lit()),
            9 => {
                let c = self.col_of(node);
                match self.rng.below(5) {
                    0 => format!("upper({c})"),
                    1 => format!("length({c})"),
                    2 => format!("substr({c}, 1, 2)"),
                    3 => format!("typeof({c})"),
                    _ => format!("round({c}, 1)"),
                }
            }
            10 => format!("CAST({} AS TEXT)", self.term(node)),
            _ => format!("({} / {})", self.term(node), self.term(node)),
        }
    }

    fn pred(&mut self, node: usize, depth: u32) -> String {
        let k = if depth == 0 { self.rng.below(5) } else { self.rng.below(8) };
        let ops = ["=", "<>", "<", "<=", ">", ">="];
        match k {
            0..=1 => format!("{} {} {}", self.col_of(node), self.rng.pick(&ops), self.lit()),
            2 => format!("{} IS NULL", self.col_of(node)),
            3 => format!("{} IS NOT NULL", self.col_of(node)),
            4 => format!(
                "{} BETWEEN {} AND {}",
                self.col_of(node),
                self.rng.range(-10, 0),
                self.rng.range(0, 10)
            ),
            5 => format!("({} AND {})", self.pred(node, depth - 1), self.pred(node, 0)),
            6 => format!("({} OR {})", self.pred(node, 0), self.pred(node, depth - 1)),
            _ => format!("NOT ({})", self.pred(node, depth - 1)),
        }
    }

    fn live(&self) -> Vec<usize> {
        (0..self.nodes.len()).filter(|&i| self.nodes[i].alive).collect()
    }

    fn sqlite_exec(&mut self, node: usize, sql: &str) -> Result<i64, String> {
        let Some(sq) = self.nodes[node].sq.as_ref() else {
            return Ok(-1);
        };
        sq.execute_batch(sql).map_err(|e| e.to_string())?;
        Ok(sq.changes() as i64)
    }

    /// Run `sql` on all arms of `node`. Returns false if every arm refused it alike (the op is
    /// then dropped from the stream); a difference in outcome is a mismatch.
    fn run_all(&mut self, node: usize, sql: &str, is_dml: bool) -> bool {
        self.log.push(format!("[{node}] {sql}"));
        let r = self.nodes[node].c_r.execute(sql);
        let e = self.nodes[node].c_e.execute(sql);
        match (&r, &e) {
            (Err(_), Err(_)) => {
                // Both refused alike (e.g. a type error in a generated expression): fine, but
                // SQLite must not have accepted it silently either way; it is not run there.
                return false;
            }
            (Ok(()), Ok(())) => {}
            _ => {
                self.mismatch(node, &format!("outcome differs: recipe {r:?} eager {e:?} for {sql}"));
                return false;
            }
        }
        if is_dml {
            let (cr, ce) = (self.nodes[node].c_r.changes(), self.nodes[node].c_e.changes());
            if cr != ce {
                self.mismatch(node, &format!("changes() recipe {cr} eager {ce} for {sql}"));
            }
            if self.use_sqlite {
                match self.sqlite_exec(node, sql) {
                    Ok(cs) if cs >= 0 && cs != ce => {
                        self.stats.sqlite_divergences += 1;
                        println!("SQLITE-DIVERGENCE seed={} node={node} changes eager {ce} sqlite {cs}: {sql}", self.seed);
                    }
                    Err(err) => {
                        self.stats.sqlite_divergences += 1;
                        println!("SQLITE-DIVERGENCE seed={} node={node} sqlite error {err}: {sql}", self.seed);
                    }
                    _ => {}
                }
            }
        } else if self.use_sqlite {
            if let Err(err) = self.sqlite_exec(node, sql) {
                self.stats.sqlite_divergences += 1;
                println!("SQLITE-DIVERGENCE seed={} node={node} sqlite error {err}: {sql}", self.seed);
            }
        }
        true
    }

    fn mismatch(&mut self, node: usize, what: &str) {
        self.stats.mismatches += 1;
        if self.stats.mismatches == 1 {
            let path = self.dir.parent().unwrap().join(format!("mismatch_seed{}.log", self.seed));
            let mut body = format!("# {what}\n");
            for l in &self.log {
                body.push_str(l);
                body.push('\n');
            }
            let _ = std::fs::write(&path, body);
            println!("  full op log: {}", path.display());
        }
        if self.stats.mismatches <= 3 {
            println!("MISMATCH seed={} node={node}: {what}", self.seed);
            for l in self.log.iter().rev().take(12).collect::<Vec<_>>().into_iter().rev() {
                println!("  ctx {l}");
            }
        }
    }

    fn compare(&mut self, node: usize) {
        let queries = [
            "SELECT * FROM t ORDER BY id".to_string(),
            format!(
                "SELECT count(*), sum(length(CAST({0} AS TEXT))), max({0}) FROM t",
                self.nodes[node].cols.last().unwrap().name
            ),
        ];
        let indexed: Vec<String> = self.nodes[node].indexed.iter().cloned().collect();
        let mut qs = queries.to_vec();
        for col in indexed {
            qs.push(format!(
                "SELECT {col}, id FROM t WHERE {col} IS NOT NULL ORDER BY {col}, id"
            ));
        }
        for q in qs {
            self.stats.compares += 1;
            let r = turso_rows(&self.nodes[node].c_r, &q);
            let e = turso_rows(&self.nodes[node].c_e, &q);
            match (&r, &e) {
                (Ok(r), Ok(e)) => {
                    self.stats.rows_compared += e.len() as u64;
                    if r != e {
                        let first = r
                            .iter()
                            .zip(e.iter())
                            .position(|(a, b)| a != b)
                            .unwrap_or(r.len().min(e.len()));
                        self.mismatch(
                            node,
                            &format!(
                                "{q}: row {first} recipe {:?} eager {:?} (rows {} vs {})",
                                r.get(first),
                                e.get(first),
                                r.len(),
                                e.len()
                            ),
                        );
                    }
                }
                _ => self.mismatch(node, &format!("{q}: recipe {r:?} eager {e:?}")),
            }
            if self.use_sqlite {
                if let (Some(sq), Ok(e)) = (self.nodes[node].sq.as_ref(), &e) {
                    match sqlite_rows(sq, &q) {
                        Ok(s) if &s != e => {
                            self.stats.sqlite_divergences += 1;
                            let first = s.iter().zip(e.iter()).position(|(a, b)| a != b);
                            println!(
                                "SQLITE-DIVERGENCE seed={} node={node} {q}: row {first:?} eager {:?} sqlite {:?}",
                                self.seed,
                                first.and_then(|i| e.get(i)),
                                first.and_then(|i| s.get(i))
                            );
                        }
                        Err(err) => {
                            self.stats.sqlite_divergences += 1;
                            println!("SQLITE-DIVERGENCE seed={} node={node} {q}: sqlite error {err}", self.seed);
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    fn has_recipes(&self, node: usize) -> bool {
        int(
            &self.nodes[node].c_r,
            "SELECT count(*) FROM sqlite_schema WHERE type = 'recipe'",
        ) > 0
    }

    fn fork(&mut self, parent: usize) {
        let br_r = self.nodes[parent]
            .c_r
            .fork_branch()
            .unwrap_or_else(|e| not_a_result(&format!("fork recipe: {e}")));
        let br_e = self.nodes[parent]
            .c_e
            .fork_branch()
            .unwrap_or_else(|e| not_a_result(&format!("fork eager: {e}")));
        let c_r = br_r.connect().unwrap();
        let c_e = br_e.connect().unwrap();
        c_r.set_recipe_backfill(true);
        let idx = self.nodes.len();
        let sq_path = self.dir.join(format!("s{}_n{idx}.db", self.seed));
        let sq = if self.use_sqlite {
            let _ = std::fs::remove_file(&sq_path);
            let p = self.nodes[parent].sq.as_ref().unwrap();
            p.execute(&format!("VACUUM INTO '{}'", sq_path.display()), [])
                .unwrap_or_else(|e| not_a_result(&format!("sqlite VACUUM INTO: {e}")));
            Some(rusqlite::Connection::open(&sq_path).unwrap())
        } else {
            None
        };
        let cols = self.nodes[parent].cols.clone();
        let indexed = self.nodes[parent].indexed.clone();
        self.log.push(format!("[{idx}] FORK of {parent}"));
        self.nodes.push(Node {
            parent: Some(parent),
            alive: true,
            br_r: Some(br_r),
            br_e: Some(br_e),
            c_r,
            c_e,
            sq,
            sq_path,
            cols,
            indexed,
        });
    }

    fn release(&mut self, node: usize) {
        // The node's connections must go before its handles: a reap waits for open connections.
        // Any live connection serves as the dead node's placeholder; it is never used again.
        let p = self.nodes[node].parent.expect("the trunk is never released");
        let (pr, pe) = (self.nodes[p].c_r.clone(), self.nodes[p].c_e.clone());
        let n = &mut self.nodes[node];
        n.alive = false;
        n.sq = None;
        let _ = std::fs::remove_file(&n.sq_path);
        drop(std::mem::replace(&mut n.c_r, pr));
        drop(std::mem::replace(&mut n.c_e, pe));
        if let Some(r) = n.br_r.take() {
            r.reap().unwrap_or_else(|err| not_a_result(&format!("reap recipe: {err}")));
        }
        if let Some(e) = n.br_e.take() {
            e.reap().unwrap_or_else(|err| not_a_result(&format!("reap eager: {err}")));
        }
        self.log.push(format!("[{node}] RELEASE"));
    }

    fn step(&mut self) {
        let live = self.live();
        let node = *self.rng.pick(&live);
        let roll = self.rng.below(100);
        self.stats.ops += 1;
        match roll {
            0..=7 => {
                if self.live().len() < 12 {
                    self.fork(node);
                    let child = self.nodes.len() - 1;
                    self.compare(child);
                }
            }
            8..=9 => {
                if node != 0 && self.nodes[node].parent.is_some() {
                    self.release(node);
                }
            }
            10..=19 => {
                self.next_col += 1;
                let name = format!("x{}", self.next_col);
                let ty = *self.rng.pick(&ADD_TYPES);
                let default = if self.rng.chance(40) {
                    format!(" DEFAULT {}", self.lit().replace("NULL", "7"))
                } else {
                    String::new()
                };
                let sql = format!("ALTER TABLE t ADD COLUMN {name} {ty}{default}");
                if self.run_all(node, &sql, false) {
                    self.nodes[node].cols.push(Col { name, ty });
                }
                self.compare(node);
            }
            20..=49 => {
                // A bulk UPDATE: the recipe path when eligible.
                let ntargets = if self.rng.chance(20) { 2 } else { 1 };
                let mut sets = Vec::new();
                let mut used = HashSet::new();
                for _ in 0..ntargets {
                    let c = self.col_of(node);
                    if used.insert(c.clone()) {
                        let e = self.expr(node, 2);
                        sets.push(format!("{c} = {e}"));
                    }
                }
                let wh = if self.rng.chance(50) {
                    format!(" WHERE {}", self.pred(node, 1))
                } else {
                    String::new()
                };
                let sql = format!("UPDATE t SET {}{wh}", sets.join(", "));
                self.run_all(node, &sql, true);
                self.compare(node);
            }
            50..=64 => {
                let id = self.rng.range(1, 80);
                let c = self.col_of(node);
                let e = self.expr(node, 1);
                let sql = format!("UPDATE t SET {c} = {e} WHERE id = {id}");
                self.run_all(node, &sql, true);
                self.compare(node);
            }
            65..=74 => {
                let cols: Vec<String> = self.nodes[node].cols.iter().map(|c| c.name.clone()).collect();
                let k = 1 + self.rng.below(cols.len() as u64) as usize;
                let chosen: Vec<String> = cols.iter().take(k).cloned().collect();
                let vals: Vec<String> = (0..k).map(|_| self.lit()).collect();
                let sql = if self.rng.chance(30) {
                    let id = self.rng.range(1, 60);
                    format!(
                        "INSERT INTO t(id, {}) VALUES ({id}, {}) ON CONFLICT(id) DO UPDATE SET {} = {}",
                        chosen.join(", "),
                        vals.join(", "),
                        chosen[0],
                        self.expr(node, 1)
                    )
                } else if self.rng.chance(20) {
                    let id = self.rng.range(1, 60);
                    format!(
                        "INSERT OR REPLACE INTO t(id, {}) VALUES ({id}, {})",
                        chosen.join(", "),
                        vals.join(", ")
                    )
                } else {
                    format!("INSERT INTO t({}) VALUES ({})", chosen.join(", "), vals.join(", "))
                };
                self.run_all(node, &sql, true);
                self.compare(node);
            }
            75..=81 => {
                let sql = if self.rng.chance(60) {
                    format!("DELETE FROM t WHERE id = {}", self.rng.range(1, 80))
                } else {
                    format!("DELETE FROM t WHERE {}", self.pred(node, 0))
                };
                self.run_all(node, &sql, true);
                self.compare(node);
            }
            82..=85 => {
                let c = self.col_of(node);
                if !self.nodes[node].indexed.contains(&c) {
                    let sql = format!("CREATE INDEX ix_{c}_{} ON t({c})", self.next_col);
                    self.next_col += 1;
                    if self.run_all(node, &sql, false) {
                        self.nodes[node].indexed.insert(c);
                    }
                }
                self.compare(node);
            }
            86..=88 => {
                // DROP COLUMN: eager everywhere on a table without recipes; on a recipe table the
                // RECIPE arm must refuse (a scope line), and the op is not run on the others.
                let cols = self.nodes[node].cols.clone();
                let candidates: Vec<&Col> = cols
                    .iter()
                    .filter(|c| c.name.starts_with('x') && !self.nodes[node].indexed.contains(&c.name))
                    .collect();
                if let Some(col) = candidates.first() {
                    let name = col.name.clone();
                    let sql = format!("ALTER TABLE t DROP COLUMN {name}");
                    if self.has_recipes(node) {
                        self.log.push(format!("[{node}] (recipe arm only) {sql}"));
                        match self.nodes[node].c_r.execute(&sql) {
                            Err(e) if e.to_string().contains("recipe backfills") => {
                                self.stats.refusals_checked += 1
                            }
                            other => self.mismatch(
                                node,
                                &format!("DROP COLUMN on a recipe table was not refused: {other:?}"),
                            ),
                        }
                    } else if self.run_all(node, &sql, false) {
                        self.nodes[node].cols.retain(|c| c.name != name);
                    }
                }
                self.compare(node);
            }
            _ => {
                // A read-only query that goes through the recipe path, compared.
                self.compare(node);
            }
        }
        if self.stats.ops % 25 == 0 {
            for n in self.live() {
                self.compare(n);
            }
        }
    }
}

fn run_diff_seed(seed: u64, ops: u64, dir: &Path, use_sqlite: bool, totals: &mut DiffStats) {
    let sub = dir.join(format!("seed{seed}"));
    let _ = std::fs::remove_dir_all(&sub);
    std::fs::create_dir_all(&sub).unwrap();
    let db_r = open_db(&sub.join("r.db"), BranchDurability::Volatile);
    let db_e = open_db(&sub.join("e.db"), BranchDurability::Volatile);
    let c_r = db_r.connect().unwrap();
    let c_e = db_e.connect().unwrap();
    c_r.set_recipe_backfill(true);
    let sq_path = sub.join("s_trunk.db");
    let sq = use_sqlite.then(|| rusqlite::Connection::open(&sq_path).unwrap());
    let mut d = Diff {
        rng: Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ 0xA5A5),
        nodes: Vec::new(),
        next_col: 0,
        use_sqlite,
        dir: sub.clone(),
        stats: DiffStats {
            ops: 0,
            compares: 0,
            rows_compared: 0,
            mismatches: 0,
            sqlite_divergences: 0,
            refusals_checked: 0,
            recipe_tables_seen: 0,
        },
        seed,
        log: Vec::new(),
    };
    let cols: Vec<Col> = BASE_COLS
        .iter()
        .map(|(n, t)| Col {
            name: n.to_string(),
            ty: t,
        })
        .collect();
    d.nodes.push(Node {
        parent: None,
        alive: true,
        br_r: None,
        br_e: None,
        c_r,
        c_e,
        sq,
        sq_path,
        cols,
        indexed: HashSet::new(),
    });
    let create = format!(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, {})",
        BASE_COLS
            .iter()
            .map(|(n, t)| format!("{n} {t}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    d.run_all(0, &create, false);
    let nrows = 20 + d.rng.below(40);
    for _ in 0..nrows {
        let vals: Vec<String> = (0..BASE_COLS.len()).map(|_| d.lit()).collect();
        let sql = format!("INSERT INTO t(a, b, c, d, e, f) VALUES ({})", vals.join(", "));
        d.run_all(0, &sql, true);
    }
    d.compare(0);
    for _ in 0..ops {
        d.step();
        if d.stats.mismatches > 0 {
            break;
        }
    }
    for n in d.live() {
        d.compare(n);
    }
    for n in d.live() {
        if d.has_recipes(n) {
            d.stats.recipe_tables_seen += 1;
        }
    }
    totals.ops += d.stats.ops;
    totals.compares += d.stats.compares;
    totals.rows_compared += d.stats.rows_compared;
    totals.mismatches += d.stats.mismatches;
    totals.sqlite_divergences += d.stats.sqlite_divergences;
    totals.refusals_checked += d.stats.refusals_checked;
    totals.recipe_tables_seen += d.stats.recipe_tables_seen;
    let nodes = std::mem::take(&mut d.nodes);
    for n in nodes.into_iter().rev() {
        drop(n.c_r);
        drop(n.c_e);
        drop(n.br_r);
        drop(n.br_e);
    }
    drop(db_r);
    drop(db_e);
    let _ = std::fs::remove_dir_all(&sub);
}

fn diff_main(args: &Args) {
    let mutant = std::env::var("K1_RECIPE_MUTANT").unwrap_or_else(|_| "none".into());
    std::fs::create_dir_all(&args.dir).unwrap();
    let mut totals = DiffStats {
        ops: 0,
        compares: 0,
        rows_compared: 0,
        mismatches: 0,
        sqlite_divergences: 0,
        refusals_checked: 0,
        recipe_tables_seen: 0,
    };
    let before = recipe_io();
    let mut seeds_with_mismatch = 0u64;
    for seed in args.start..args.start + args.seeds {
        let m0 = totals.mismatches;
        run_diff_seed(seed, args.ops, &args.dir, args.sqlite, &mut totals);
        if totals.mismatches > m0 {
            seeds_with_mismatch += 1;
        }
    }
    let after = recipe_io();
    let d = |i: usize| after[i] - before[i];
    println!(
        "DIFF mutant={mutant} seeds={} start={} ops_per_seed={} ops={} compares={} rows_compared={} \
         mismatches={} seeds_with_mismatch={seeds_with_mismatch} sqlite_divergences={} refusals_checked={} \
         live_recipe_tables_at_end={} recipes_installed={} fallbacks={} stale_reads={} recipe_evals={} \
         cache_hits={} empty_match={} sqlite={}",
        args.seeds,
        args.start,
        args.ops,
        totals.ops,
        totals.compares,
        totals.rows_compared,
        totals.mismatches,
        totals.sqlite_divergences,
        totals.refusals_checked,
        totals.recipe_tables_seen,
        d(counter::INSTALLED),
        d(counter::FALLBACKS),
        d(counter::STALE_READS),
        d(counter::RECIPE_EVALS),
        d(counter::CACHE_HITS),
        d(counter::EMPTY_MATCH),
        args.sqlite
    );
    if totals.ops == 0 || totals.compares == 0 || d(counter::INSTALLED) == 0 || d(counter::STALE_READS) == 0 {
        not_a_result("the differential collected nothing on the recipe path");
    }
    if totals.mismatches > 0 {
        std::process::exit(3);
    }
}

// ---------------------------------------------------------------------------------------------
// bench
// ---------------------------------------------------------------------------------------------

const CUSTOMER_DDL: &str = "CREATE TABLE customer (
    c_id           INT NOT NULL,
    c_d_id         SMALLINT NOT NULL,
    c_w_id         SMALLINT NOT NULL,
    c_first        VARCHAR(16),
    c_middle       CHAR(2),
    c_last         VARCHAR(16),
    c_street_1     VARCHAR(20),
    c_street_2     VARCHAR(20),
    c_city         VARCHAR(20),
    c_state        CHAR(2),
    c_zip          CHAR(9),
    c_phone        CHAR(16),
    c_since        TIMESTAMP,
    c_credit       CHAR(2),
    c_credit_lim   DECIMAL(12, 2),
    c_discount     DECIMAL(4, 4),
    c_balance      DECIMAL(12, 2),
    c_ytd_payment  FLOAT,
    c_payment_cnt  SMALLINT,
    c_delivery_cnt SMALLINT,
    c_data         VARCHAR(500),
    c_n_nationkey  INTEGER,
    PRIMARY KEY (c_w_id, c_d_id, c_id)
)";

const FR_DDL: [&str; 3] = [
    "CREATE TABLE orders (
    o_id         INT NOT NULL,
    o_d_id       SMALLINT NOT NULL,
    o_w_id       SMALLINT NOT NULL,
    o_c_id       INT,
    o_entry_d    TIMESTAMP,
    o_carrier_id SMALLINT,
    o_ol_cnt     SMALLINT,
    o_all_local  SMALLINT,
    PRIMARY KEY (o_w_id, o_d_id, o_id)
)",
    "CREATE TABLE order_line (
    ol_o_id        INT NOT NULL,
    ol_d_id        SMALLINT NOT NULL,
    ol_w_id        SMALLINT NOT NULL,
    ol_number      SMALLINT NOT NULL,
    ol_i_id        INT,
    ol_supply_w_id SMALLINT,
    ol_delivery_d  TIMESTAMP,
    ol_quantity    SMALLINT,
    ol_amount      DECIMAL(6, 2),
    ol_dist_info   CHAR(24),
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
)",
    "CREATE TABLE stock (
    s_i_id       INT NOT NULL,
    s_w_id       SMALLINT NOT NULL,
    s_quantity   SMALLINT,
    s_dist_01    CHAR(24),
    s_dist_02    CHAR(24),
    s_dist_03    CHAR(24),
    s_dist_04    CHAR(24),
    s_dist_05    CHAR(24),
    s_dist_06    CHAR(24),
    s_dist_07    CHAR(24),
    s_dist_08    CHAR(24),
    s_dist_09    CHAR(24),
    s_dist_10    CHAR(24),
    s_ytd        DECIMAL(8, 2),
    s_order_cnt  SMALLINT,
    s_remote_cnt SMALLINT,
    s_data       VARCHAR(50),
    s_su_suppkey INTEGER,
    PRIMARY KEY (s_w_id, s_i_id)
)",
];

fn bind(stmt: &mut turso_core::Statement, i: usize, v: Value) {
    stmt.bind_at(NonZero::new(i).unwrap(), v).unwrap();
}

fn text(s: String) -> Value {
    Value::from_text(s)
}

/// The customer table, N rows, PREREG §5.
fn load_customer(trunk: &Arc<Connection>, n: i64, null_every: i64, rng: &mut Rng) {
    exec(trunk, CUSTOMER_DDL);
    exec(trunk, "BEGIN");
    let mut stmt = trunk
        .prepare("INSERT INTO customer VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)")
        .unwrap();
    for k in 0..n {
        let c_id = k % 3000 + 1;
        let c_d_id = (k / 3000) % 10 + 1;
        let c_w_id = k / 30000 + 1;
        bind(&mut stmt, 1, Value::from_i64(c_id));
        bind(&mut stmt, 2, Value::from_i64(c_d_id));
        bind(&mut stmt, 3, Value::from_i64(c_w_id));
        let l1 = 8 + rng.below(9) as usize;
        bind(&mut stmt, 4, text(rng.alpha(l1)));
        bind(&mut stmt, 5, text("OE".into()));
        let l2 = 6 + rng.below(11) as usize;
        bind(&mut stmt, 6, text(rng.alpha(l2)));
        for i in 7..=9 {
            let l = 10 + rng.below(11) as usize;
            bind(&mut stmt, i, text(rng.alpha(l)));
        }
        bind(&mut stmt, 10, text(rng.alpha(2)));
        bind(&mut stmt, 11, text(format!("{:09}", rng.below(1_000_000_000))));
        bind(&mut stmt, 12, text(format!("{:016}", rng.below(10_000_000_000_000_000))));
        bind(&mut stmt, 13, text("2025-01-01 00:00:00".into()));
        bind(&mut stmt, 14, text(if rng.chance(10) { "BC" } else { "GC" }.into()));
        bind(&mut stmt, 15, Value::from_f64(50000.00));
        bind(&mut stmt, 16, Value::from_f64(rng.below(5001) as f64 / 10000.0));
        if null_every > 0 && c_id % null_every == 0 {
            bind(&mut stmt, 17, Value::Null);
        } else {
            bind(&mut stmt, 17, Value::from_f64(-10.00));
        }
        bind(&mut stmt, 18, Value::from_f64((c_id % 10) as f64 * 1000.0));
        bind(&mut stmt, 19, Value::from_i64(1));
        bind(&mut stmt, 20, Value::from_i64(0));
        let l3 = 300 + rng.below(201) as usize;
        bind(&mut stmt, 21, text(rng.alpha(l3)));
        bind(&mut stmt, 22, Value::from_i64(rng.below(25) as i64));
        stmt.run_ignore_rows().unwrap();
        stmt.reset().unwrap();
    }
    drop(stmt);
    exec(trunk, "COMMIT");
}

/// FailureRepro's other tables at TPC-C scale-1 cardinalities (orders 30,000; order_line about
/// 300,000; stock 100,000).
fn load_fr(trunk: &Arc<Connection>, rng: &mut Rng) {
    for ddl in FR_DDL {
        exec(trunk, ddl);
    }
    exec(trunk, "BEGIN");
    let mut o = trunk
        .prepare("INSERT INTO orders VALUES (?1, ?2, ?3, ?4, '2025-01-01 00:00:00', ?5, ?6, 1)")
        .unwrap();
    let mut ol = trunk
        .prepare("INSERT INTO order_line VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, 5, ?7, ?8)")
        .unwrap();
    for d in 1..=10i64 {
        for oid in 1..=3000i64 {
            let cnt = 5 + rng.below(11) as i64;
            bind(&mut o, 1, Value::from_i64(oid));
            bind(&mut o, 2, Value::from_i64(d));
            bind(&mut o, 3, Value::from_i64(1));
            bind(&mut o, 4, Value::from_i64(rng.range(1, 3000)));
            bind(&mut o, 5, Value::from_i64(rng.range(1, 10)));
            bind(&mut o, 6, Value::from_i64(cnt));
            o.run_ignore_rows().unwrap();
            o.reset().unwrap();
            for num in 1..=cnt {
                bind(&mut ol, 1, Value::from_i64(oid));
                bind(&mut ol, 2, Value::from_i64(d));
                bind(&mut ol, 3, Value::from_i64(1));
                bind(&mut ol, 4, Value::from_i64(num));
                bind(&mut ol, 5, Value::from_i64(rng.range(1, 100000)));
                bind(&mut ol, 6, Value::from_i64(1));
                bind(&mut ol, 7, Value::from_f64(rng.below(1000000) as f64 / 100.0));
                bind(&mut ol, 8, text(rng.alpha(24)));
                ol.run_ignore_rows().unwrap();
                ol.reset().unwrap();
            }
        }
    }
    drop(o);
    drop(ol);
    let mut s = trunk
        .prepare("INSERT INTO stock VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 0, 0, 0, ?13, ?14)")
        .unwrap();
    for i in 1..=100000i64 {
        bind(&mut s, 1, Value::from_i64(i));
        bind(&mut s, 2, Value::from_i64(rng.range(10, 100)));
        for k in 3..=12 {
            bind(&mut s, k, text(rng.alpha(24)));
        }
        let l = 26 + rng.below(25) as usize;
        bind(&mut s, 13, text(rng.alpha(l)));
        bind(&mut s, 14, Value::from_i64(rng.range(0, 9999)));
        s.run_ignore_rows().unwrap();
        s.reset().unwrap();
    }
    drop(s);
    exec(trunk, "COMMIT");
}

/// One workload step's statements: (label, sql, kind). kind: 'D' DDL, 'M' DML, 'Q' query.
fn step_sql(workload: &str, thread: u64, step: u64, rng: &mut Rng) -> Vec<(String, String, char)> {
    let sfx = format!("t{thread}_s{step}");
    match workload {
        // SoftwareDevOps, macrobench/workflows.py @ 58cf262, M_s = 2 (PREREG P2).
        "sd" => vec![
            ("ddl1".into(), format!("ALTER TABLE customer ADD COLUMN loyalty_tier_{sfx} VARCHAR(8);"), 'D'),
            ("ddl2".into(), format!("ALTER TABLE customer ADD COLUMN credit_lim_{sfx} DECIMAL(10,2);"), 'D'),
            ("dml".into(), format!(
                "UPDATE customer SET
                   loyalty_tier_{sfx} = CASE
                       WHEN c_ytd_payment > 9000 THEN 'Gold'
                       WHEN c_ytd_payment > 5000 THEN 'Silver'
                       ELSE 'Bronze'
                   END,
                   credit_lim_{sfx} = c_credit_lim;"), 'M'),
            ("eval1".into(), format!("SELECT COUNT(*) FROM customer WHERE loyalty_tier_{sfx} IS NULL;"), 'Q'),
            ("eval2".into(), format!(
                "SELECT loyalty_tier_{sfx}, COUNT(*), AVG(credit_lim_{sfx})
               FROM customer GROUP BY loyalty_tier_{sfx};"), 'Q'),
            ("cmp1".into(), format!(
                "SELECT loyalty_tier_{sfx}, COUNT(*), AVG(credit_lim_{sfx})
               FROM customer GROUP BY loyalty_tier_{sfx};"), 'Q'),
        ],
        // DataCleaningOps.
        "dc" => {
            let dml = if thread % 2 == 0 {
                "UPDATE customer SET c_balance = 0 WHERE c_balance IS NULL;".to_string()
            } else {
                "DELETE FROM customer WHERE c_balance IS NULL;".to_string()
            };
            vec![
                ("ddl1".into(), format!("ALTER TABLE customer ADD COLUMN c_cleaned_{sfx} BOOLEAN DEFAULT false;"), 'D'),
                ("dml".into(), dml, 'M'),
                ("eval1".into(), "SELECT COUNT(CASE WHEN c_balance < 0 THEN 1 END) AS invalid
               FROM customer;".into(), 'Q'),
                ("cmp1".into(), "SELECT COUNT(CASE WHEN c_balance IS NULL THEN 1 END) AS nulls,
                      MAX(c_ytd_payment) - MIN(c_ytd_payment) AS spread
               FROM customer;".into(), 'Q'),
                ("cmp2".into(), "SELECT COUNT(CASE WHEN c_balance IS NULL THEN 1 END) AS nulls,
                      MAX(c_ytd_payment) - MIN(c_ytd_payment) AS spread
               FROM customer;".into(), 'Q'),
            ]
        }
        // Synthetic full-table repair (labelled; Git4Data's description).
        "repair" => vec![
            ("dml".into(), "UPDATE customer SET c_balance = c_balance - 72.50;".into(), 'M'),
            ("eval1".into(), "SELECT SUM(c_balance), COUNT(*) FROM customer;".into(), 'Q'),
        ],
        // FailureReproOps (statement shapes verbatim; w_id/d_id/amount from this RNG).
        "fr" => {
            let mut v: Vec<(String, String, char)> = Vec::new();
            v.push(("ddl1".into(), format!("ALTER TABLE order_line ADD COLUMN ol_discount_{sfx} DECIMAL(5,2);"), 'D'));
            if step % 2 == 0 {
                v.push(("ddl_drop".into(), format!("ALTER TABLE order_line DROP COLUMN ol_discount_{sfx};"), 'D'));
            }
            v.push(("ddl3".into(), format!("ALTER TABLE orders ADD COLUMN o_flag_{sfx} BOOLEAN DEFAULT false;"), 'D'));
            v.push(("ddl4".into(), format!("ALTER TABLE customer ADD COLUMN c_note_{sfx} VARCHAR(64);"), 'D'));
            v.push(("ddl5".into(), format!("ALTER TABLE stock ADD COLUMN s_tag_{sfx} VARCHAR(16);"), 'D'));
            let w_id = 1;
            let d_id = rng.range(1, 10);
            let o_id = 100_000 + thread * 100_000 + step;
            v.push(("dml_o".into(), format!(
                "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id,
                o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
                VALUES ({o_id}, {d_id}, {w_id}, 42, NULL, 5, 1,
                        CURRENT_TIMESTAMP)
                ON CONFLICT DO NOTHING;"), 'M'));
            v.push(("dml_c".into(), format!(
                "UPDATE customer SET c_balance = c_balance - 72.50
                WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = 42;"), 'M'));
            v.push(("dml_d".into(), format!(
                "DELETE FROM order_line
                WHERE ol_o_id = {o_id} AND ol_d_id = {d_id}
                AND ol_w_id = {w_id};"), 'M'));
            for i in 0..42 {
                let amount = *rng.pick(&[-1i64, 0, 1, 50, 100]);
                v.push((format!("dml_ol{i}"), format!(
                    "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id,
                    ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d,
                    ol_quantity, ol_amount, ol_dist_info)
                    VALUES ({o_id}, {d_id}, {w_id}, {}, 1, {w_id},
                            NULL, 5, {amount}, 'dist_info')
                    ON CONFLICT DO NOTHING;", i + 1), 'M'));
            }
            v.push(("eval1".into(), "SELECT DISTINCT o.o_id
               FROM orders o
               JOIN order_line ol ON ol.ol_o_id = o.o_id
                                  AND ol.ol_d_id = o.o_d_id
                                  AND ol.ol_w_id = o.o_w_id
               WHERE o.o_id >= (SELECT MAX(o_id) - 100000 FROM orders)
                 AND ol.ol_i_id <= 15000
               GROUP BY o.o_id, o.o_ol_cnt
               HAVING o.o_ol_cnt <> COUNT(*);".into(), 'Q'));
            v
        }
        other => die(&format!("unknown workload {other}")),
    }
}

/// LAZY (PREREG §3): before a query, migrate the rows it reads in place with an eager UPDATE.
/// Every workload query here reads the whole table, so the migration has no WHERE.
fn lazy_migration(workload: &str, thread: u64, step: u64) -> Option<String> {
    let sfx = format!("t{thread}_s{step}");
    match workload {
        "sd" => Some(format!("UPDATE customer SET loyalty_tier_{sfx} = loyalty_tier_{sfx};")),
        "dc" | "repair" => Some("UPDATE customer SET c_balance = c_balance;".to_string()),
        _ => None,
    }
}

fn fnv(h: &mut u64, s: &str) {
    for b in s.bytes() {
        *h ^= b as u64;
        *h = h.wrapping_mul(0x100_0000_01B3);
    }
}

fn result_hash(rs: &[Vec<Value>]) -> u64 {
    let mut h = 0xCBF2_9CE4_8422_2325u64;
    for r in rs {
        for v in r {
            fnv(&mut h, &format!("{:?}|", cv_turso(v)));
        }
        fnv(&mut h, "\n");
    }
    h
}

struct Counters([u64; 10], [u64; 4]);
fn snap() -> Counters {
    Counters(recipe_io(), turso_core::branch::page_io())
}

fn bench_main(args: &Args) {
    let workload = args.workload.as_str();
    let arm = args.arm.as_str();
    if !matches!(arm, "eager" | "recipe" | "lazy") {
        die("--arm must be eager, recipe or lazy");
    }
    if args.db.as_os_str().is_empty() || args.n < 1 || args.v < 1 {
        die("--db, --n and --v are required");
    }
    let base = args.db.to_string_lossy().to_string();
    for suffix in ["", "-wal", "-branch-log", "-branch-snap", "-branch-arena", "-branch-cat", "-branch-cat-wal"] {
        let _ = std::fs::remove_file(format!("{base}{suffix}"));
    }
    let db = open_db(&args.db, BranchDurability::Durable { sync: false });
    let trunk = db.connect().unwrap();
    let sync = rows(&trunk, "PRAGMA synchronous").unwrap();
    let mut rng = Rng(args.seed);
    let t0 = Instant::now();
    let null_every = if workload == "dc" { 3 } else { 0 };
    load_customer(&trunk, args.n, null_every, &mut rng);
    if workload == "fr" {
        load_fr(&trunk, &mut rng);
    }
    let load_ns = t0.elapsed().as_nanos();
    let ckpt = rows(&trunk, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let page_size = int(&trunk, "PRAGMA page_size");
    let page_count = int(&trunk, "PRAGMA page_count");
    println!(
        "# bench workload={workload} arm={arm} n={} v={} shape={} seed={} page_size={page_size} \
         page_count={page_count} checkpoint={ckpt:?} synchronous={sync:?} load_ns={load_ns} \
         mutant={:?}",
        args.n,
        args.v,
        args.shape,
        args.seed,
        turso_core::recipe::mutant()
    );

    // The branch tree.
    let mut handles: Vec<Branch> = Vec::with_capacity(args.v as usize);
    let mut parent_of: Vec<Option<usize>> = Vec::new();
    let mut children: Vec<u64> = Vec::new();
    let mut depth: Vec<u64> = Vec::new();
    let mut root_children = 0u64;
    let mut tree_rng = Rng(args.seed ^ 0x51ED_2701);
    let mut per_branch_owned: Vec<usize> = Vec::new();
    let mut hashes: BTreeMap<String, u64> = BTreeMap::new();
    let sample_every = (args.v / args.sample.max(1)).max(1);
    let mut point_lines = 0u64;
    let bench_t0 = Instant::now();
    for b in 0..args.v {
        // Parent: FAN = trunk; CHAIN = previous branch; TREE = BranchBench's rule (uniform over
        // nodes below their fanout cap and depth bound; F_r=5, F_i=3, D=4).
        let parent: Option<usize> = match args.shape.as_str() {
            "fan" => None,
            "chain" => b.checked_sub(1).map(|p| p as usize),
            "tree" => {
                let mut eligible: Vec<Option<usize>> = Vec::new();
                if root_children < 5 {
                    eligible.push(None);
                }
                for (i, d) in depth.iter().enumerate() {
                    if *d < 4 && children[i] < 3 {
                        eligible.push(Some(i));
                    }
                }
                if eligible.is_empty() {
                    not_a_result("tree full: BranchBench's F_r=5, F_i=3, D=4 caps the tree at 200 nodes");
                }
                *tree_rng.pick(&eligible)
            }
            other => die(&format!("unknown shape {other}")),
        };
        let br = match parent {
            None => trunk.fork_branch(),
            Some(p) => handles[p].fork(),
        }
        .unwrap_or_else(|e| not_a_result(&format!("fork {b}: {e}")));
        match parent {
            None => root_children += 1,
            Some(p) => children[p] += 1,
        }
        parent_of.push(parent);
        children.push(0);
        depth.push(parent.map_or(1, |p| depth[p] + 1));
        let conn = br.connect().unwrap();
        exec(&conn, "PRAGMA cache_size = -4000000");
        conn.set_recipe_backfill(arm != "eager");
        let thread = match workload {
            "dc" => b % 10,
            _ => b % 5,
        };
        let step = b;
        let owned_before = br.owned_slots().len();
        let mut line = format!("BR b={b} parent={} depth={} thread={thread} step={step}", parent.map_or(-1, |p| p as i64), depth[b as usize]);
        let stmts = step_sql(workload, thread, step, &mut rng);
        let step_t0 = Instant::now();
        for (label, sql, kind) in &stmts {
            if kind == &'Q' && arm == "lazy" {
                if let Some(m) = lazy_migration(workload, thread, step) {
                    let c0 = snap();
                    conn.set_recipe_backfill(false);
                    let t = Instant::now();
                    exec(&conn, &m);
                    let ns = t.elapsed().as_nanos();
                    conn.set_recipe_backfill(true);
                    let c1 = snap();
                    line.push_str(&format!(
                        " mig_{label}_fetch={} mig_{label}_dirty={} mig_{label}_ns={ns}",
                        c1.0[counter::PAGE_FETCH] - c0.0[counter::PAGE_FETCH],
                        c1.0[counter::BRANCH_DIRTY] - c0.0[counter::BRANCH_DIRTY],
                    ));
                }
            }
            let c0 = snap();
            let t = Instant::now();
            let out = rows(&conn, sql).unwrap_or_else(|e| not_a_result(&format!("branch {b} {label}: {e}: {sql}")));
            let ns = t.elapsed().as_nanos();
            let c1 = snap();
            let dd = |i: usize| c1.0[i] - c0.0[i];
            line.push_str(&format!(
                " {label}_fetch={} {label}_dirty={} {label}_evals={} {label}_installed={} {label}_ns={ns}",
                dd(counter::PAGE_FETCH),
                dd(counter::BRANCH_DIRTY),
                dd(counter::RECIPE_EVALS),
                dd(counter::INSTALLED),
            ));
            if kind == &'M' {
                line.push_str(&format!(" {label}_changes={}", conn.changes()));
            }
            if kind == &'Q' {
                let h = result_hash(&out);
                line.push_str(&format!(" {label}_hash={h:016x}"));
                hashes.insert(format!("{b}:{label}"), h);
            }
        }
        let step_ns = step_t0.elapsed().as_nanos();
        let owned_after = br.owned_slots().len();
        per_branch_owned.push(owned_after);
        let st = db.branch_stats().unwrap();
        line.push_str(&format!(
            " owned_before={owned_before} owned={owned_after} arena_in_use={} step_ns={step_ns}",
            st.arena_slots_in_use
        ));
        println!("{line}");
        // Point reads on sampled branches (KC3), on a fresh connection.
        if b % sample_every == 0 && args.points > 0 {
            drop(conn);
            let pc = br.connect().unwrap();
            pc.set_recipe_backfill(arm != "eager");
            let mut prng = Rng(args.seed ^ 0xBEEF ^ b);
            let mut fetch_rowid = 0u64;
            let mut fetch_pk = 0u64;
            let mut evals = 0u64;
            let mut ns_rowid = 0u128;
            let mut ns_pk = 0u128;
            let mut h = 0xCBF2_9CE4_8422_2325u64;
            let nrows = args.n;
            // Warm the schema so its pages are not counted against the first lookup.
            let _ = int(&pc, "SELECT 1");
            for _ in 0..args.points {
                let k = prng.below(nrows as u64) as i64;
                let rowid = k + 1;
                let (c_id, c_d_id, c_w_id) = (k % 3000 + 1, (k / 3000) % 10 + 1, k / 30000 + 1);
                let c0 = snap();
                let t = Instant::now();
                let r1 = rows(&pc, &format!("SELECT * FROM customer WHERE rowid = {rowid}")).unwrap();
                ns_rowid += t.elapsed().as_nanos();
                let c1 = snap();
                let t = Instant::now();
                let r2 = rows(
                    &pc,
                    &format!("SELECT * FROM customer WHERE c_w_id = {c_w_id} AND c_d_id = {c_d_id} AND c_id = {c_id}"),
                )
                .unwrap();
                ns_pk += t.elapsed().as_nanos();
                let c2 = snap();
                fetch_rowid += c1.0[counter::PAGE_FETCH] - c0.0[counter::PAGE_FETCH];
                fetch_pk += c2.0[counter::PAGE_FETCH] - c1.0[counter::PAGE_FETCH];
                evals += c2.0[counter::RECIPE_EVALS] - c0.0[counter::RECIPE_EVALS];
                if workload != "dc" && (r1.len() != 1 || r2.len() != 1 || r1 != r2) {
                    not_a_result(&format!("point read {rowid}: {} and {} rows", r1.len(), r2.len()));
                }
                h ^= result_hash(&r1).rotate_left((k % 63) as u32);
            }
            println!(
                "PT b={b} points={} fetch_rowid={fetch_rowid} fetch_pk={fetch_pk} evals={evals} \
                 ns_rowid={ns_rowid} ns_pk={ns_pk} hash={h:016x}",
                args.points
            );
            point_lines += 1;
            drop(pc);
        } else {
            drop(conn);
        }
        handles.push(br);
    }
    let bench_ns = bench_t0.elapsed().as_nanos();
    let st = db.branch_stats().unwrap();
    let total_owned: usize = per_branch_owned.iter().sum();
    let max_owned = per_branch_owned.iter().max().copied().unwrap_or(0);
    let min_owned = per_branch_owned.iter().min().copied().unwrap_or(0);
    let mut hh = 0xCBF2_9CE4_8422_2325u64;
    for (k, v) in &hashes {
        fnv(&mut hh, &format!("{k}={v:x};"));
    }
    // Owned pages are re-read at the end: a later sibling never changes an earlier branch's.
    let final_owned: Vec<usize> = handles.iter().map(|h| h.owned_slots().len()).collect();
    let final_total: usize = final_owned.iter().sum();
    let io = recipe_io();
    println!(
        "SUMMARY workload={workload} arm={arm} n={} v={} shape={} branches={} sum_owned={total_owned} \
         sum_owned_end={final_total} min_owned={min_owned} max_owned={max_owned} mean_owned={:.3} \
         arena_in_use={} arena_free={} live_branches={} point_lines={point_lines} installed={} fallbacks={} \
         recipe_evals={} stale_reads={} result_hash={hh:016x} bench_ns={bench_ns} page_io={:?}",
        args.n,
        args.v,
        args.shape,
        handles.len(),
        total_owned as f64 / handles.len().max(1) as f64,
        st.arena_slots_in_use,
        st.arena_slots_free,
        st.live_branches,
        io[counter::INSTALLED],
        io[counter::FALLBACKS],
        io[counter::RECIPE_EVALS],
        io[counter::STALE_READS],
        turso_core::branch::page_io(),
    );
    if handles.is_empty() || total_owned == 0 {
        not_a_result("no branch owned any page: the instrument read nothing");
    }
    if arm != "eager" && workload == "sd" && io[counter::INSTALLED] == 0 {
        not_a_result("recipe arm installed no recipe");
    }
    if arm == "eager" && io[counter::INSTALLED] != 0 {
        not_a_result("eager arm installed a recipe");
    }
    drop(handles);
}

/// Debug aid: run the trunk-only (`[0] `) lines of a `mismatch_seed*.log` on a RECIPE and an
/// EAGER database, printing the first statement after which `SELECT * FROM t` differs.
fn replay_main(args: &Args) {
    let text = std::fs::read_to_string(&args.db).unwrap_or_else(|e| die(&format!("read: {e}")));
    let dir = args.dir.clone();
    std::fs::create_dir_all(&dir).unwrap();
    for f in ["rr.db", "rr.db-wal", "re.db", "re.db-wal"] {
        let _ = std::fs::remove_file(dir.join(f));
    }
    let db_r = open_db(&dir.join("rr.db"), BranchDurability::Volatile);
    let db_e = open_db(&dir.join("re.db"), BranchDurability::Volatile);
    let c_r = db_r.connect().unwrap();
    let c_e = db_e.connect().unwrap();
    c_r.set_recipe_backfill(true);
    for line in text.lines() {
        let Some(sql) = line.strip_prefix("[0] ") else { continue };
        if sql.starts_with("FORK") || sql.starts_with("RELEASE") || sql.starts_with('(') {
            continue;
        }
        let r = c_r.execute(sql);
        let e = c_e.execute(sql);
        let q = "SELECT * FROM t ORDER BY id";
        let rr = turso_rows(&c_r, q);
        let ee = turso_rows(&c_e, q);
        println!(
            "{} | r={:?} e={:?} | equal={} | recipes(local, shared)={:?}",
            sql,
            r.is_ok(),
            e.is_ok(),
            rr == ee,
            turso_core::recipe::debug_recipe_count(&c_r, "t")
        );
        if rr != ee {
            let (rr, ee) = (rr.unwrap(), ee.unwrap());
            for (i, (a, b)) in rr.iter().zip(ee.iter()).enumerate() {
                if a != b {
                    println!("  row {i}: recipe {a:?}\n         eager  {b:?}");
                }
            }
            for r in rows(&c_r, "SELECT type, name, tbl_name, sql FROM sqlite_schema").unwrap() {
                println!("  schema {r:?}");
            }
            for r in rows(&c_r, "SELECT id, __turso_gen FROM t ORDER BY id LIMIT 5").unwrap() {
                println!("  gen {r:?}");
            }
            break;
        }
    }
}

struct Args {
    cmd: String,
    seeds: u64,
    start: u64,
    ops: u64,
    dir: PathBuf,
    sqlite: bool,
    workload: String,
    arm: String,
    n: i64,
    v: u64,
    shape: String,
    db: PathBuf,
    points: u64,
    sample: u64,
    seed: u64,
}

fn parse_args() -> Args {
    let mut it = std::env::args().skip(1);
    let cmd = it.next().unwrap_or_else(|| die("usage: branch_recipe diff|bench ..."));
    let mut a = Args {
        cmd,
        seeds: 0,
        start: 0,
        ops: 200,
        dir: PathBuf::new(),
        sqlite: true,
        workload: "sd".into(),
        arm: String::new(),
        n: 0,
        v: 0,
        shape: "fan".into(),
        db: PathBuf::new(),
        points: 1000,
        sample: 10,
        seed: 0x6B31_5245_4349_5045,
    };
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--seeds" => a.seeds = val().parse().unwrap_or_else(|_| die("bad --seeds")),
            "--start" => a.start = val().parse().unwrap_or_else(|_| die("bad --start")),
            "--ops" => a.ops = val().parse().unwrap_or_else(|_| die("bad --ops")),
            "--dir" => a.dir = PathBuf::from(val()),
            "--no-sqlite" => a.sqlite = false,
            "--workload" => a.workload = val(),
            "--arm" => a.arm = val(),
            "--n" => a.n = val().parse().unwrap_or_else(|_| die("bad --n")),
            "--v" => a.v = val().parse().unwrap_or_else(|_| die("bad --v")),
            "--shape" => a.shape = val(),
            "--db" => a.db = PathBuf::from(val()),
            "--points" => a.points = val().parse().unwrap_or_else(|_| die("bad --points")),
            "--sample" => a.sample = val().parse().unwrap_or_else(|_| die("bad --sample")),
            "--seed" => a.seed = val().parse().unwrap_or_else(|_| die("bad --seed")),
            other => die(&format!("unknown argument {other}")),
        }
    }
    a
}

fn main() {
    let args = parse_args();
    match args.cmd.as_str() {
        "diff" => {
            if args.seeds == 0 || args.dir.as_os_str().is_empty() {
                die("diff needs --seeds and --dir");
            }
            diff_main(&args)
        }
        "bench" => bench_main(&args),
        "replay" => replay_main(&args),
        other => die(&format!("unknown command {other}")),
    }
}
