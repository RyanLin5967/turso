//! Recipe backfill harness (lane k1-recipe-build; artie-research
//! `frontier/round14/k1-recipe-build/PREREG.md`). Integer counters only, except the `ns_*` fields,
//! which are secondary (PREREG §10) and unlocked unless the caller holds the fleet lock.
//!
//!   branch_recipe diff  --seeds S [--start S0] --ops K --dir DIR [--no-sqlite]
//!   branch_recipe bench --workload sd|dc|repair|fr --arm eager|recipe|lazy --n N --v V
//!                       --shape fan|tree|chain --db PATH [--points P] [--sample B] [--seed X]
//!                       [--dedup] [--reap] [--fr F] [--fi F] [--depth D] [--dirty-map]
//!   branch_recipe crash --seeds S [--start S0] --n N --ops K --dir DIR
//!
//! `crash` is PREREG A4.4 (a): per seed a child process runs a W-SD op stream with recipes on, on
//! `Durable { sync: true }` branches, and is SIGKILLed at a seeded point; the parent reopens the
//! database and every live branch must equal an EAGER oracle at the child's last committed op (or,
//! with an op in flight, at the one after it). `K1_RECIPE_MUTANT=MC` must make it differ.
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
    recipes: bool,
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
        c_r.set_recipe_backfill(self.recipes);
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

fn run_diff_seed(
    seed: u64,
    ops: u64,
    dir: &Path,
    use_sqlite: bool,
    recipes: bool,
    totals: &mut DiffStats,
) {
    let sub = dir.join(format!("seed{seed}"));
    let _ = std::fs::remove_dir_all(&sub);
    std::fs::create_dir_all(&sub).unwrap();
    let db_r = open_db(&sub.join("r.db"), BranchDurability::Volatile);
    let db_e = open_db(&sub.join("e.db"), BranchDurability::Volatile);
    let c_r = db_r.connect().unwrap();
    let c_e = db_e.connect().unwrap();
    c_r.set_recipe_backfill(recipes);
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
        recipes,
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
        run_diff_seed(seed, args.ops, &args.dir, args.sqlite, !args.no_recipe, &mut totals);
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
         cache_hits={} empty_match={} sqlite={} recipes={}",
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
        args.sqlite,
        !args.no_recipe
    );
    if totals.ops == 0 || totals.compares == 0 {
        not_a_result("the differential collected nothing");
    }
    if !args.no_recipe && (d(counter::INSTALLED) == 0 || d(counter::STALE_READS) == 0) {
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
        // SoftwareDevOps, macrobench/workflows.py @ 58cf262, M_s = 2 (PREREG P2). "sdx" is the
        // amendment-2 variant (NOT verbatim): each branch's credit_lim backfill differs.
        "sd" | "sdx" => vec![
            ("ddl1".into(), format!("ALTER TABLE customer ADD COLUMN loyalty_tier_{sfx} VARCHAR(8);"), 'D'),
            ("ddl2".into(), format!("ALTER TABLE customer ADD COLUMN credit_lim_{sfx} DECIMAL(10,2);"), 'D'),
            ("dml".into(), format!(
                "UPDATE customer SET
                   loyalty_tier_{sfx} = CASE
                       WHEN c_ytd_payment > 9000 THEN 'Gold'
                       WHEN c_ytd_payment > 5000 THEN 'Silver'
                       ELSE 'Bronze'
                   END,
                   credit_lim_{sfx} = c_credit_lim{};", if workload == "sdx" { format!(" + {step}") } else { String::new() }), 'M'),
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
        "sd" | "sdx" => Some(format!("UPDATE customer SET loyalty_tier_{sfx} = loyalty_tier_{sfx};")),
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

/// This process's CPU time (user + system) in microseconds (getrusage RUSAGE_SELF).
fn cpu_us() -> u64 {
    // SAFETY: getrusage fills the struct it is given; a zeroed rusage is a valid initial value.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        (ru.ru_utime.tv_sec as u64 * 1_000_000 + ru.ru_utime.tv_usec as u64)
            + (ru.ru_stime.tv_sec as u64 * 1_000_000 + ru.ru_stime.tv_usec as u64)
    }
}

/// DEDUP arm (PREREG A4.1): a 128-bit hash of every page image the connection sees.
fn page_hashes(conn: &Arc<Connection>) -> Vec<u128> {
    use std::hash::{Hash, Hasher};
    let rs = rows(conn, "SELECT data FROM sqlite_dbpage")
        .unwrap_or_else(|e| not_a_result(&format!("sqlite_dbpage: {e}")));
    rs.iter()
        .map(|r| {
            let bytes: &[u8] = match r[0].as_ref() {
                ValueRef::Blob(b) => b,
                other => not_a_result(&format!("sqlite_dbpage data is {other:?}")),
            };
            let mut a = std::collections::hash_map::DefaultHasher::new();
            0u8.hash(&mut a);
            bytes.hash(&mut a);
            let mut b = std::collections::hash_map::DefaultHasher::new();
            1u8.hash(&mut b);
            bytes.hash(&mut b);
            ((a.finish() as u128) << 64) | b.finish() as u128
        })
        .collect()
}

/// `--dirty-map`: every page image the connection sees, by page number (128-bit hash).
fn page_map(conn: &Arc<Connection>) -> BTreeMap<u32, u128> {
    use std::hash::{Hash, Hasher};
    let rs = rows(conn, "SELECT pgno, data FROM sqlite_dbpage")
        .unwrap_or_else(|e| not_a_result(&format!("sqlite_dbpage: {e}")));
    rs.iter()
        .map(|r| {
            let p = r[0].as_int().unwrap_or_else(|| not_a_result("sqlite_dbpage pgno")) as u32;
            let bytes: &[u8] = match r[1].as_ref() {
                ValueRef::Blob(b) => b,
                other => not_a_result(&format!("sqlite_dbpage data is {other:?}")),
            };
            let mut a = std::collections::hash_map::DefaultHasher::new();
            0u8.hash(&mut a);
            bytes.hash(&mut a);
            let mut b = std::collections::hash_map::DefaultHasher::new();
            1u8.hash(&mut b);
            bytes.hash(&mut b);
            (p, ((a.finish() as u128) << 64) | b.finish() as u128)
        })
        .collect()
}

fn page_bytes(conn: &Arc<Connection>, p: u32) -> Vec<u8> {
    let rs = rows(conn, &format!("SELECT data FROM sqlite_dbpage WHERE pgno = {p}"))
        .unwrap_or_else(|e| not_a_result(&format!("sqlite_dbpage {p}: {e}")));
    match rs.first().map(|r| r[0].as_ref()) {
        Some(ValueRef::Blob(b)) => b.to_vec(),
        other => not_a_result(&format!("sqlite_dbpage {p}: {other:?}")),
    }
}

fn varint(b: &[u8]) -> (u64, usize) {
    let mut v = 0u64;
    for (i, &x) in b.iter().take(9).enumerate() {
        if i == 8 {
            return ((v << 8) | x as u64, 9);
        }
        v = (v << 7) | (x & 0x7f) as u64;
        if x & 0x80 == 0 {
            return (v, i + 1);
        }
    }
    (v, 9)
}

fn be32(b: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

/// `--dirty-map`: label pages by where they sit, from the page images alone: page 1, sqlite_schema's b-tree (walked
/// from page 1: interior children, leaf cells' overflow chains, by SQLite's file format), the freelist (trunks from
/// header offset 32), and the roots named in sqlite_schema. Anything else is NON-SCHEMA.
fn page_classes(conn: &Arc<Connection>) -> BTreeMap<u32, String> {
    let p1 = page_bytes(conn, 1);
    let page_size = match u16::from_be_bytes([p1[16], p1[17]]) {
        1 => 65536usize,
        n => n as usize,
    };
    let usable = page_size - p1[20] as usize;
    let mut out: BTreeMap<u32, String> = BTreeMap::new();
    let mut stack = vec![1u32];
    while let Some(p) = stack.pop() {
        let b = if p == 1 { p1.clone() } else { page_bytes(conn, p) };
        let off = if p == 1 { 100 } else { 0 };
        let ty = b[off];
        let ncells = u16::from_be_bytes([b[off + 3], b[off + 4]]) as usize;
        let hdr = if ty == 5 { 12 } else { 8 };
        let cell = |i: usize| u16::from_be_bytes([b[off + hdr + 2 * i], b[off + hdr + 2 * i + 1]]) as usize;
        match ty {
            5 => {
                out.insert(p, format!("schema-interior{}", if p == 1 { "(p1)" } else { "" }));
                for i in 0..ncells {
                    stack.push(be32(&b, cell(i)));
                }
                stack.push(be32(&b, off + 8));
            }
            13 => {
                out.insert(p, format!("schema-leaf{}", if p == 1 { "(p1)" } else { "" }));
                for i in 0..ncells {
                    let c = cell(i);
                    let (payload, n1) = varint(&b[c..]);
                    let (_, n2) = varint(&b[c + n1..]);
                    let payload = payload as usize;
                    let x = usable - 35;
                    if payload <= x {
                        continue;
                    }
                    let m = ((usable - 12) * 32 / 255) - 23;
                    let k = m + ((payload - m) % (usable - 4));
                    let local = if k <= x { k } else { m };
                    let mut ov = be32(&b, c + n1 + n2 + local);
                    while ov != 0 {
                        out.insert(ov, "schema-overflow".into());
                        ov = be32(&page_bytes(conn, ov), 0);
                    }
                }
            }
            other => {
                out.insert(p, format!("schema-page-of-type-{other}"));
            }
        }
    }
    let mut trunk = be32(&p1, 32);
    while trunk != 0 {
        let t = page_bytes(conn, trunk);
        out.insert(trunk, "freelist-trunk".into());
        for i in 0..be32(&t, 4) as usize {
            out.insert(be32(&t, 8 + 4 * i), "freelist-leaf".into());
        }
        trunk = be32(&t, 0);
    }
    for r in rows(conn, "SELECT name, rootpage FROM sqlite_schema WHERE rootpage > 0").unwrap_or_default() {
        if let (Some(ValueRef::Text(n)), Some(root)) = (r.first().map(|v| v.as_ref()), r.get(1).and_then(|v| v.as_int())) {
            out.entry(root as u32).or_insert_with(|| format!("root-of-{}", n.as_str()));
        }
    }
    out
}

fn bench_main(args: &Args) {
    let workload = args.workload.as_str();
    let arm = args.arm.as_str();
    if !matches!(arm, "eager" | "recipe" | "lazy" | "alt") {
        die("--arm must be eager, recipe, lazy or alt");
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
         mutant={:?} fr={} fi={} depth={} dedup={} reap={}",
        args.n,
        args.v,
        args.shape,
        args.seed,
        turso_core::recipe::mutant(),
        args.fr,
        args.fi,
        args.depth,
        args.dedup,
        args.reap
    );

    // DEDUP arm: the trunk's page images seed the content-addressed set.
    let mut dedup: HashSet<u128> = HashSet::new();
    let mut dedup_new_per_branch: Vec<usize> = Vec::new();
    if args.dedup {
        for h in page_hashes(&trunk) {
            dedup.insert(h);
        }
        println!("# dedup seed: {} trunk page images, {} distinct", page_count, dedup.len());
    }
    if args.reap && args.shape != "fan" {
        die("--reap needs --shape fan (a reaped parent cannot be forked from)");
    }
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
                if root_children < args.fr {
                    eligible.push(None);
                }
                for (i, d) in depth.iter().enumerate() {
                    if *d < args.depth && children[i] < args.fi {
                        eligible.push(Some(i));
                    }
                }
                if eligible.is_empty() {
                    not_a_result("tree full: F_r, F_i and D cap the tree below --v");
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
        // "alt" interleaves the arms branch by branch for PREREG §10's timed run: even b eager,
        // odd b recipe, on one trunk.
        let recipe_on = match arm {
            "eager" => false,
            "alt" => b % 2 == 1,
            _ => true,
        };
        conn.set_recipe_backfill(recipe_on);
        let thread = match workload {
            "dc" => b % 10,
            _ => b % 5,
        };
        let step = b;
        let owned_before = br.owned_slots().len();
        let mut line = format!(
            "BR b={b} arm_b={} parent={} depth={} thread={thread} step={step}",
            if recipe_on { if arm == "lazy" { "lazy" } else { "recipe" } } else { "eager" },
            parent.map_or(-1, |p| p as i64),
            depth[b as usize]
        );
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
            // --dirty-map snapshots sit outside the statement's counter window, so its counters are unchanged.
            let before = (args.dirty_map && label == "dml").then(|| page_map(&conn));
            let c0 = snap();
            let u0 = cpu_us();
            let t = Instant::now();
            let out = rows(&conn, sql).unwrap_or_else(|e| not_a_result(&format!("branch {b} {label}: {e}: {sql}")));
            let ns = t.elapsed().as_nanos();
            let cpu = cpu_us() - u0;
            let c1 = snap();
            let dd = |i: usize| c1.0[i] - c0.0[i];
            line.push_str(&format!(
                " {label}_fetch={} {label}_dirty={} {label}_evals={} {label}_installed={} {label}_ns={ns} {label}_cpu_us={cpu}",
                dd(counter::PAGE_FETCH),
                dd(counter::BRANCH_DIRTY),
                dd(counter::RECIPE_EVALS),
                dd(counter::INSTALLED),
            ));
            if kind == &'M' {
                line.push_str(&format!(" {label}_changes={}", conn.changes()));
            }
            if let Some(before) = before {
                let after = page_map(&conn);
                let mut changed: Vec<u32> =
                    after.iter().filter(|(p, h)| before.get(p) != Some(h)).map(|(p, _)| *p).collect();
                changed.extend(before.keys().filter(|p| !after.contains_key(p)));
                let classes = page_classes(&conn);
                let labels: Vec<String> = changed
                    .iter()
                    .map(|p| format!("{p}:{}", classes.get(p).map_or("NON-SCHEMA", |c| c.as_str())))
                    .collect();
                line.push_str(&format!(
                    " {label}_changed_n={} {label}_changed={} schema_pages={}",
                    changed.len(),
                    labels.join(","),
                    classes.values().filter(|c| c.starts_with("schema")).count()
                ));
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
        if args.dedup {
            let hs = page_hashes(&conn);
            let seen = hs.len();
            let mut new = 0usize;
            for h in hs {
                if dedup.insert(h) {
                    new += 1;
                }
            }
            dedup_new_per_branch.push(new);
            line.push_str(&format!(" dedup_pages={seen} dedup_new={new} dedup_set={}", dedup.len()));
        }
        println!("{line}");
        // Point reads on sampled branches (KC3), on a fresh connection.
        if b % sample_every == 0 && args.points > 0 {
            drop(conn);
            let pc = br.connect().unwrap();
            pc.set_recipe_backfill(recipe_on);
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
        if args.reap {
            br.reap().unwrap_or_else(|e| not_a_result(&format!("reap {b}: {e}")));
        } else {
            handles.push(br);
        }
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
    let branches_run = per_branch_owned.len();
    if args.dedup {
        let total: usize = dedup_new_per_branch.iter().sum();
        let half = &dedup_new_per_branch[dedup_new_per_branch.len() / 2..];
        let marginal = half.iter().sum::<usize>() as f64 / half.len().max(1) as f64;
        println!(
            "DEDUP branches={} new_pages_total={total} amortised={:.3} marginal_second_half={marginal:.3} first={} set={}",
            dedup_new_per_branch.len(),
            total as f64 / dedup_new_per_branch.len().max(1) as f64,
            dedup_new_per_branch.first().copied().unwrap_or(0),
            dedup.len()
        );
    }
    let io = recipe_io();
    println!(
        "SUMMARY workload={workload} arm={arm} n={} v={} shape={} branches={} sum_owned={total_owned} \
         sum_owned_end={final_total} min_owned={min_owned} max_owned={max_owned} mean_owned={:.3} \
         arena_in_use={} arena_free={} live_branches={} point_lines={point_lines} installed={} fallbacks={} \
         recipe_evals={} stale_reads={} result_hash={hh:016x} bench_ns={bench_ns} page_io={:?} reaped={}",
        args.n,
        args.v,
        args.shape,
        branches_run,
        total_owned as f64 / branches_run.max(1) as f64,
        st.arena_slots_in_use,
        st.arena_slots_free,
        st.live_branches,
        io[counter::INSTALLED],
        io[counter::FALLBACKS],
        io[counter::RECIPE_EVALS],
        io[counter::STALE_READS],
        turso_core::branch::page_io(),
        args.reap,
    );
    if branches_run == 0 || total_owned == 0 {
        not_a_result("no branch owned any page: the instrument read nothing");
    }
    if arm != "eager" && (workload == "sd" || workload == "sdx") && io[counter::INSTALLED] == 0 {
        not_a_result("recipe arm installed no recipe");
    }
    if arm == "alt" && (io[counter::INSTALLED] == 0 || io[counter::INSTALLED] as usize > branches_run) {
        not_a_result("alt arm: recipe branches installed no recipe, or eager ones did");
    }
    if arm == "eager" && io[counter::INSTALLED] != 0 {
        not_a_result("eager arm installed a recipe");
    }
    if args.dedup {
        // Fire-check (A4.1), after SUMMARY so it moves none of its counters: a branch that changed
        // nothing adds no page image. One probe forked from the trunk, one from the last measured
        // branch when it is still held.
        let mut probes = vec![(
            "trunk",
            trunk
                .fork_branch()
                .unwrap_or_else(|e| not_a_result(&format!("dedup probe fork: {e}"))),
        )];
        if let Some(last) = handles.last() {
            probes.push((
                "last",
                last.fork()
                    .unwrap_or_else(|e| not_a_result(&format!("dedup probe fork: {e}"))),
            ));
        }
        for (what, probe) in probes {
            let pc = probe.connect().unwrap();
            let hs = page_hashes(&pc);
            drop(pc);
            let seen = hs.len();
            let new = hs.iter().filter(|h| !dedup.contains(h)).count();
            println!("DEDUP-FIRECHECK probe={what} pages={seen} new={new}");
            if seen == 0 || new != 0 {
                not_a_result(&format!("dedup fire-check: an unchanged {what} branch read {seen} pages, {new} new"));
            }
            probe
                .reap()
                .unwrap_or_else(|e| not_a_result(&format!("dedup probe reap: {e}")));
        }
    }
    drop(handles);
}

// ---------------------------------------------------------------------------------------------
// crash (PREREG A4.4 (a))
// ---------------------------------------------------------------------------------------------

/// One op of a crash seed's stream. Node 0 is the trunk; node k is the branch the k-th `Fork` made.
#[derive(Clone, Debug)]
enum COp {
    Fork { parent: usize },
    Sql { node: usize, sql: String },
    Reap { node: usize },
}

/// The op stream of one crash seed: W-SD steps (each statement its own op, so a kill can land
/// between the ALTERs and the backfill), repairs, point writes of recipe inputs and targets,
/// INSERTs, DELETEs, trunk writes, forks of forks and reaps of leaves.
fn crash_ops(seed: u64, n: i64, len: usize) -> Vec<COp> {
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xC4A5);
    let mut ops = Vec::new();
    let mut alive = vec![true];
    let mut parent: Vec<Option<usize>> = vec![None];
    let mut cols: Vec<Vec<String>> = vec![Vec::new()];
    let mut next_id = n + 1;
    while ops.len() < len {
        let live: Vec<usize> = (1..alive.len()).filter(|&i| alive[i]).collect();
        let roll = rng.below(100);
        if live.is_empty() || (roll < 12 && live.len() < 6) {
            let mut from = vec![0];
            from.extend(&live);
            let p = *rng.pick(&from);
            ops.push(COp::Fork { parent: p });
            alive.push(true);
            parent.push(Some(p));
            cols.push(cols[p].clone());
            continue;
        }
        let node = *rng.pick(&live);
        let k = rng.range(1, n + 20);
        match roll {
            0..=39 => {
                let sfx = format!("t{node}_s{}", ops.len());
                let salt = if rng.chance(50) { format!(" + {}", ops.len()) } else { String::new() };
                ops.push(COp::Sql { node, sql: format!("ALTER TABLE customer ADD COLUMN loyalty_tier_{sfx} VARCHAR(8)") });
                ops.push(COp::Sql { node, sql: format!("ALTER TABLE customer ADD COLUMN credit_lim_{sfx} DECIMAL(10,2)") });
                ops.push(COp::Sql {
                    node,
                    sql: format!(
                        "UPDATE customer SET loyalty_tier_{sfx} = CASE WHEN c_ytd_payment > 9000 THEN 'Gold' \
                         WHEN c_ytd_payment > 5000 THEN 'Silver' ELSE 'Bronze' END, credit_lim_{sfx} = c_credit_lim{salt}"
                    ),
                });
                cols[node].push(format!("loyalty_tier_{sfx}"));
                cols[node].push(format!("credit_lim_{sfx}"));
            }
            40..=47 => {
                let wh = if rng.chance(50) {
                    format!(" WHERE c_ytd_payment > {}", rng.range(0, 9) * 1000)
                } else {
                    String::new()
                };
                ops.push(COp::Sql { node, sql: format!("UPDATE customer SET c_balance = c_balance - 72.50{wh}") });
            }
            48..=59 => ops.push(COp::Sql {
                node,
                sql: format!("UPDATE customer SET c_ytd_payment = {} WHERE rowid = {k}", rng.range(0, 10) * 1000),
            }),
            60..=69 => {
                let c = if cols[node].is_empty() { "c_balance".to_string() } else { rng.pick(&cols[node]).clone() };
                ops.push(COp::Sql { node, sql: format!("UPDATE customer SET {c} = 'Z{}' WHERE rowid = {k}", rng.below(9)) });
            }
            70..=77 => {
                ops.push(COp::Sql {
                    node,
                    sql: format!(
                        "INSERT INTO customer (c_id, c_d_id, c_w_id, c_credit_lim, c_balance, c_ytd_payment, c_data) \
                         VALUES ({next_id}, 1, 99, 50000.00, -10.00, {}, '{}')",
                        rng.range(0, 9) * 1000,
                        rng.alpha(40)
                    ),
                });
                next_id += 1;
            }
            78..=85 => ops.push(COp::Sql { node, sql: format!("DELETE FROM customer WHERE rowid = {k}") }),
            86..=92 => ops.push(COp::Sql {
                node: 0,
                sql: format!("UPDATE customer SET c_data = '{}' WHERE rowid = {k}", rng.alpha(30)),
            }),
            _ => {
                let leaves: Vec<usize> = live
                    .iter()
                    .copied()
                    .filter(|&b| !(1..alive.len()).any(|c| alive[c] && parent[c] == Some(b)))
                    .collect();
                if let Some(&b) = leaves.first() {
                    ops.push(COp::Reap { node: b });
                    alive[b] = false;
                }
            }
        }
    }
    ops
}

/// A database the crash stream runs on: the child's (RECIPE, durable) or the oracle's (EAGER).
/// Fields drop in order: connections before the handles that would release their branches.
struct CrashDb {
    conns: Vec<Option<Arc<Connection>>>,
    trunk: Arc<Connection>,
    handles: Vec<Option<Branch>>,
    db: Arc<Database>,
    recipes: bool,
}

impl CrashDb {
    fn new(db: Arc<Database>, recipes: bool) -> Self {
        let trunk = db.connect().unwrap();
        Self {
            db,
            trunk,
            handles: vec![None],
            conns: vec![None],
            recipes,
        }
    }

    fn conn(&self, node: usize) -> Result<&Arc<Connection>, String> {
        if node == 0 {
            Ok(&self.trunk)
        } else {
            self.conns[node].as_ref().ok_or_else(|| format!("node {node} has no connection"))
        }
    }

    /// Apply one op. Ok carries a note for the progress log (a fork's branch id, a statement's
    /// changes()); Err is the engine's refusal, which the oracle must reproduce. A failed fork
    /// still takes its node number, so later ops address the same nodes in every database.
    fn apply(&mut self, op: &COp) -> Result<String, String> {
        match op {
            COp::Fork { parent } => {
                let made = (|| {
                    let br = if *parent == 0 {
                        self.trunk.fork_branch()
                    } else {
                        self.handles[*parent]
                            .as_ref()
                            .ok_or_else(|| turso_core::LimboError::InternalError("parent gone".into()))?
                            .fork()
                    }?;
                    let c = br.connect()?;
                    c.set_recipe_backfill(self.recipes);
                    Ok::<_, turso_core::LimboError>((br, c))
                })();
                match made {
                    Ok((br, c)) => {
                        let id = br.id().0;
                        self.handles.push(Some(br));
                        self.conns.push(Some(c));
                        Ok(format!("id={id}"))
                    }
                    Err(e) => {
                        self.handles.push(None);
                        self.conns.push(None);
                        Err(e.to_string())
                    }
                }
            }
            COp::Sql { node, sql } => {
                let c = self.conn(*node)?;
                c.execute(sql).map_err(|e| e.to_string())?;
                Ok(format!("changes={}", c.changes()))
            }
            COp::Reap { node } => {
                // The connection goes first: a reap waits for open connections.
                self.conns[*node] = None;
                let br = self.handles[*node].take().ok_or("reap of a node with no handle")?;
                let id = br.id().0;
                br.reap().map_err(|e| e.to_string())?;
                Ok(format!("id={id}"))
            }
        }
    }

    /// Every live node's content: (columns, rows, hash) of `SELECT * FROM customer ORDER BY rowid`.
    fn state(&self) -> BTreeMap<usize, (usize, usize, u64)> {
        let mut out = BTreeMap::new();
        for node in 0..self.conns.len() {
            if let Ok(c) = self.conn(node) {
                out.insert(node, content(c).unwrap_or_else(|e| not_a_result(&format!("oracle read: {e}"))));
            }
        }
        out
    }
}

fn content(conn: &Arc<Connection>) -> Result<(usize, usize, u64), String> {
    let rs = rows(conn, "SELECT * FROM customer ORDER BY rowid").map_err(|e| e.to_string())?;
    Ok((rs.first().map_or(0, |r| r.len()), rs.len(), result_hash(&rs)))
}

/// What the child left on disk, reopened: each live node's content, keyed by the node the child's
/// progress log names (`ids`: branch id to node). `in_flight_fork` is the node an unlogged fork
/// would have made. Returns the connections too, for the post-reopen writes (each before its
/// handle, so a connection drops first).
#[allow(clippy::type_complexity)]
fn crash_recover(
    path: &Path,
    ids: &BTreeMap<u64, usize>,
    in_flight_fork: Option<usize>,
) -> Result<(Arc<Database>, BTreeMap<usize, (usize, usize, u64)>, BTreeMap<usize, (Arc<Connection>, Branch)>), String> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new().with_branch_durability(BranchDurability::Durable { sync: true }),
        None,
        Arc::new(SqliteDialect),
    )
    .map_err(|e| format!("reopen: {e}"))?;
    let trunk = db.connect().map_err(|e| format!("trunk connect: {e}"))?;
    let mut state = BTreeMap::new();
    state.insert(0, content(&trunk).map_err(|e| format!("trunk read: {e}"))?);
    let mut live = BTreeMap::new();
    let mut unknown = Vec::new();
    for id in db.branch_ids().map_err(|e| format!("branch_ids: {e}"))? {
        unknown.push(id);
    }
    unknown.retain(|id| {
        let Some(&node) = ids.get(&id.0) else { return true };
        live.insert(node, *id);
        false
    });
    match (unknown.as_slice(), in_flight_fork) {
        ([], _) => {}
        ([id], Some(node)) => {
            live.insert(node, *id);
        }
        (more, _) => return Err(format!("recovered branch ids the child never logged: {more:?}")),
    }
    let mut conns = BTreeMap::new();
    for (node, id) in live {
        let br = db.branch(id).map_err(|e| format!("attach {}: {e}", id.0))?;
        let c = br.connect().map_err(|e| format!("connect {}: {e}", id.0))?;
        c.set_recipe_backfill(true);
        state.insert(node, content(&c).map_err(|e| format!("node {node} read: {e}"))?);
        conns.insert(node, (c, br));
    }
    drop(trunk);
    Ok((db, state, conns))
}

fn crash_child(args: &Args) {
    let ops = crash_ops(args.seed, args.n, args.ops as usize);
    let db = open_db(&args.dir.join("c.db"), BranchDurability::Durable { sync: true });
    let mut cdb = CrashDb::new(db, true);
    let mut log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(args.dir.join("progress.log"))
        .unwrap_or_else(|e| die(&format!("progress log: {e}")));
    // write(2) per line: a SIGKILL loses no byte a returned write handed the kernel.
    let mut say = |s: String| {
        log.write_all(format!("{s}\n").as_bytes())
            .unwrap_or_else(|e| die(&format!("progress log: {e}")))
    };
    say("start 0 load".into());
    load_customer(&cdb.trunk, args.n, 0, &mut Rng(args.seed ^ 0x10AD));
    say("done 0 ok".into());
    for (i, op) in ops.iter().enumerate() {
        let i = i + 1;
        say(format!("start {i}"));
        let before = recipe_io()[counter::INSTALLED];
        let r = cdb.apply(op);
        let installed = recipe_io()[counter::INSTALLED] - before;
        match r {
            Ok(note) => say(format!("done {i} ok {note} installed={installed}")),
            Err(e) => say(format!("done {i} err installed={installed} {}", e.replace('\n', " "))),
        }
    }
    say("end".into());
    // exit() runs no destructors: dropping a Branch handle would release the branch.
    std::process::exit(0);
}

/// Replay ops 1..=upto on a fresh EAGER database; return it with each op's outcome (true = Ok).
fn crash_oracle(args: &Args, seed: u64, ops: &[COp], upto: usize, dir: &Path) -> (CrashDb, Vec<bool>) {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let db = open_db(&dir.join("o.db"), BranchDurability::Volatile);
    let mut o = CrashDb::new(db, false);
    load_customer(&o.trunk, args.n, 0, &mut Rng(seed ^ 0x10AD));
    let mut outcomes = Vec::new();
    for op in &ops[..upto] {
        outcomes.push(o.apply(op).is_ok());
    }
    (o, outcomes)
}

fn crash_main(args: &Args) {
    let mutant = std::env::var("K1_RECIPE_MUTANT").unwrap_or_else(|_| "none".into());
    std::fs::create_dir_all(&args.dir).unwrap();
    let exe = std::env::current_exe().unwrap();
    let (mut killed, mut mid_op, mut matched_a, mut matched_b, mut mismatches) = (0u64, 0u64, 0u64, 0u64, 0u64);
    let (mut with_recipe, mut post_mismatches, mut ran_to_end) = (0u64, 0u64, 0u64);
    println!(
        "# crash seeds={} start={} n={} ops={} mutant={mutant} durability=Durable{{sync:true}} \
         (SIGKILL is a process crash: kernel buffers survive it; power loss is not tested)",
        args.seeds, args.start, args.n, args.ops
    );
    for seed in args.start..args.start + args.seeds {
        let sub = args.dir.join(format!("seed{seed}"));
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();
        let ops = crash_ops(seed, args.n, args.ops as usize);
        let mut krng = Rng(seed ^ 0x5EED_C4A5);
        // 1..=len kills after op `target` starts; len + 1 lets the child run to its end.
        let target = 1 + krng.below(ops.len() as u64 + 1) as usize;
        let delay_us = if krng.chance(50) { krng.below(200) } else { krng.below(3_000) };
        let out = std::fs::File::create(sub.join("child.txt")).unwrap();
        let mut child = std::process::Command::new(&exe)
            .args(["crash-child", "--seed", &seed.to_string(), "--n", &args.n.to_string()])
            .args(["--ops", &args.ops.to_string(), "--dir", sub.to_str().unwrap()])
            .stdout(out.try_clone().unwrap())
            .stderr(out)
            .spawn()
            .unwrap_or_else(|e| die(&format!("spawn child: {e}")));
        let progress = sub.join("progress.log");
        let want = format!("start {target}\n");
        let status = loop {
            if let Some(st) = child.try_wait().unwrap() {
                break Some(st);
            }
            if std::fs::read_to_string(&progress).map_or(false, |s| s.contains(&want)) {
                std::thread::sleep(std::time::Duration::from_micros(delay_us));
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
            std::thread::sleep(std::time::Duration::from_micros(200));
        };
        let was_killed = match status {
            None => true,
            Some(st) if st.success() => false,
            Some(st) => not_a_result(&format!("seed {seed}: child exited {st}; see {}", sub.join("child.txt").display())),
        };
        let text = std::fs::read_to_string(&progress).unwrap_or_default();
        let mut last_done = None;
        let mut started = 0usize;
        let mut ids: BTreeMap<u64, usize> = BTreeMap::new();
        let mut child_ok: Vec<bool> = Vec::new();
        let mut recipes_before = 0u64;
        let mut next_node = 1usize;
        for line in text.lines() {
            let mut w = line.split_whitespace();
            match (w.next(), w.next().and_then(|x| x.parse::<usize>().ok())) {
                (Some("start"), Some(i)) => started = started.max(i),
                (Some("done"), Some(i)) => {
                    last_done = Some(i);
                    if i == 0 {
                        continue;
                    }
                    let ok = w.next() == Some("ok");
                    child_ok.push(ok);
                    if line.contains("installed=1") {
                        recipes_before += 1;
                    }
                    if let COp::Fork { .. } = ops[i - 1] {
                        if ok {
                            let id: u64 = line.split("id=").nth(1).and_then(|x| x.split_whitespace().next())
                                .and_then(|x| x.parse().ok())
                                .unwrap_or_else(|| not_a_result(&format!("seed {seed}: fork line without id: {line}")));
                            ids.insert(id, next_node);
                        }
                        next_node += 1;
                    }
                }
                _ => {}
            }
        }
        let Some(last_done) = last_done else {
            not_a_result(&format!("seed {seed}: no op committed, not even the load (target {target})"));
        };
        let in_flight = (started > last_done).then_some(last_done + 1);
        if was_killed {
            killed += 1;
        } else {
            ran_to_end += 1;
        }
        if in_flight.is_some() {
            mid_op += 1;
        }
        if recipes_before > 0 {
            with_recipe += 1;
        }
        // Reopen what the child left. A reopen or read that fails is a difference, not an abort.
        let in_flight_fork = in_flight.filter(|&f| matches!(ops[f - 1], COp::Fork { .. })).map(|_| next_node);
        let recovered = crash_recover(&sub.join("c.db"), &ids, in_flight_fork);
        let mut verdict = "NONE";
        let mut oracle = None;
        match &recovered {
            Err(e) => println!("  seed={seed} recovery failed: {e}"),
            Ok((_, rec_state, _)) => {
                // The oracle at the last committed op, else (an op in flight) at the one after it.
                for (label, upto) in [("A", Some(last_done)), ("B", in_flight)] {
                    let Some(upto) = upto else { continue };
                    let (o, outcomes) = crash_oracle(args, seed, &ops, upto, &sub.join(format!("oracle_{label}")));
                    let k = child_ok.len().min(upto);
                    if outcomes[..k] != child_ok[..k] {
                        println!("  seed={seed} op outcomes differ (child {child_ok:?} oracle {outcomes:?})");
                        break;
                    }
                    let st = o.state();
                    if &st == rec_state {
                        verdict = label;
                        oracle = Some(o);
                        break;
                    }
                    if label == "B" || in_flight.is_none() {
                        for (node, want) in &st {
                            let got = rec_state.get(node);
                            if got != Some(want) {
                                println!("  seed={seed} oracle_{label} node={node} want (cols, rows, hash)={want:?} got={got:?}");
                            }
                        }
                        for node in rec_state.keys().filter(|n| !st.contains_key(n)) {
                            println!("  seed={seed} oracle_{label} node={node} recovered but not live in the oracle");
                        }
                    }
                }
            }
        }
        // After reopen: a point write and a new recipe on every recovered branch, compared again.
        let mut post = "skipped";
        if let (Some(o), Ok((_, _, conns))) = (&oracle, &recovered) {
            post = "ok";
            for (&node, (c, _)) in conns {
                let oc = o.conn(node).unwrap_or_else(|e| not_a_result(&e));
                for sql in ["UPDATE customer SET c_balance = 7 WHERE rowid = 3", "UPDATE customer SET c_balance = c_balance + 1"] {
                    let r = c.execute(sql).map(|_| c.changes()).map_err(|e| e.to_string());
                    let e = oc.execute(sql).map(|_| oc.changes()).map_err(|e| e.to_string());
                    if r != e {
                        post = "MISMATCH";
                        println!("  seed={seed} post-reopen node={node} {sql}: recovered {r:?} oracle {e:?}");
                    }
                }
                if content(c) != content(oc) {
                    post = "MISMATCH";
                    println!("  seed={seed} post-reopen node={node}: content differs after the post-reopen writes");
                }
            }
        }
        match verdict {
            "A" => matched_a += 1,
            "B" => matched_b += 1,
            _ => mismatches += 1,
        }
        if post == "MISMATCH" {
            post_mismatches += 1;
        }
        println!(
            "CRASH seed={seed} ops={} target={target} delay_us={delay_us} killed={was_killed} last_done={last_done} \
             in_flight={} in_flight_op={} recipes_installed_before={recipes_before} branches_recovered={} \
             matched={verdict} post_reopen={post}",
            ops.len(),
            in_flight.map_or("-".to_string(), |f| f.to_string()),
            in_flight.map_or("-".to_string(), |f| match &ops[f - 1] {
                COp::Fork { .. } => "fork".to_string(),
                COp::Reap { .. } => "reap".to_string(),
                COp::Sql { sql, .. } => sql.split_whitespace().next().unwrap_or("?").to_string(),
            }),
            recovered.as_ref().map_or(0, |(_, _, c)| c.len())
        );
        drop(oracle);
        drop(recovered);
        let _ = std::fs::remove_dir_all(&sub);
    }
    println!(
        "CRASHSUM mutant={mutant} seeds={} killed={killed} ran_to_end={ran_to_end} killed_mid_op={mid_op} \
         seeds_with_recipe_before_kill={with_recipe} matched_last_done={matched_a} matched_in_flight={matched_b} \
         mismatches={mismatches} post_reopen_mismatches={post_mismatches}",
        args.seeds
    );
    if killed == 0 || mid_op == 0 || with_recipe == 0 {
        not_a_result("the crash run collected nothing: no kill, no mid-op kill, or no recipe before a kill");
    }
    if mismatches + post_mismatches > 0 {
        std::process::exit(3);
    }
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
    /// diff: run the RECIPE arm with recipes off too (the BASE control's mode, PREREG §3).
    no_recipe: bool,
    /// bench: DEDUP arm (A4.1), reap each branch after its step (A4.3), tree fanouts and depth.
    dedup: bool,
    reap: bool,
    /// bench: page numbers the dml statement changed, labelled by b-tree (needs --features cli_only).
    dirty_map: bool,
    fr: u64,
    fi: u64,
    depth: u64,
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
        no_recipe: false,
        dedup: false,
        reap: false,
        dirty_map: false,
        fr: 5,
        fi: 3,
        depth: 4,
    };
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--seeds" => a.seeds = val().parse().unwrap_or_else(|_| die("bad --seeds")),
            "--start" => a.start = val().parse().unwrap_or_else(|_| die("bad --start")),
            "--ops" => a.ops = val().parse().unwrap_or_else(|_| die("bad --ops")),
            "--dir" => a.dir = PathBuf::from(val()),
            "--no-sqlite" => a.sqlite = false,
            "--no-recipe" => a.no_recipe = true,
            "--dedup" => a.dedup = true,
            "--reap" => a.reap = true,
            "--dirty-map" => a.dirty_map = true,
            "--fr" => a.fr = val().parse().unwrap_or_else(|_| die("bad --fr")),
            "--fi" => a.fi = val().parse().unwrap_or_else(|_| die("bad --fi")),
            "--depth" => a.depth = val().parse().unwrap_or_else(|_| die("bad --depth")),
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
        "crash" => {
            if args.seeds == 0 || args.dir.as_os_str().is_empty() || args.n < 1 {
                die("crash needs --seeds, --dir and --n");
            }
            crash_main(&args)
        }
        "crash-child" => crash_child(&args),
        "replay" => replay_main(&args),
        other => die(&format!("unknown command {other}")),
    }
}
