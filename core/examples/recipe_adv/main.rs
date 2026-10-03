//! Adversarial differential for recipe backfill (lane k1-adv). Runs one script on a RECIPE
//! database (connections with `set_recipe_backfill(true)` unless `@recipe off`) and an EAGER
//! database (recipe backfill never on), statement by statement, and prints every statement whose
//! outcome (rows, error-or-not, changes()) differs between the two as `DIFF`.
//!
//!   recipe_adv <script> <dir>
//!
//! Script: one statement per line; `#` starts a comment line; a line ending in `\` continues.
//!   @conn NAME          switch to connection NAME, creating it on the current branch (trunk at start)
//!   @recipe on|off      recipe flag of the current connection on the RECIPE arm
//!   @fork NAME          fork a branch from the current connection; NAME is the branch and its connection
//!   @reap NAME          close branch NAME's connection and reap it
//!   @close NAME         drop connection NAME (trunk connections)
//!   @reconnect NAME     drop connection NAME and open a fresh one where it was (trunk or branch)
//!   `$SELF` in a SQL line is replaced by each arm's own database path (for ATTACH)
//!   @reopen             drop everything, reopen both databases, connection `main` on the trunk
//!   @prep ID SQL        prepare SQL on the current connection, kept as ID
//!   @bind ID V...       reset ID, bind V... (NULL, 12, 1.5, 'text', x'0a') to ?1.., and run to completion
//!   @open ID V...       reset ID, bind V..., no step
//!   @next ID            step ID once; prints the row or DONE
//!   @drop ID            drop prepared statement ID
//!   @expect-diff        a marker only: echoed, so a reader sees which DIFFs a script targets
//! Exit status: 3 when any DIFF was printed, 0 otherwise. A script that ran no statement exits 2.

use std::collections::HashMap;
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use turso_core::branch::{Branch, BranchDurability};
use turso_core::recipe::{counter, recipe_io};
use turso_core::{
    Connection, Database, DatabaseOpts, Numeric, OpenFlags, PlatformIO, SqliteDialect, Statement,
    StepResult, Value, ValueRef, IO,
};

fn open_db(path: &Path) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new()
            .with_branch_durability(BranchDurability::Volatile)
            .with_attach(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap_or_else(|e| {
        eprintln!("open {} failed: {e}", path.display());
        std::process::exit(2)
    })
}

fn show(v: &Value) -> String {
    match v.as_ref() {
        ValueRef::Null => "NULL".into(),
        ValueRef::Numeric(Numeric::Integer(i)) => format!("{i}"),
        ValueRef::Numeric(Numeric::Float(f)) => {
            let f = f64::from(f);
            format!("{f:?}r")
        }
        ValueRef::Text(t) => format!("'{}'", t.as_str()),
        ValueRef::Blob(b) => format!(
            "x'{}'",
            b.iter().map(|x| format!("{x:02x}")).collect::<String>()
        ),
    }
}

fn parse_val(s: &str) -> Value {
    if s.eq_ignore_ascii_case("NULL") {
        Value::Null
    } else if let Some(t) = s.strip_prefix('\'').and_then(|t| t.strip_suffix('\'')) {
        Value::from_text(t.to_string())
    } else if let Some(h) = s.strip_prefix("x'").and_then(|t| t.strip_suffix('\'')) {
        let bytes = (0..h.len() / 2)
            .map(|i| u8::from_str_radix(&h[2 * i..2 * i + 2], 16).unwrap())
            .collect::<Vec<u8>>();
        Value::from_blob(bytes)
    } else if let Ok(i) = s.parse::<i64>() {
        Value::from_i64(i)
    } else if let Ok(f) = s.parse::<f64>() {
        Value::from_f64(f)
    } else {
        Value::from_text(s.to_string())
    }
}

/// What one arm produced for one statement.
#[derive(PartialEq)]
enum Out {
    Rows(Vec<String>, i64),
    Err(String),
}

impl Out {
    fn same_as(&self, other: &Out) -> bool {
        match (self, other) {
            (Out::Rows(a, ca), Out::Rows(b, cb)) => a == b && ca == cb,
            (Out::Err(_), Out::Err(_)) => true,
            _ => false,
        }
    }
    fn render(&self) -> String {
        match self {
            Out::Rows(rows, ch) => {
                if rows.is_empty() {
                    format!("(no rows) changes={ch}")
                } else {
                    format!("{} changes={ch}", rows.join(" | "))
                }
            }
            Out::Err(e) => format!("ERROR {e}"),
        }
    }
}

fn drive(stmt: &mut Statement, conn: &Arc<Connection>) -> Out {
    let mut rows = Vec::new();
    loop {
        match stmt.step() {
            Ok(StepResult::Done) => return Out::Rows(rows, conn.changes()),
            Ok(StepResult::IO) | Ok(StepResult::Yield) | Ok(StepResult::Sleep { .. }) => {
                if let Err(e) = stmt._io().step() {
                    return Out::Err(e.to_string());
                }
            }
            Ok(StepResult::Row) => {
                let r = stmt.row().unwrap();
                rows.push(
                    r.get_values()
                        .map(show)
                        .collect::<Vec<_>>()
                        .join(", "),
                );
            }
            Ok(other) => return Out::Err(format!("{other:?}")),
            Err(e) => return Out::Err(e.to_string()),
        }
    }
}

fn step_once(stmt: &mut Statement) -> Out {
    loop {
        match stmt.step() {
            Ok(StepResult::Done) => return Out::Rows(vec!["DONE".into()], 0),
            Ok(StepResult::IO) | Ok(StepResult::Yield) | Ok(StepResult::Sleep { .. }) => {
                if let Err(e) = stmt._io().step() {
                    return Out::Err(e.to_string());
                }
            }
            Ok(StepResult::Row) => {
                let r = stmt.row().unwrap();
                return Out::Rows(
                    vec![r.get_values().map(show).collect::<Vec<_>>().join(", ")],
                    0,
                );
            }
            Ok(other) => return Out::Err(format!("{other:?}")),
            Err(e) => return Out::Err(e.to_string()),
        }
    }
}

fn run_sql(conn: &Arc<Connection>, sql: &str) -> Out {
    match conn.prepare(sql) {
        Ok(mut s) => drive(&mut s, conn),
        Err(e) => Out::Err(e.to_string()),
    }
}

struct Arm {
    path: PathBuf,
    db: Option<Arc<Database>>,
    conns: HashMap<String, Arc<Connection>>,
    /// Branch handles by name (the branch's own connection has the same name).
    branches: HashMap<String, Branch>,
    /// Which branch (None = trunk) each connection is on.
    home: HashMap<String, Option<String>>,
    stmts: HashMap<String, Statement>,
    recipe: bool,
}

impl Arm {
    fn new(path: PathBuf, recipe: bool) -> Self {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
        let db = open_db(&path);
        let mut a = Arm {
            path,
            db: Some(db),
            conns: HashMap::new(),
            branches: HashMap::new(),
            home: HashMap::new(),
            stmts: HashMap::new(),
            recipe,
        };
        a.add_trunk_conn("main");
        a
    }
    fn add_trunk_conn(&mut self, name: &str) {
        let c = self.db.as_ref().unwrap().connect().unwrap();
        c.set_recipe_backfill(self.recipe);
        self.conns.insert(name.to_string(), c);
        self.home.insert(name.to_string(), None);
    }
    fn reopen(&mut self) {
        self.stmts.clear();
        self.conns.clear();
        self.branches.clear();
        self.home.clear();
        // The process-wide registry hands back a still-live Database (and its in-memory schema),
        // so the old one must be gone before the open, or nothing is parsed from disk.
        let old = self.db.take().unwrap();
        let weak = Arc::downgrade(&old);
        drop(old);
        if weak.upgrade().is_some() {
            println!("-- reopen: the old Database is still referenced; not a real reopen");
            std::process::exit(2);
        }
        self.db = Some(open_db(&self.path));
        self.add_trunk_conn("main");
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let script = args.next().expect("usage: recipe_adv <script> <dir>");
    let dir = PathBuf::from(args.next().expect("usage: recipe_adv <script> <dir>"));
    std::fs::create_dir_all(&dir).unwrap();
    let text = std::fs::read_to_string(&script).unwrap();
    let mut lines: Vec<String> = Vec::new();
    let mut acc = String::new();
    for raw in text.lines() {
        let l = raw.trim_end();
        if acc.is_empty() && (l.trim_start().starts_with('#') || l.trim().is_empty()) {
            continue;
        }
        if let Some(stripped) = l.strip_suffix('\\') {
            acc.push_str(stripped);
            acc.push('\n');
            continue;
        }
        acc.push_str(l);
        lines.push(std::mem::take(&mut acc));
    }
    let mut r = Arm::new(dir.join("recipe.db"), true);
    let mut e = Arm::new(dir.join("eager.db"), false);
    let mut cur = "main".to_string();
    let mut diffs = 0u64;
    let mut ran = 0u64;
    for line in &lines {
        let line = line.trim();
        let mut words = line.split_whitespace();
        let head = words.next().unwrap_or("");
        let rest: Vec<&str> = words.collect();
        match head {
            "@expect-diff" => {
                println!("-- {line}");
                continue;
            }
            "@conn" => {
                let name = rest[0].to_string();
                for arm in [&mut r, &mut e] {
                    if !arm.conns.contains_key(&name) {
                        match arm.home.get(&cur).cloned().flatten() {
                            None => arm.add_trunk_conn(&name),
                            Some(b) => {
                                let c = arm.branches[&b].connect().unwrap();
                                c.set_recipe_backfill(arm.recipe);
                                arm.conns.insert(name.clone(), c);
                                arm.home.insert(name.clone(), Some(b));
                            }
                        }
                    }
                }
                cur = name;
                println!("-- {line}");
                continue;
            }
            "@reconnect" => {
                // Drop connection NAME and open a fresh one on the same trunk or branch.
                let name = rest[0].to_string();
                for arm in [&mut r, &mut e] {
                    arm.stmts.clear();
                    arm.conns.remove(&name);
                    match arm.home.get(&name).cloned().flatten() {
                        None => arm.add_trunk_conn(&name),
                        Some(b) => {
                            let c = arm.branches[&b].connect().unwrap();
                            c.set_recipe_backfill(arm.recipe);
                            arm.conns.insert(name.clone(), c);
                        }
                    }
                }
                cur = name;
                println!("-- {line} (prepared statements dropped)");
                continue;
            }
            "@recipe" => {
                let on = rest[0] == "on";
                r.conns[&cur].set_recipe_backfill(on);
                println!("-- {line}");
                continue;
            }
            "@fork" => {
                let name = rest[0].to_string();
                let mut outs = Vec::new();
                for arm in [&mut r, &mut e] {
                    match arm.conns[&cur].fork_branch() {
                        Ok(b) => {
                            let c = b.connect().unwrap();
                            c.set_recipe_backfill(arm.recipe);
                            arm.conns.insert(name.clone(), c);
                            arm.home.insert(name.clone(), Some(name.clone()));
                            arm.branches.insert(name.clone(), b);
                            outs.push("ok".to_string());
                        }
                        Err(err) => outs.push(format!("ERROR {err}")),
                    }
                }
                println!("-- {line}: recipe {} eager {}", outs[0], outs[1]);
                cur = name;
                continue;
            }
            "@reap" => {
                let name = rest[0].to_string();
                for arm in [&mut r, &mut e] {
                    let names: Vec<String> = arm
                        .home
                        .iter()
                        .filter(|(_, h)| h.as_deref() == Some(name.as_str()))
                        .map(|(n, _)| n.clone())
                        .collect();
                    for n in names {
                        arm.conns.remove(&n);
                        arm.home.remove(&n);
                    }
                    arm.stmts.clear();
                    if let Some(b) = arm.branches.remove(&name) {
                        if let Err(err) = b.reap() {
                            println!("-- reap error {err}");
                        }
                    }
                }
                cur = "main".into();
                println!("-- {line} (current connection is now main; prepared statements dropped)");
                continue;
            }
            "@close" => {
                let name = rest[0].to_string();
                for arm in [&mut r, &mut e] {
                    arm.conns.remove(&name);
                    arm.home.remove(&name);
                }
                println!("-- {line}");
                continue;
            }
            "@reopen" => {
                r.reopen();
                e.reopen();
                cur = "main".into();
                println!("-- {line}");
                continue;
            }
            "@prep" => {
                let id = rest[0].to_string();
                let sql = line.splitn(3, char::is_whitespace).nth(2).unwrap_or("").trim();
                let mut outs = Vec::new();
                for arm in [&mut r, &mut e] {
                    match arm.conns[&cur].prepare(sql) {
                        Ok(s) => {
                            arm.stmts.insert(id.clone(), s);
                            outs.push("ok".to_string());
                        }
                        Err(err) => outs.push(format!("ERROR {err}")),
                    }
                }
                println!("-- {line}: recipe {} eager {}", outs[0], outs[1]);
                continue;
            }
            "@drop" => {
                r.stmts.remove(rest[0]);
                e.stmts.remove(rest[0]);
                println!("-- {line}");
                continue;
            }
            "@bind" | "@open" | "@next" => {
                let id = rest[0];
                let vals: Vec<Value> = rest[1..].iter().map(|s| parse_val(s)).collect();
                let before = recipe_io();
                let mut outs = Vec::new();
                for arm in [&mut r, &mut e] {
                    let conn = arm.conns[&cur].clone();
                    let Some(s) = arm.stmts.get_mut(id) else {
                        outs.push(Out::Err(format!("no statement {id}")));
                        continue;
                    };
                    if head != "@next" {
                        if let Err(err) = s.reset() {
                            outs.push(Out::Err(err.to_string()));
                            continue;
                        }
                        let mut bad = None;
                        for (i, v) in vals.iter().enumerate() {
                            if let Err(err) = s.bind_at(NonZero::new(i + 1).unwrap(), v.clone()) {
                                bad = Some(err.to_string());
                            }
                        }
                        if let Some(b) = bad {
                            outs.push(Out::Err(b));
                            continue;
                        }
                    }
                    outs.push(match head {
                        "@bind" => drive(s, &conn),
                        "@open" => Out::Rows(vec![], 0),
                        _ => step_once(s),
                    });
                }
                ran += 1;
                report(line, &cur, &outs[0], &outs[1], &before, &mut diffs);
                continue;
            }
            _ => {}
        }
        // `$SELF` is each arm's own database file, so a script can ATTACH the file it is running on.
        let sql_r = line.replace("$SELF", &r.path.display().to_string());
        let sql_e = line.replace("$SELF", &e.path.display().to_string());
        let before = recipe_io();
        let ro = run_sql(&r.conns[&cur], &sql_r);
        let eo = run_sql(&e.conns[&cur], &sql_e);
        ran += 1;
        report(line, &cur, &ro, &eo, &before, &mut diffs);
    }
    println!("SUMMARY statements={ran} diffs={diffs}");
    drop(r);
    drop(e);
    if ran == 0 {
        std::process::exit(2);
    }
    std::process::exit(if diffs > 0 { 3 } else { 0 });
}

fn report(line: &str, cur: &str, ro: &Out, eo: &Out, before: &[u64; 10], diffs: &mut u64) {
    let after = recipe_io();
    let mut notes = Vec::new();
    let installed = after[counter::INSTALLED] - before[counter::INSTALLED];
    let fallbacks = after[counter::FALLBACKS] - before[counter::FALLBACKS];
    let stale = after[counter::STALE_READS] - before[counter::STALE_READS];
    let empty = after[counter::EMPTY_MATCH] - before[counter::EMPTY_MATCH];
    if installed > 0 {
        notes.push(format!("INSTALLED+{installed}"));
    }
    if fallbacks > 0 {
        notes.push(format!("fallback+{fallbacks}"));
    }
    if empty > 0 {
        notes.push(format!("empty-match+{empty}"));
    }
    if stale > 0 {
        notes.push(format!("stale-reads+{stale}"));
    }
    let tag = if ro.same_as(eo) { "same" } else { "DIFF" };
    let one = line.replace('\n', " ");
    println!("[{cur}] {one}   <{tag}> {}", notes.join(" "));
    if tag == "DIFF" {
        *diffs += 1;
        println!("    recipe: {}", ro.render());
        println!("    eager : {}", eo.render());
    } else {
        println!("    both  : {}", ro.render());
        if let (Out::Err(a), Out::Err(b)) = (ro, eo) {
            if a != b {
                println!("    (errors differ in text: recipe {a} / eager {b})");
            }
        }
    }
}
