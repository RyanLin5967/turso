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
//!   (k1-adv2, branch mode)
//!   @trunk SQL          run SQL on each arm's trunk connection
//!   @newmain            fork a fresh branch from the trunk and make it `main` (old main reaped)
//!   @hfork CHILD        fork CHILD through the current branch's HANDLE (Branch::fork), switch to it
//!   @reopenall          detach every branch, reopen from disk, re-attach each by id, reconnect
//!   @detach NAME        detach branch NAME's handle (into_id); main's branch is `__main`
//!   @attach NAME        re-attach a detached branch (Database::branch) and reconnect it
//!   @interrupt          Connection::interrupt on the current connection
//! Exit status: 3 when any DIFF was printed, 0 otherwise. A script that ran no statement exits 2.
//!
//! `RECIPE_ADV_BRANCH=1` (lane k1-recipe-build, after recipes became branch-only): both databases
//! are opened with durable branches, a seed table is created on the trunk, and connection `main`
//! is on a branch forked from it, so every script statement runs on a branch. `@reopen` detaches
//! that branch, reopens the database from disk and re-attaches it. A second connection on a
//! branch is refused by the store; `@conn` reports that and stays where it is.

use std::collections::HashMap;
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use turso_core::branch::{Branch, BranchDurability, BranchId};
use turso_core::recipe::{counter, recipe_io};
use turso_core::{
    Connection, Database, DatabaseOpts, Numeric, OpenFlags, PlatformIO, SqliteDialect, Statement,
    StepResult, Value, ValueRef, IO,
};

fn branch_mode() -> bool {
    std::env::var_os("RECIPE_ADV_BRANCH").is_some()
}

fn open_db(path: &Path) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let durability = if branch_mode() {
        BranchDurability::Durable { sync: false }
    } else {
        BranchDurability::Volatile
    };
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new()
            .with_branch_durability(durability)
            .with_attach(true)
            .with_autovacuum(true)
            // k1-adv2: experimental features a script can opt into, identically on both arms.
            .with_custom_types(std::env::var_os("RECIPE_ADV_TYPES").is_some())
            .with_views(std::env::var_os("RECIPE_ADV_VIEWS").is_some())
            .with_vacuum(std::env::var_os("RECIPE_ADV_VACUUM").is_some())
            .with_index_method(std::env::var_os("RECIPE_ADV_IDXM").is_some()),
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
    /// Branch mode: the branch connection `main` lives on, and the trunk connection that seeded it.
    main_branch: Option<BranchId>,
    trunk: Option<Arc<Connection>>,
    /// k1-adv2 `@detach`: branches whose handle was detached (into_id), by name.
    detached: HashMap<String, BranchId>,
}

impl Arm {
    fn new(path: PathBuf, recipe: bool) -> Self {
        for suffix in [
            "", "-wal", "-shm", "-branch-log", "-branch-snap", "-branch-arena", "-branch-cat",
            "-branch-cat-wal",
        ] {
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
            main_branch: None,
            trunk: None,
            detached: HashMap::new(),
        };
        if branch_mode() {
            let t = a.db.as_ref().unwrap().connect().unwrap();
            t.execute("CREATE TABLE __adv_seed(x)").unwrap();
            let b = t.fork_branch().unwrap();
            a.main_branch = Some(b.id());
            a.trunk = Some(t);
            a.attach_main(b);
        } else {
            a.add_trunk_conn("main");
        }
        a
    }
    fn attach_main(&mut self, b: Branch) {
        let c = b.connect().unwrap();
        c.set_recipe_backfill(self.recipe);
        self.conns.insert("main".into(), c);
        self.home.insert("main".into(), Some("__main".into()));
        self.branches.insert("__main".into(), b);
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
        // Branch mode: the main branch outlives the process's handles (durable store).
        if let Some(b) = self.branches.remove("__main") {
            let _ = b.into_id();
        }
        self.branches.clear();
        self.home.clear();
        self.trunk = None;
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
        match self.main_branch {
            Some(id) => {
                let b = self
                    .db
                    .as_ref()
                    .unwrap()
                    .branch(id)
                    .unwrap_or_else(|e| {
                        println!("-- reopen: re-attaching branch {id:?} failed: {e}");
                        std::process::exit(2)
                    });
                self.attach_main(b);
            }
            None => self.add_trunk_conn("main"),
        }
    }

    /// k1-adv2 `@reopenall`: detach EVERY branch (not only main), reopen the database from disk,
    /// re-attach each by id through `Database::branch`, and reconnect every connection that was on
    /// one. Reports each step's outcome instead of exiting, so an arm that cannot re-attach shows as
    /// a DIFF.
    fn reopen_all(&mut self) -> String {
        self.stmts.clear();
        let homes: Vec<(String, Option<String>)> =
            self.home.iter().map(|(n, h)| (n.clone(), h.clone())).collect();
        self.conns.clear();
        self.home.clear();
        self.trunk = None;
        let mut ids: Vec<(String, BranchId)> = Vec::new();
        for (name, b) in self.branches.drain() {
            ids.push((name, b.into_id()));
        }
        ids.sort_by(|a, b| a.0.cmp(&b.0));
        let old = self.db.take().unwrap();
        let weak = Arc::downgrade(&old);
        drop(old);
        if weak.upgrade().is_some() {
            println!("-- reopenall: the old Database is still referenced; not a real reopen");
            std::process::exit(2);
        }
        self.db = Some(open_db(&self.path));
        self.trunk = Some(self.db.as_ref().unwrap().connect().unwrap());
        let mut out = Vec::new();
        for (name, id) in ids {
            match self.db.as_ref().unwrap().branch(id) {
                Ok(b) => {
                    self.branches.insert(name.clone(), b);
                    out.push(format!("{name}:attached"));
                }
                Err(e) => out.push(format!("{name}:attach ERROR {e}")),
            }
        }
        let mut homes = homes;
        homes.sort();
        for (cname, home) in homes {
            match home {
                None => self.add_trunk_conn(&cname),
                Some(bname) => {
                    let Some(b) = self.branches.get(&bname) else {
                        continue;
                    };
                    match b.connect() {
                        Ok(c) => {
                            c.set_recipe_backfill(self.recipe);
                            self.conns.insert(cname.clone(), c);
                            self.home.insert(cname.clone(), Some(bname));
                            out.push(format!("{cname}:connected"));
                        }
                        Err(e) => out.push(format!("{cname}:connect ERROR {e}")),
                    }
                }
            }
        }
        out.join(" ")
    }

    /// The branch-handle name a connection lives on (`main` lives on `__main`).
    fn branch_of(&self, conn: &str) -> Option<String> {
        self.home.get(conn).cloned().flatten()
    }
}

/// Compare two arms' one-line outcomes of a runner command, print, and count a DIFF.
fn report_cmd(line: &str, cur: &str, outs: &[String], ran: &mut u64, diffs: &mut u64) {
    *ran += 1;
    if outs[0] == outs[1] {
        println!("[{cur}] {line}   <same>\n    both  : {}", outs[0]);
    } else {
        *diffs += 1;
        println!(
            "[{cur}] {line}   <DIFF>\n    recipe: {}\n    eager : {}",
            outs[0], outs[1]
        );
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
            // ---- k1-adv2 commands -------------------------------------------------------------
            "@trunk" => {
                // @trunk SQL: run SQL on each arm's trunk connection (branch mode).
                let sql = line.splitn(2, char::is_whitespace).nth(1).unwrap_or("").trim();
                let before = recipe_io();
                let mut outs = Vec::new();
                for arm in [&mut r, &mut e] {
                    if arm.trunk.is_none() {
                        arm.trunk = Some(arm.db.as_ref().unwrap().connect().unwrap());
                    }
                    outs.push(run_sql(arm.trunk.as_ref().unwrap(), sql));
                }
                ran += 1;
                report(line, "trunk", &outs[0], &outs[1], &before, &mut diffs);
                continue;
            }
            "@newmain" => {
                // Fork a fresh branch from the trunk and make it `main`; the old main is reaped.
                let mut outs = Vec::new();
                for arm in [&mut r, &mut e] {
                    arm.stmts.clear();
                    if arm.trunk.is_none() {
                        arm.trunk = Some(arm.db.as_ref().unwrap().connect().unwrap());
                    }
                    let res = arm.trunk.as_ref().unwrap().fork_branch();
                    match res {
                        Ok(b) => {
                            arm.conns.remove("main");
                            if let Some(old) = arm.branches.remove("__main") {
                                let _ = old.reap();
                            }
                            arm.main_branch = Some(b.id());
                            arm.attach_main(b);
                            outs.push("ok".to_string());
                        }
                        Err(err) => outs.push(format!("ERROR {err}")),
                    }
                }
                cur = "main".into();
                report_cmd(line, &cur, &outs, &mut ran, &mut diffs);
                continue;
            }
            "@hfork" => {
                // @hfork CHILD: fork through the Branch HANDLE of the current connection's branch
                // (Branch::fork, not Connection::fork_branch), connect CHILD there, switch to it.
                let child = rest[0].to_string();
                let mut outs = Vec::new();
                for arm in [&mut r, &mut e] {
                    let Some(parent) = arm.branch_of(&cur) else {
                        outs.push("ERROR current connection is on the trunk".to_string());
                        continue;
                    };
                    match arm.branches[&parent].fork() {
                        Ok(b) => match b.connect() {
                            Ok(c) => {
                                c.set_recipe_backfill(arm.recipe);
                                arm.conns.insert(child.clone(), c);
                                arm.home.insert(child.clone(), Some(child.clone()));
                                arm.branches.insert(child.clone(), b);
                                outs.push("ok".to_string());
                            }
                            Err(err) => {
                                arm.branches.insert(child.clone(), b);
                                outs.push(format!("connect ERROR {err}"));
                            }
                        },
                        Err(err) => outs.push(format!("ERROR {err}")),
                    }
                }
                if r.conns.contains_key(&child) && e.conns.contains_key(&child) {
                    cur = child;
                }
                report_cmd(line, &cur, &outs, &mut ran, &mut diffs);
                continue;
            }
            "@reopenall" => {
                let ro = r.reopen_all();
                let eo = e.reopen_all();
                cur = "main".into();
                report_cmd(line, &cur, &[ro, eo], &mut ran, &mut diffs);
                continue;
            }
            "@detach" | "@attach" => {
                // @detach NAME: drop the connections on branch NAME and detach its handle
                // (Branch::into_id); @attach NAME: Database::branch(id) and reconnect NAME. For main
                // use NAME = __main (its connection is `main`).
                let name = rest[0].to_string();
                let cname = if name == "__main" { "main".to_string() } else { name.clone() };
                let mut outs = Vec::new();
                for arm in [&mut r, &mut e] {
                    arm.stmts.clear();
                    if head == "@detach" {
                        arm.conns.remove(&cname);
                        match arm.branches.remove(&name) {
                            Some(b) => {
                                let id = b.into_id();
                                arm.detached.insert(name.clone(), id);
                                outs.push("detached".to_string());
                            }
                            None => outs.push(format!("no branch {name}")),
                        }
                    } else {
                        let Some(id) = arm.detached.remove(&name) else {
                            outs.push(format!("no detached branch {name}"));
                            continue;
                        };
                        match arm.db.as_ref().unwrap().branch(id) {
                            Ok(b) => match b.connect() {
                                Ok(c) => {
                                    c.set_recipe_backfill(arm.recipe);
                                    arm.conns.insert(cname.clone(), c);
                                    arm.home.insert(cname.clone(), Some(name.clone()));
                                    arm.branches.insert(name.clone(), b);
                                    outs.push("attached".to_string());
                                }
                                Err(err) => {
                                    arm.branches.insert(name.clone(), b);
                                    outs.push(format!("connect ERROR {err}"));
                                }
                            },
                            Err(err) => outs.push(format!("attach ERROR {err}")),
                        }
                    }
                }
                if head == "@attach" && r.conns.contains_key(&cname) && e.conns.contains_key(&cname)
                {
                    cur = cname;
                } else if head == "@detach" {
                    cur = "main".into();
                }
                report_cmd(line, &cur, &outs, &mut ran, &mut diffs);
                continue;
            }
            "@interrupt" => {
                // @interrupt: Connection::interrupt on the current connection.
                for arm in [&mut r, &mut e] {
                    arm.conns[&cur].interrupt();
                }
                println!("-- {line}");
                continue;
            }
            // ---- end k1-adv2 commands ---------------------------------------------------------
            "@conn" => {
                let name = rest[0].to_string();
                for arm in [&mut r, &mut e] {
                    if !arm.conns.contains_key(&name) {
                        match arm.home.get(&cur).cloned().flatten() {
                            None => arm.add_trunk_conn(&name),
                            Some(b) => match arm.branches[&b].connect() {
                                Ok(c) => {
                                    c.set_recipe_backfill(arm.recipe);
                                    arm.conns.insert(name.clone(), c);
                                    arm.home.insert(name.clone(), Some(b));
                                }
                                Err(err) => {
                                    println!("-- {line}: a second connection on a branch is refused ({err}); staying on {cur}");
                                    continue;
                                }
                            },
                        }
                    }
                }
                if r.conns.contains_key(&name) && e.conns.contains_key(&name) {
                    cur = name;
                }
                println!("-- {line}");
                continue;
            }
            "@merge" => {
                // Close branch NAME's connection and merge it into the trunk (BaseRead validation).
                use turso_core::branch::merge::{MergePolicy, Merger, Validation};
                let name = rest[0].to_string();
                let mut outs = Vec::new();
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
                    let Some(b) = arm.branches.remove(&name) else {
                        outs.push(format!("no branch {name}"));
                        continue;
                    };
                    let main = arm.conns["main"].clone();
                    let res = Merger::new(main).and_then(|mut m| {
                        m.merge(
                            b,
                            MergePolicy {
                                validation: Validation::BaseRead,
                                keep_merged: false,
                            },
                        )
                    });
                    outs.push(match res {
                        Ok(o) => match o.refused {
                            None => format!("merged rows_changed={}", o.rows_changed),
                            Some(why) => format!(
                                "refused {why:?} scope={:?} install_error={:?}",
                                o.scope, o.install_error
                            ),
                        },
                        Err(err) => format!("ERROR {err}"),
                    });
                }
                cur = "main".into();
                ran += 1;
                if outs[0] == outs[1] {
                    println!("[main] {line}   <same>\n    both  : {}", outs[0]);
                } else {
                    diffs += 1;
                    println!(
                        "[main] {line}   <DIFF>\n    recipe: {}\n    eager : {}",
                        outs[0], outs[1]
                    );
                }
                continue;
            }
            "@blobread" | "@blobwrite" => {
                // @blobread TABLE COL ROWID / @blobwrite TABLE COL ROWID HEX (at offset 0): the
                // incremental blob API (sqlite3_blob_open family) on the current connection.
                let (table, col, rowid) = (rest[0], rest[1], rest[2].parse::<i64>().unwrap());
                let data: Vec<u8> = rest
                    .get(3)
                    .map(|h| {
                        (0..h.len() / 2)
                            .map(|i| u8::from_str_radix(&h[2 * i..2 * i + 2], 16).unwrap())
                            .collect()
                    })
                    .unwrap_or_default();
                let mut outs = Vec::new();
                for arm in [&mut r, &mut e] {
                    let conn = arm.conns[&cur].clone();
                    let res = (|| -> turso_core::Result<String> {
                        let mut b = conn.blob_open(table, col, rowid, head == "@blobwrite")?;
                        let out = if head == "@blobread" {
                            let mut buf = vec![0u8; b.bytes()];
                            b.read(0, &mut buf)?;
                            format!(
                                "bytes={} x'{}'",
                                buf.len(),
                                buf.iter().map(|x| format!("{x:02x}")).collect::<String>()
                            )
                        } else {
                            b.write(0, &data)?;
                            format!("wrote {} bytes", data.len())
                        };
                        b.close()?;
                        Ok(out)
                    })();
                    outs.push(match res {
                        Ok(s) => s,
                        Err(err) => format!("ERROR {err}"),
                    });
                }
                ran += 1;
                if outs[0] == outs[1] {
                    println!("[{cur}] {line}   <same>\n    both  : {}", outs[0]);
                } else {
                    diffs += 1;
                    println!(
                        "[{cur}] {line}   <DIFF>\n    recipe: {}\n    eager : {}",
                        outs[0], outs[1]
                    );
                }
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
                // Eager first, so a recipe-arm open failure (which exits) shows the eager reopen held.
                e.reopen();
                println!("-- @reopen: eager database reopened");
                r.reopen();
                println!("-- @reopen: recipe database reopened");
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
