//! The merge-floor arms (r11-merge-floor REPORT part 6, adopted by r11-merge PREREG A11) on the fork's own merge.
//!
//!   cargo run -p turso_core --release --example branch_merge_floor
//!
//! Every configuration builds a FRESH trunk: t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, v TEXT), R = 20,000
//! rows, a = id, b = 37*id mod R, v 100 characters; s = 0, 1 or 2 secondary indexes (ia on a, ib on b). The trunk's
//! auto-checkpoint is off and its WAL is truncated before the measured merge, so the merge's commit is the only thing
//! the WAL holds: W_m = (WAL bytes - 32-byte header) / 4,120 exactly (a 24-byte frame header per 4,096-byte page).
//!
//! Arms (one output row per merge):
//!   A1/A2  one branch changes delta rows, CLUSTERED (consecutive ids) or SCATTERED (a random set), by UPDATE
//!          (v rewritten, a += R: its ia entry moves to the right edge; b = (b * 7919) mod R: its ib entry moves to
//!          a random place) or by INSERT (fresh ids); merged by the physical install (page stamps) or the replay
//!          (row stamps). Printed: W_m, the branch's own page count |pages(D_b)|, the floor ceil(delta/c) with
//!          c = rows per table leaf read from the trunk, and the log ratio (WAL bytes / (delta * row bytes)).
//!   A3/chi one branch B changes D_b; K observer branches fork first; then the trunk commits D_t (another branch's
//!          merge) in one of three patterns: NONE, DISJOINT (rows on leaves D_b does not touch) or SAME (other rows
//!          on the very leaves D_b touches); then B merges by replay. Printed: W_m of B's merge and
//!          R_m = arena pages retained by that merge for the observers.
//!   FF     F_r sibling branches fork from one trunk state, write disjoint rows and merge serially (row stamps);
//!          each merge's ff flag is "no trunk commit since its fork". Printed: the ff fraction.
//!
//! A row that disagrees with the harness's own expectation of the trunk after the merge (every changed row read
//! back) prints NOT A RESULT and exits 1.

use std::path::PathBuf;
use std::sync::Arc;

use turso_core::branch::merge::{Install, MergePolicy, Merger, Validation};
use turso_core::branch::Branch;
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};

const R: i64 = 20_000;
const VALUE_LEN: usize = 100;
const FRAME: u64 = 4_096 + 24;
const WAL_HEADER: u64 = 32;
/// Record bytes of one row: header (5 bytes) + a and b (up to 4 bytes each) + v; the rowid lives in the key.
const ROW_BYTES: u64 = 5 + 4 + 4 + VALUE_LEN as u64;

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    std::process::exit(1)
}

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

fn value(tag: &str, n: i64) -> String {
    let head = format!("{tag}{n}-");
    format!("{head}{}", "y".repeat(VALUE_LEN - head.len()))
}

struct Trunk {
    _dir: tempfile::TempDir,
    db: Arc<Database>,
    conn: Arc<Connection>,
    wal: PathBuf,
}

/// Ids are 16 apart so an INSERT can land between any two rows.
const GAP: i64 = 16;

fn trunk(s: usize) -> Trunk {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("floor.db");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.wal_auto_actions_disable();
    conn.execute("PRAGMA synchronous = NORMAL").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, v TEXT)")
        .unwrap();
    if s >= 1 {
        conn.execute("CREATE INDEX ia ON t(a)").unwrap();
    }
    if s >= 2 {
        conn.execute("CREATE INDEX ib ON t(b)").unwrap();
    }
    conn.execute("BEGIN").unwrap();
    {
        let mut ins = conn.prepare("INSERT INTO t VALUES (?1, ?2, ?3, ?4)").unwrap();
        for i in 1..=R {
            ins.bind_at(1.try_into().unwrap(), Value::from_i64(i * GAP)).unwrap();
            ins.bind_at(2.try_into().unwrap(), Value::from_i64(i)).unwrap();
            ins.bind_at(3.try_into().unwrap(), Value::from_i64((37 * i) % R)).unwrap();
            ins.bind_at(4.try_into().unwrap(), Value::from_text(value("t", i))).unwrap();
            ins.run_ignore_rows().unwrap();
            ins.reset().unwrap();
        }
    }
    conn.execute("COMMIT").unwrap();
    let wal = PathBuf::from(format!("{}-wal", path.to_str().unwrap()));
    Trunk {
        _dir: dir,
        db,
        conn,
        wal,
    }
}

impl Trunk {
    fn checkpoint(&self) {
        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        if self.wal_bytes() != 0 {
            not_a_result(&format!("WAL not truncated: {} bytes", self.wal_bytes()));
        }
    }
    fn wal_bytes(&self) -> u64 {
        std::fs::metadata(&self.wal).map_or(0, |m| m.len())
    }
    fn int(&self, sql: &str) -> i64 {
        self.conn.prepare(sql).unwrap().run_collect_rows().unwrap()[0][0]
            .as_int()
            .unwrap()
    }
    fn text(&self, sql: &str) -> Option<String> {
        let rows = self.conn.prepare(sql).unwrap().run_collect_rows().unwrap();
        match rows.as_slice() {
            [] => None,
            [row] => match &row[0] {
                Value::Text(t) => Some(t.as_str().to_string()),
                other => not_a_result(&format!("{sql}: {other:?}")),
            },
            _ => not_a_result(&format!("{sql}: several rows")),
        }
    }
    fn page_count(&self) -> i64 {
        self.int("PRAGMA page_count")
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Shape {
    Clustered,
    Scattered,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Op {
    Upd,
    Ins,
}

/// The ids a branch changes: CLUSTERED consecutive rows starting at the middle of the table; SCATTERED a random
/// set. For INSERT, a fresh id just after each chosen row's id.
fn ids(shape: Shape, op: Op, delta: i64, rng: &mut Rng) -> Vec<i64> {
    let rows: Vec<i64> = match shape {
        Shape::Clustered => (0..delta).map(|j| R / 2 - delta / 2 + j).collect(),
        Shape::Scattered => {
            let mut all: Vec<i64> = (1..=R).collect();
            for i in (1..all.len()).rev() {
                all.swap(i, rng.below(i as u64 + 1) as usize);
            }
            all.truncate(delta as usize);
            all.sort_unstable();
            all
        }
    };
    rows.into_iter()
        .map(|i| match op {
            Op::Upd => i * GAP,
            Op::Ins => i * GAP + 1,
        })
        .collect()
}

/// Apply D_b on `conn` (a branch connection) and return what each id must read afterwards.
fn apply(conn: &Arc<Connection>, op: Op, ids: &[i64], tag: &str) -> Vec<(i64, String)> {
    conn.execute("BEGIN").unwrap();
    let mut expect = Vec::with_capacity(ids.len());
    for &id in ids {
        let v = value(tag, id);
        match op {
            Op::Upd => conn
                .execute(format!(
                    "UPDATE t SET v = '{v}', a = a + {R}, b = (b * 7919) % {R} WHERE id = {id}"
                ))
                .unwrap(),
            Op::Ins => conn
                .execute(format!(
                    "INSERT INTO t VALUES ({id}, {}, {}, '{v}')",
                    id + R,
                    (id * 7919) % R
                ))
                .unwrap(),
        }
        expect.push((id, v));
    }
    conn.execute("COMMIT").unwrap();
    expect
}

fn check(t: &Trunk, expect: &[(i64, String)], what: &str) {
    for (id, v) in expect {
        let got = t.text(&format!("SELECT v FROM t WHERE id = {id}"));
        if got.as_deref() != Some(v.as_str()) {
            not_a_result(&format!("{what}: row {id} reads {got:?} after the merge"));
        }
    }
    let ic = t.text("PRAGMA integrity_check");
    if ic.as_deref() != Some("ok") {
        not_a_result(&format!("{what}: integrity_check {ic:?}"));
    }
}

fn policy(install: Install) -> MergePolicy {
    MergePolicy {
        validation: match install {
            Install::Physical => Validation::PageStamp,
            Install::Replay => Validation::KeyStamp,
        },
        install,
    }
}

fn arm_a1(rng: &mut Rng) {
    println!("A1\top\tshape\ts\tinstall\tdelta\tW_m\tpages_Db\tfloor_ceil_delta_over_c\tc\twal_bytes\tlog_ratio\tcommitted");
    for op in [Op::Upd, Op::Ins] {
        for shape in [Shape::Clustered, Shape::Scattered] {
            for s in 0..=2usize {
                for delta in [1i64, 16, 256, 4096] {
                    for install in [Install::Physical, Install::Replay] {
                        let t = trunk(s);
                        if install == Install::Physical {
                            t.db.set_branch_read_tracking(true);
                        }
                        // Rows per table leaf, from the table alone (s = 0 has no index pages): the harness
                        // prints the c it used; with indexes it is the same table layout.
                        let c = rows_per_leaf();
                        t.checkpoint();
                        let ids = ids(shape, op, delta, rng);
                        let b = t.conn.fork_branch().unwrap();
                        let expect = apply(&b.connect().unwrap(), op, &ids, "b");
                        let mut m = Merger::new(t.conn.clone()).unwrap();
                        let before = t.wal_bytes();
                        let o = m.merge(b, policy(install)).unwrap();
                        let after = t.wal_bytes();
                        if o.refused.is_some() {
                            not_a_result(&format!("A1 {op:?} {shape:?} s={s} {delta} {install:?}: {o:?}"));
                        }
                        check(&t, &expect, "A1");
                        let bytes = after - before;
                        let frames = bytes.saturating_sub(if before == 0 { WAL_HEADER } else { 0 });
                        if frames % FRAME != 0 {
                            not_a_result(&format!("A1: {bytes} WAL bytes is not a whole number of frames"));
                        }
                        let floor = (delta + c - 1) / c;
                        println!(
                            "A1\t{op:?}\t{shape:?}\t{s}\t{install:?}\t{delta}\t{}\t{}\t{floor}\t{c}\t{bytes}\t{:.3}\t{}",
                            frames / FRAME,
                            o.pages_written,
                            bytes as f64 / (delta as u64 * ROW_BYTES) as f64,
                            o.refused.is_none()
                        );
                    }
                }
            }
        }
    }
}

/// Rows per table leaf: a fresh s = 0 trunk's page count minus its interior pages, against R. Page 1 holds the
/// schema; interior pages are ~1 per 450 leaves, so leaves = page_count - 1 - ceil(leaves / 450).
fn rows_per_leaf() -> i64 {
    static C: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *C.get_or_init(|| {
        let t = trunk(0);
        let pages = t.page_count();
        let leaves = pages - 1 - (pages + 449) / 450;
        (R + leaves - 1) / leaves
    })
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Pattern {
    None,
    Disjoint,
    Same,
}

fn arm_a3(rng: &mut Rng) {
    println!("A3\tshape\tpattern\tdelta\tobservers\tW_m\tpages_Db\tR_m\tD_t_rows");
    for shape in [Shape::Clustered, Shape::Scattered] {
        for pattern in [Pattern::None, Pattern::Disjoint, Pattern::Same] {
            for delta in [16i64, 256] {
                let t = trunk(0);
                let ids = ids(shape, Op::Upd, delta, rng);
                // D_t: the rows adjacent to D_b's (same leaves, other rows), or rows a quarter-table away.
                let dt: Vec<i64> = match pattern {
                    Pattern::None => Vec::new(),
                    Pattern::Same => ids
                        .iter()
                        .map(|&id| if (id / GAP) % 2 == 0 { id + GAP } else { id - GAP })
                        .filter(|x| !ids.contains(x))
                        .collect(),
                    Pattern::Disjoint => ids
                        .iter()
                        .map(|&id| ((id / GAP + R / 4 - 1) % R + 1) * GAP)
                        .filter(|x| !ids.contains(x))
                        .collect(),
                };
                t.checkpoint();
                let observers: Vec<Branch> = (0..4).map(|_| t.conn.fork_branch().unwrap()).collect();
                let b = t.conn.fork_branch().unwrap();
                let expect = apply(&b.connect().unwrap(), Op::Upd, &ids, "b");
                let mut m = Merger::new(t.conn.clone()).unwrap();
                let mut expect_t = Vec::new();
                if !dt.is_empty() {
                    let d = t.conn.fork_branch().unwrap();
                    expect_t = apply(&d.connect().unwrap(), Op::Upd, &dt, "d");
                    let o = m.merge(d, policy(Install::Replay)).unwrap();
                    if o.refused.is_some() {
                        not_a_result(&format!("A3 D_t refused: {o:?}"));
                    }
                }
                t.checkpoint();
                let arena0 = t.db.branch_stats().arena_slots_in_use;
                let before = t.wal_bytes();
                let o = m.merge(b, policy(Install::Replay)).unwrap();
                let after = t.wal_bytes();
                let arena1 = t.db.branch_stats().arena_slots_in_use;
                if o.refused.is_some() {
                    not_a_result(&format!("A3 refused: {o:?}"));
                }
                check(&t, &expect, "A3 D_b");
                check(&t, &expect_t, "A3 D_t");
                let frames = (after - before).saturating_sub(WAL_HEADER) / FRAME;
                // B's own pages were freed by the merge (it is released); what the merge retained for the observers
                // is the rest of the arena's change.
                let r_m = arena1 as i64 - arena0 as i64 + o.pages_written as i64;
                println!(
                    "A3\t{shape:?}\t{pattern:?}\t{delta}\t{}\t{frames}\t{}\t{r_m}\t{}",
                    observers.len(),
                    o.pages_written,
                    dt.len()
                );
                drop(observers);
            }
        }
    }
}

fn arm_ff() {
    println!("FF\tF_r\tmerges\tff_merges\tff_fraction\tcommitted");
    for f_r in [1usize, 5, 10] {
        let t = trunk(0);
        let branches: Vec<(Branch, Vec<(i64, String)>)> = (0..f_r)
            .map(|k| {
                let b = t.conn.fork_branch().unwrap();
                let id = (1 + k as i64 * 97) * GAP;
                let e = apply(&b.connect().unwrap(), Op::Upd, &[id], &format!("w{k}"));
                (b, e)
            })
            .collect();
        let mut m = Merger::new(t.conn.clone()).unwrap();
        let (mut ff, mut committed, mut all) = (0, 0, Vec::new());
        for (b, e) in branches {
            let o = m.merge(b, policy(Install::Replay)).unwrap();
            ff += usize::from(o.commits_since_fork == 0);
            committed += usize::from(o.refused.is_none());
            all.extend(e);
        }
        check(&t, &all, "FF");
        println!("FF\t{f_r}\t{f_r}\t{ff}\t{:.4}\t{committed}", ff as f64 / f_r as f64);
    }
}

fn main() {
    println!("# branch_merge_floor: r11-merge PREREG A11 (merge-floor REPORT part 6 arms), R={R}, value_len={VALUE_LEN}");
    println!(
        "# build={}; c (rows per table leaf, derived) = {}",
        if cfg!(debug_assertions) { "DEBUG" } else { "release" },
        rows_per_leaf()
    );
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    arm_a1(&mut rng);
    arm_a3(&mut rng);
    arm_ff();
    println!("# done");
}
