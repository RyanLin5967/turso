//! Catalog mode (r11-restart lane; a PROTOTYPE of the published fixes for an open that grows with
//! the number of branches). ⚠ Unreviewed.
//!
//! In snapshot mode ([`super::BranchDurability::Durable`]) the branch store checkpoints by writing
//! ALL live branch state into `<db>-branch-snap`, and every open reads that file and the log back
//! whole and rebuilds every branch in memory: open, recovery and each compaction are Θ(live
//! state). Catalog mode ([`super::BranchDurability::Catalog`]) keeps the operation log exactly as
//! it is — same records, same flush order, same replay through the store's `apply_*` code — and
//! replaces the snapshot with four published mechanisms:
//!
//! * **a persistent branch index read in place** (ZFS's MOS: the pool's object set, indexed by
//!   object number and read on demand, not replayed): `<db>-branch-cat`, a Turso database whose
//!   B-trees hold one row per branch, one per page a branch owns and one per retained version. Its
//!   own WAL, crash recovery and page cache are Turso's;
//! * **on-demand recovery** (Graefe & Sauer, "instant restart"): an open reads the catalog's meta
//!   row and replays the log's tail; a branch's state is read the first time something touches
//!   it, and a trunk page's retained versions the first time that page is resolved or written;
//! * **an incremental checkpoint** (ARIES; Turso's own WAL checkpoint underneath): a checkpoint
//!   writes only the branches and trunk pages changed since the last one, in one catalog
//!   transaction, then starts the log over. The log is therefore bounded by a constant
//!   (`COMPACT_MIN_LOG_BYTES`), not by twice the live state;
//! * **a persistent free-space table** (LMDB's freelist) in place of the reachability sweep over
//!   the arena at open.
//!
//! # Crash consistency
//!
//! The catalog holds the state as of generation `g` (its meta row); the log holds generation `g`'s
//! records since. A checkpoint commits the catalog at `g + 1` and only then starts the log over at
//! `g + 1`, so a crash between the two leaves an older-generation log, which recovery ignores —
//! the snapshot's rename rule, with the catalog commit as the commit point. (r11-restart-r2, F-FZ:
//! except the records after the checkpoint's own `Record::Checkpoint`, which it replays; see
//! "The fuzzy checkpoint" below.) The arena is synced
//! before the catalog commits, as before a snapshot. Frees derived by the log's replay are applied
//! to the free table (they are the effects of durable records); a slot allocated after the
//! checkpoint that no durable record names is free after a crash, because the catalog still lists
//! it free or it lies past the catalog's high-water mark.
//!
//! # Blind spots (stated, not solved)
//!
//! * The in-memory cache is never evicted: memory grows with the branches touched since open.
//! * A slot the catalog lists free and this process has not fetched is not in the arena's free
//!   bitmap, so the arena's double-free and use-after-free assertions do not see it.
//! * `ids()` and `slots_in_use()` scan the catalog: they are Θ(N) by what they return.
//! * A checkpoint rewrites every row of a dirty branch (its current map and retained versions),
//!   not only the changed pages. (The TRUNK's versions are written one row each: C-P below.)
//!
//! # The trunk's versions, read in place (a12-durable-open C-P)
//!
//! A trunk page can hold one retained version per child forked between two of its writes, so a
//! page's version list grows with the branch count. The trunk's `ret` rows (owner 0) are therefore
//! never loaded a page at a time: a resolve is one descending probe of `ret_page` (the version with
//! the greatest `born <= f`; Becker et al.'s multiversion B-tree answers "the version of key k live at
//! time t" with one root-to-leaf search), a trunk write or a replayed pre-image reads only the page's
//! last version, a trunk child's reap reads F2's two ranges from `ret_born` and `ret_died` (ZFS keeps
//! its deadlists on disk and splits them in place), and a checkpoint inserts and deletes single rows.
//!
//! # The fuzzy checkpoint (r11-restart-r2, F-FZ; UNBUILT when written)
//!
//! ARIES's fuzzy checkpoint (Mohan et al., TODS 1992) in place of a sharp one: the store mutex is
//! held to CAPTURE what changed since the last checkpoint and to install the result, never across
//! the catalog writes, the commit or the WAL backfill. A second connection ([`Catalog::writer`])
//! writes the captured rows while the store's own connection serves on-demand reads from a snapshot
//! pinned at the capture ([`Catalog::begin_read_snapshot`]), which in-memory state overrides exactly
//! as it does between checkpoints. The capture appends a `Record::Checkpoint` to the log (ARIES's
//! begin-checkpoint record), so the log is cut to what follows it only after the commit, and a crash
//! between the two replays just that suffix (see `Journal::recover_catalog`).

use std::num::NonZero;
use std::path::Path;

use super::arena::Slot;
use super::SyncClass;
use super::prewarm::{warm_files, wal_of, Prewarm, PrewarmStats};
use crate::storage::pager::{PageRef, Pager};
use crate::sync::Arc;
use crate::util::IOExt as _;
use crate::{
    Connection, Database, DatabaseOpts, LimboError, OpenFlags, PlatformIO, Result, SqliteDialect,
    Statement, Value, IO,
};

const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS meta(k INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS branch(id INTEGER PRIMARY KEY, parent INTEGER NOT NULL, \
     fork_epoch INTEGER NOT NULL, epoch INTEGER NOT NULL, released INTEGER NOT NULL, \
     lease INTEGER, n_children INTEGER NOT NULL)",
    "CREATE INDEX IF NOT EXISTS branch_children ON branch(parent, fork_epoch)",
    "CREATE INDEX IF NOT EXISTS branch_lease ON branch(lease)",
    // `released` is 0, 1, or 2 for a branch released while a connection held it open (r11-ever's
    // F7 durable port): only those can be collected at an open (a released branch no connection
    // held was collected at its release: freed, retired or spliced), so an open reads them alone and
    // never walks retired interiors.
    "CREATE INDEX IF NOT EXISTS branch_released ON branch(released, n_children)",
    // k = branch << 32 | page: one B-tree lookup per (branch, page), one range per branch.
    "CREATE TABLE IF NOT EXISTS cur(k INTEGER PRIMARY KEY, slot INTEGER NOT NULL, \
     born INTEGER NOT NULL, crc INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS ret(owner INTEGER NOT NULL, page INTEGER NOT NULL, \
     born INTEGER NOT NULL, died INTEGER NOT NULL, slot INTEGER NOT NULL, crc INTEGER NOT NULL)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ret_page ON ret(owner, page, born)",
    "CREATE INDEX IF NOT EXISTS ret_born ON ret(owner, born)",
    // a12-durable-open C-P: F2's other side, read in place for the trunk (owner 0).
    "CREATE INDEX IF NOT EXISTS ret_died ON ret(owner, died)",
    "CREATE TABLE IF NOT EXISTS free(slot INTEGER PRIMARY KEY)",
    // fastest-engine M1 item 4: a named server branch's name, unique among unreleased branches (a
    // release deletes its row), and the index a release and a branch load find it by.
    "CREATE TABLE IF NOT EXISTS branch_name(name TEXT PRIMARY KEY, id INTEGER NOT NULL)",
    "CREATE UNIQUE INDEX IF NOT EXISTS branch_name_id ON branch_name(id)",
];

/// The catalog's per-operation lookups, as prepared below (for the plan test).
#[cfg(test)]
const LOOKUPS: &[&str] = &[
    "SELECT parent, fork_epoch, epoch, released, lease, n_children FROM branch WHERE id = ?1",
    "SELECT k, slot, born, crc FROM cur WHERE k >= ?1 AND k < ?2",
    "SELECT page, born, died, slot, crc FROM ret WHERE owner = ?1",
    "SELECT born, died, slot, crc FROM ret WHERE owner = ?1 AND page = ?2 AND born <= ?3 ORDER BY born DESC LIMIT 1",
    "SELECT page, born, died, slot, crc FROM ret WHERE owner = ?1 AND born > ?2 AND born <= ?3 ORDER BY born ASC LIMIT ?4",
    "SELECT page, born, died, slot, crc FROM ret WHERE owner = ?1 AND died > ?2 AND died <= ?3 ORDER BY died ASC LIMIT ?4",
    "SELECT fork_epoch, id FROM branch WHERE parent = ?1 AND fork_epoch >= ?2 AND fork_epoch < ?3 ORDER BY fork_epoch ASC LIMIT ?4",
    "SELECT fork_epoch, id FROM branch WHERE parent = ?1 AND fork_epoch < ?2 ORDER BY fork_epoch DESC LIMIT ?3",
    "SELECT fork_epoch, id FROM branch WHERE parent = ?1 AND fork_epoch > ?2 ORDER BY fork_epoch ASC LIMIT ?3",
    "SELECT id FROM branch WHERE lease = ?1 AND id > ?2 ORDER BY id ASC LIMIT ?3",
    "SELECT id, lease FROM branch WHERE lease > ?2 AND lease <= ?1 ORDER BY lease ASC, id ASC LIMIT ?3",
    "SELECT lease FROM branch WHERE lease > -1 ORDER BY lease ASC LIMIT 1",
    "SELECT lease FROM branch WHERE lease > ?1 ORDER BY lease ASC LIMIT 1",
    "SELECT id FROM branch WHERE released = 2",
    "SELECT id FROM branch WHERE parent = ?1 AND fork_epoch = ?2",
    "SELECT slot FROM free WHERE slot > ?1 ORDER BY slot ASC LIMIT ?2",
    "SELECT slot FROM free WHERE slot > -1 ORDER BY slot ASC LIMIT ?1",
    "SELECT 1 FROM free WHERE slot = ?1",
    "DELETE FROM cur WHERE k >= ?1 AND k < ?2",
    "DELETE FROM ret WHERE owner = ?1",
    "DELETE FROM ret WHERE owner = ?1 AND page = ?2 AND born = ?3",
    "DELETE FROM free WHERE slot <= ?1",
    "DELETE FROM free WHERE slot = ?1",
    "DELETE FROM branch WHERE id = ?1",
    "UPDATE branch SET epoch = ?2, released = ?3, lease = ?4, n_children = ?5 WHERE id = ?1",
    "UPDATE branch SET parent = ?2, fork_epoch = ?3 WHERE id = ?1",
    "SELECT id FROM branch_name WHERE name = ?1",
    "SELECT name FROM branch_name WHERE id = ?1",
    "DELETE FROM branch_name WHERE name = ?1",
    "DELETE FROM branch_name WHERE id = ?1",
];

const META_GENERATION: i64 = 1;
const META_PAGE_SIZE: i64 = 2;
const META_NEXT_ID: i64 = 3;
const META_TRUNK_EPOCH: i64 = 4;
const META_TRUNK_CHILDREN: i64 = 5;
const META_LEASE_NOW: i64 = 6;
const META_ARENA_HW: i64 = 7;
const META_IN_USE: i64 = 8;
const META_STATES: i64 = 9;

/// Pages of Turso page cache each catalog connection keeps (r11-restart-r2, item 4): sized to hold
/// a checkpoint window's dirty leaves (~31.8k commits at 33 B in a 1 MiB log) so the writer does
/// not spill mid-checkpoint, where Turso's default (2,000 pages) spilled at 1,800 and let the
/// catalog's leaves go cold at 10^6 branches (R7, the r11-restart-refute report). Overridden by
/// `R11_CAT_CACHE_PAGES` (a measurement arm).
pub(crate) fn cat_cache_pages() -> u64 {
    static PAGES: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *PAGES.get_or_init(|| {
        std::env::var("R11_CAT_CACHE_PAGES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(32_768)
    })
}

/// r12-catload's F2 fire-check (PREREG A2): `R12_MUTANT` names a deliberate defect in the prewarm, so the
/// prewarm test can be shown to fail on it. Unset, every mutant is off. This branch is a lane prototype,
/// never merged or pushed; the mutants would not survive a port.
fn mutant(name: &str) -> bool {
    std::env::var("R12_MUTANT").as_deref() == Ok(name)
}

/// Read page `idx` through the page cache and wait until it is loaded.
fn read_loaded(pager: &Pager, idx: i64) -> Result<PageRef> {
    let (page, pending) = pager.io.block(|| pager.read_page(idx))?;
    if let Some(c) = pending {
        pager.io.wait_for_completion(c)?;
    }
    Ok(page)
}

/// The catalog's meta row: what an open needs before it touches any branch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Meta {
    pub(crate) generation: u64,
    pub(crate) page_size: u32,
    pub(crate) next_id: u64,
    pub(crate) trunk_epoch: u64,
    pub(crate) trunk_children: u64,
    pub(crate) lease_now_ms: u64,
    pub(crate) arena_hw: u32,
    pub(crate) in_use: u64,
    pub(crate) states: u64,
    /// The store's format version (`journal::format_version`: 5, or 6 in the F7 splice arm), so that
    /// a catalog opened with a torn log header, which has no version to check, is still refused by a
    /// store of another format or arm (0: written before the catalog carried it). Carried in the
    /// high 32 bits of the page-size key's value, so the meta row keeps its nine keys (F7-durable
    /// fa16116b3; r13-compose A2.R6, S-6).
    pub(crate) format: u32,
}

/// One branch as the catalog holds it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CatBranch {
    pub(crate) id: u64,
    pub(crate) parent: u64,
    pub(crate) fork_epoch: u64,
    pub(crate) epoch: u64,
    pub(crate) released: bool,
    /// Released while a connection held it open: kept whole until that connection's `Close`, or the
    /// end of the next recovery (`released` column 2; r11-ever's F7 durable port).
    pub(crate) held_open: bool,
    pub(crate) lease: Option<u64>,
    pub(crate) n_children: u64,
    /// (page, slot, born, crc)
    pub(crate) current: Vec<(u32, Slot, u64, u32)>,
    /// (page, born, died, slot, crc)
    pub(crate) retained: Vec<(u32, u64, u64, Slot, u32)>,
    /// A named server branch's name (fastest-engine M1 item 4), kept in `branch_name`.
    pub(crate) name: Option<String>,
}

/// Counters, observing only (r11-restart lane instrument).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CatalogCounters {
    pub(crate) queries: u64,
    pub(crate) rows_read: u64,
    pub(crate) rows_written: u64,
}

/// An integer as the catalog stores it. Every value the store keeps is below 2^63 except a
/// saturated lease deadline, which is clamped: `i64::MAX` ms is as unreachable as `u64::MAX`.
fn int(v: u64) -> Value {
    Value::from_i64(v.min(i64::MAX as u64) as i64)
}

fn get(row: &[Value], at: usize) -> Result<u64> {
    row.get(at)
        .and_then(Value::as_int)
        .map(|v| v as u64)
        .ok_or_else(|| LimboError::Corrupt(format!("branch catalog: column {at} is not an integer")))
}

/// A `ret` row as (page, born, died, slot, crc).
fn ret_row(row: &[Value]) -> Result<(u32, u64, u64, Slot, u32)> {
    Ok((
        get(row, 0)? as u32,
        get(row, 1)?,
        get(row, 2)?,
        get(row, 3)? as Slot,
        get(row, 4)? as u32,
    ))
}

/// The `released` column: 0, 1, or 2 when released while a connection held it open.
fn released_code(b: &CatBranch) -> Value {
    Value::from_i64(match (b.released, b.held_open) {
        (false, _) => 0,
        (true, false) => 1,
        (true, true) => 2,
    })
}

/// The catalog key of `(branch, page)`. Refused past 2^31 branch ids rather than wrapped.
fn cur_key(branch: u64, page: u32) -> Result<i64> {
    if branch >= 1 << 31 {
        return Err(LimboError::InternalError(format!(
            "branch catalog: branch id {branch} does not fit the catalog key"
        )));
    }
    Ok(((branch as i64) << 32) | page as i64)
}

struct Stmt {
    stmt: Statement,
}

impl Stmt {
    fn rows(&mut self, params: &[Value], counters: &mut CatalogCounters) -> Result<Vec<Vec<Value>>> {
        self.stmt.reset()?;
        for (i, p) in params.iter().enumerate() {
            self.stmt
                .bind_at(NonZero::new(i + 1).expect("i + 1 > 0"), p.clone())?;
        }
        let rows = self.stmt.run_collect_rows();
        // Reset at once, finished or not: a statement left un-reset must hold no read mark on the
        // catalog's WAL (a held mark keeps the WAL from ever restarting; PREREG A7).
        self.stmt.reset()?;
        let rows = rows?;
        counters.queries += 1;
        counters.rows_read += rows.len() as u64;
        Ok(rows)
    }

    fn exec(&mut self, params: &[Value], counters: &mut CatalogCounters) -> Result<()> {
        self.stmt.reset()?;
        for (i, p) in params.iter().enumerate() {
            self.stmt
                .bind_at(NonZero::new(i + 1).expect("i + 1 > 0"), p.clone())?;
        }
        let done = self.stmt.run_ignore_rows();
        self.stmt.reset()?;
        done?;
        counters.queries += 1;
        counters.rows_written += 1;
        Ok(())
    }
}

/// A SQLite varint at `at` in `d`: its value and its length in bytes (1 to 9; the ninth byte
/// carries 8 bits). `None` past the end.
fn varint(d: &[u8], at: usize) -> Option<(u64, usize)> {
    let mut v = 0u64;
    for i in 0..9 {
        let b = *d.get(at + i)?;
        if i == 8 {
            return Some(((v << 8) | u64::from(b), 9));
        }
        v = (v << 7) | u64::from(b & 0x7f);
        if b & 0x80 == 0 {
            return Some((v, i + 1));
        }
    }
    None
}

/// Adds one leaf page's cells to `c` (r11-ever amendment 35's census). `d` is the page, `h` its
/// header's offset (100 on page 1); `table` is true for a table leaf (type 13: payload-length
/// varint, rowid varint, record) and false for an index leaf (type 10: payload-length varint,
/// record). A record is a header-length varint, one serial-type varint per field, then the body.
/// `None` if a cell does not parse as the file format says.
fn census_leaf(d: &[u8], h: usize, table: bool, c: &mut super::LeafCensus) -> Option<()> {
    let usable = d.len();
    let n = u16::from_be_bytes([*d.get(h + 3)?, *d.get(h + 4)?]) as usize;
    // The largest payload a cell keeps on its page: U - 35 on a table leaf, and
    // ((U - 12) * 64 / 255) - 23 on an index page (fileformat2, "B-tree Pages").
    let max_local = if table { usable - 35 } else { (usable - 12) * 64 / 255 - 23 };
    c.leaf_pages += 1;
    c.used_bytes += 8 + 2 * n as u64;
    for i in 0..n {
        let p = h + 8 + 2 * i;
        let at = u16::from_be_bytes([*d.get(p)?, *d.get(p + 1)?]) as usize;
        let (len, l1) = varint(d, at)?;
        let mut body = at + l1;
        let mut cell = l1 as u64;
        if table {
            let (_, l2) = varint(d, body)?;
            body += l2;
            cell += l2 as u64;
            c.rowid_varint_bytes += l2 as u64;
        }
        c.cells += 1;
        if len as usize > max_local {
            // Its on-page part needs the min-local rule; a spilled cell is counted, not measured.
            c.overflow_cells += 1;
            continue;
        }
        cell += len;
        c.cell_bytes += cell;
        c.used_bytes += cell;
        let rec = d.get(body..body + len as usize)?;
        let (hlen, l3) = varint(rec, 0)?;
        let mut f = l3;
        while f < hlen as usize {
            let (t, l) = varint(rec, f)?;
            f += l;
            match t {
                1..=6 => c.ints[t as usize - 1] += 1,
                8 | 9 => c.ints[6] += 1,
                _ => {}
            }
        }
    }
    Some(())
}

#[cfg(test)]
mod census_tests {
    use super::*;

    /// The census reads integer widths and rowid varints as SQLite stores them: a planted table
    /// whose rows straddle 2^21 (rowid varint 3 -> 4 bytes) and 2^23 (a value 3 -> 4 bytes).
    #[test]
    fn the_census_reads_integer_widths_and_rowid_varints_as_stored() {
        let dir = tempfile::TempDir::new().unwrap();
        let cat = Catalog::open(&dir.path().join("c.db"), SyncClass::Off).unwrap();
        cat.conn
            .execute("CREATE TABLE plant(k INTEGER PRIMARY KEY, v INTEGER NOT NULL)")
            .unwrap();
        for (k, v) in [(5i64, 5i64), ((1 << 21) - 1, (1 << 23) - 1), (1 << 21, 1 << 23)] {
            cat.conn
                .execute(format!("INSERT INTO plant VALUES ({k}, {v})"))
                .unwrap();
        }
        let shape = cat.shape().unwrap();
        assert_eq!(shape.unaccounted, 0, "{shape:?}");
        let (_, c) = shape
            .census
            .iter()
            .find(|(n, _)| n == "plant")
            .expect("the planted table's census");
        assert_eq!((c.leaf_pages, c.cells, c.overflow_cells), (1, 3, 0), "{c:?}");
        // k is the rowid (stored as NULL in the record); v is 5, 2^23 - 1, 2^23.
        assert_eq!(c.ints, [1, 0, 1, 1, 0, 0, 0], "{c:?}");
        assert_eq!(c.rowid_varint_bytes, 1 + 3 + 4, "{c:?}");
        assert_eq!(shape.page_size, 4096, "{shape:?}");
    }

    #[test]
    fn a_varint_reads_as_the_file_format_writes_it() {
        assert_eq!(varint(&[0x05], 0), Some((5, 1)));
        assert_eq!(varint(&[0x81, 0x00], 0), Some((128, 2)));
        // 2^21 - 1 is the largest 3-byte varint.
        assert_eq!(varint(&[0xff, 0xff, 0x7f], 0), Some(((1 << 21) - 1, 3)));
        assert_eq!(varint(&[0x81, 0x80, 0x80, 0x00], 0), Some((1 << 21, 4)));
        assert_eq!(varint(&[0xff; 9], 0), Some((u64::MAX, 9)));
        assert_eq!(varint(&[0x81], 0), None);
    }
}

pub(crate) struct Catalog {
    _db: Arc<Database>,
    conn: Arc<Connection>,
    pub(crate) counters: CatalogCounters,
    /// What this handle's open prewarmed (r12-catload; `Prewarm::Off` unless [`Catalog::prewarm`] ran).
    pub(crate) prewarm: PrewarmStats,
    meta_all: Stmt,
    meta_put: Stmt,
    branch_get: Stmt,
    branch_put: Stmt,
    branch_update: Stmt,
    branch_rekey: Stmt,
    branch_del: Stmt,
    cur_range: Stmt,
    cur_del_range: Stmt,
    cur_put: Stmt,
    ret_owner: Stmt,
    ret_pred: Stmt,
    ret_born_range: Stmt,
    ret_died_range: Stmt,
    ret_count: Stmt,
    ret_del_owner: Stmt,
    ret_del_one: Stmt,
    ret_put: Stmt,
    ret_any: Stmt,
    child_in: Stmt,
    child_below: Stmt,
    child_above: Stmt,
    lease_tie: Stmt,
    lease_page: Stmt,
    child_at: Stmt,
    lease_min: Stmt,
    lease_min_after: Stmt,
    released: Stmt,
    unreleased: Stmt,
    free_after: Stmt,
    free_first: Stmt,
    free_has: Stmt,
    free_all: Stmt,
    free_del_upto: Stmt,
    free_del: Stmt,
    free_put: Stmt,
    name_get: Stmt,
    name_of: Stmt,
    name_put: Stmt,
    name_del: Stmt,
    name_del_id: Stmt,
}

impl Catalog {
    /// Open (creating if absent) the catalog at `path`. A `sync` class that syncs selects
    /// `synchronous = FULL`, so a checkpoint's commit is durable before the log starts over; `Off`
    /// selects OFF (a measurement arm, like `Durable { sync: SyncClass::Off }`).
    pub(crate) fn open(path: &Path, sync: SyncClass) -> Result<Catalog> {
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
        let path = path.to_str().ok_or_else(|| {
            LimboError::InvalidArgument("branch catalog path is not UTF-8".to_string())
        })?;
        let db = Database::open_file_with_flags(
            io,
            path,
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )?;
        let conn = db.connect()?;
        for sql in SCHEMA {
            conn.execute(*sql)?;
        }
        Self::prepared(db, conn, sync)
    }

    /// A second handle on the same catalog database, over its own connection: the writer of a
    /// fuzzy checkpoint (F-FZ), which runs without the store mutex while the store's own
    /// connection keeps serving on-demand reads from a pinned snapshot.
    pub(crate) fn writer(&self, sync: SyncClass) -> Result<Catalog> {
        let conn = self._db.connect()?;
        Self::prepared(self._db.clone(), conn, sync)
    }

    /// Set a connection's pragmas and prepare every statement on it.
    fn prepared(db: Arc<Database>, conn: Arc<Connection>, sync: SyncClass) -> Result<Catalog> {
        conn.execute(if sync.syncs() {
            "PRAGMA synchronous = FULL"
        } else {
            "PRAGMA synchronous = OFF"
        })?;
        // D2: the catalog's commits are F_FULLFSYNC, as the log's and the arena's are.
        if sync == SyncClass::FullFsync {
            conn.execute("PRAGMA fullfsync = ON")?;
        }
        conn.execute(format!("PRAGMA cache_size = {}", cat_cache_pages()))?;
        let p = |sql: &str| -> Result<Stmt> { Ok(Stmt { stmt: conn.prepare(sql)? }) };
        Ok(Catalog {
            meta_all: p("SELECT k, v FROM meta")?,
            meta_put: p("INSERT OR REPLACE INTO meta(k, v) VALUES (?1, ?2)")?,
            branch_get: p(
                "SELECT parent, fork_epoch, epoch, released, lease, n_children FROM branch WHERE id = ?1",
            )?,
            branch_put: p(
                "INSERT OR REPLACE INTO branch(id, parent, fork_epoch, epoch, released, lease, n_children) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?,
            branch_update: p(
                "UPDATE branch SET epoch = ?2, released = ?3, lease = ?4, n_children = ?5 WHERE id = ?1",
            )?,
            branch_rekey: p("UPDATE branch SET parent = ?2, fork_epoch = ?3 WHERE id = ?1")?,
            branch_del: p("DELETE FROM branch WHERE id = ?1")?,
            cur_range: p("SELECT k, slot, born, crc FROM cur WHERE k >= ?1 AND k < ?2")?,
            cur_del_range: p("DELETE FROM cur WHERE k >= ?1 AND k < ?2")?,
            cur_put: p("INSERT OR REPLACE INTO cur(k, slot, born, crc) VALUES (?1, ?2, ?3, ?4)")?,
            ret_owner: p("SELECT page, born, died, slot, crc FROM ret WHERE owner = ?1")?,
            ret_pred: p(
                "SELECT born, died, slot, crc FROM ret WHERE owner = ?1 AND page = ?2 AND born <= ?3 \
                 ORDER BY born DESC LIMIT 1",
            )?,
            ret_born_range: p(
                "SELECT page, born, died, slot, crc FROM ret WHERE owner = ?1 AND born > ?2 \
                 AND born <= ?3 ORDER BY born ASC LIMIT ?4",
            )?,
            ret_died_range: p(
                "SELECT page, born, died, slot, crc FROM ret WHERE owner = ?1 AND died > ?2 \
                 AND died <= ?3 ORDER BY died ASC LIMIT ?4",
            )?,
            ret_count: p("SELECT count(*) FROM ret WHERE owner = ?1")?,
            ret_del_owner: p("DELETE FROM ret WHERE owner = ?1")?,
            ret_del_one: p("DELETE FROM ret WHERE owner = ?1 AND page = ?2 AND born = ?3")?,
            ret_put: p(
                "INSERT INTO ret(owner, page, born, died, slot, crc) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?,
            ret_any: p("SELECT 1 FROM ret LIMIT 1")?,
            child_in: p(
                "SELECT fork_epoch, id FROM branch WHERE parent = ?1 AND fork_epoch >= ?2 \
                 AND fork_epoch < ?3 ORDER BY fork_epoch ASC LIMIT ?4",
            )?,
            child_below: p(
                "SELECT fork_epoch, id FROM branch WHERE parent = ?1 AND fork_epoch < ?2 \
                 ORDER BY fork_epoch DESC LIMIT ?3",
            )?,
            child_above: p(
                "SELECT fork_epoch, id FROM branch WHERE parent = ?1 AND fork_epoch > ?2 \
                 ORDER BY fork_epoch ASC LIMIT ?3",
            )?,
            child_at: p("SELECT id FROM branch WHERE parent = ?1 AND fork_epoch = ?2")?,
            // Range seeks on `branch_lease`: NULL sorts first in the index, so `lease > -1` skips
            // every unleased row instead of walking them (deadlines are never negative).
            // F-EXP's keyset pages over `branch_lease`, whose entries sort by deadline then row id:
            // the rest of one deadline, then the deadlines after it.
            lease_tie: p("SELECT id FROM branch WHERE lease = ?1 AND id > ?2 ORDER BY id ASC LIMIT ?3")?,
            lease_page: p(
                "SELECT id, lease FROM branch WHERE lease > ?2 AND lease <= ?1 \
                 ORDER BY lease ASC, id ASC LIMIT ?3",
            )?,
            lease_min: p("SELECT lease FROM branch WHERE lease > -1 ORDER BY lease ASC LIMIT 1")?,
            lease_min_after: p(
                "SELECT lease FROM branch WHERE lease > ?1 ORDER BY lease ASC LIMIT 1",
            )?,
            released: p("SELECT id FROM branch WHERE released = 2")?,
            unreleased: p("SELECT id FROM branch WHERE released = 0")?,
            free_after: p("SELECT slot FROM free WHERE slot > ?1 ORDER BY slot ASC LIMIT ?2")?,
            free_first: p("SELECT slot FROM free WHERE slot > -1 ORDER BY slot ASC LIMIT ?1")?,
            free_has: p("SELECT 1 FROM free WHERE slot = ?1")?,
            free_all: p("SELECT slot FROM free")?,
            free_del_upto: p("DELETE FROM free WHERE slot <= ?1")?,
            free_del: p("DELETE FROM free WHERE slot = ?1")?,
            free_put: p("INSERT OR REPLACE INTO free(slot) VALUES (?1)")?,
            name_get: p("SELECT id FROM branch_name WHERE name = ?1")?,
            name_of: p("SELECT name FROM branch_name WHERE id = ?1")?,
            name_put: p("INSERT OR REPLACE INTO branch_name(name, id) VALUES (?1, ?2)")?,
            name_del: p("DELETE FROM branch_name WHERE name = ?1")?,
            name_del_id: p("DELETE FROM branch_name WHERE id = ?1")?,
            counters: CatalogCounters::default(),
            prewarm: PrewarmStats::default(),
            conn,
            _db: db,
        })
    }

    /// r12-catload: warm this catalog as `mode` says (see `super::prewarm`), once, at a store's
    /// open: its file and WAL in the OS page cache (`read`, `prefetch`), or this connection's page
    /// cache (`interior`, `buffer`). `path` is the catalog file this handle opened.
    pub(crate) fn prewarm(&mut self, path: &Path, mode: Prewarm) -> Result<()> {
        let t = std::time::Instant::now();
        let mut st = PrewarmStats {
            mode,
            cache_pages: cat_cache_pages(),
            ..PrewarmStats::default()
        };
        match mode {
            Prewarm::Off => {}
            Prewarm::Read | Prewarm::Prefetch => {
                warm_files(&[path, wal_of(path).as_path()], mode, &mut st)?;
            }
            Prewarm::Interior | Prewarm::Buffer => {
                let ints = |conn: &Arc<Connection>, sql: &str| -> Result<Vec<u64>> {
                    conn.prepare(sql)?
                        .run_collect_rows()?
                        .iter()
                        .map(|row| get(row, 0))
                        .collect()
                };
                let n = ints(&self.conn, "PRAGMA page_count")?.first().copied().unwrap_or(0);
                if mode == Prewarm::Buffer {
                    st.cache_pages = st.cache_pages.max(n + 64);
                    self.conn
                        .execute(format!("PRAGMA cache_size = {}", st.cache_pages))?;
                }
                // Page 1 is sqlite_schema's root; every other tree's root is listed in it.
                let mut roots = vec![1u64];
                if mode == Prewarm::Interior {
                    roots.extend(ints(
                        &self.conn,
                        "SELECT rootpage FROM sqlite_schema WHERE rootpage > 1",
                    )?);
                }
                let pager = self.conn.pager.load().clone();
                pager.begin_read_tx()?;
                // F2 fire-check mutant (r12-catload PREREG A2; unset, no effect): `buffer1` reads page 1 only.
                let last = if mutant("buffer1") { 1 } else { n };
                let read = if mode == Prewarm::Buffer {
                    (1..=last).try_for_each(|idx| -> Result<()> {
                        read_loaded(&pager, idx as i64)?;
                        st.pages += 1;
                        Ok(())
                    })
                } else {
                    roots
                        .iter()
                        .try_for_each(|&root| Self::interior_walk(&pager, root as u32, &mut st))
                };
                pager.end_read_tx();
                read?;
            }
        }
        st.ns = u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.prewarm = st;
        Ok(())
    }

    /// Read one B-tree's interior pages, level by level from `root`. The first page of a level
    /// tells whether the whole level is leaves, since a B-tree's leaves share one depth: that one
    /// leaf is the only leaf read.
    fn interior_walk(pager: &Pager, root: u32, st: &mut PrewarmStats) -> Result<()> {
        let mut level = vec![root];
        while !level.is_empty() {
            let mut next = Vec::new();
            for &idx in &level {
                let page = read_loaded(pager, idx as i64)?;
                st.pages += 1;
                let c = page.get_contents();
                if c.is_leaf() {
                    // F2 fire-check mutant (unset, no effect): `interiorall` reads the whole leaf level.
                    if mutant("interiorall") {
                        continue;
                    }
                    return Ok(());
                }
                st.interior += 1;
                for i in 0..c.cell_count() {
                    next.push(c.cell_interior_read_left_child_page(i)?);
                }
                if let Some(right) = c.rightmost_pointer()? {
                    next.push(right);
                }
            }
            level = next;
        }
        Ok(())
    }

    /// The meta row, or `None` for a catalog no checkpoint has committed yet.
    pub(crate) fn meta(&mut self) -> Result<Option<Meta>> {
        let rows = self.meta_all.rows(&[], &mut self.counters)?;
        if rows.is_empty() {
            return Ok(None);
        }
        let mut m = Meta::default();
        let mut seen = 0u32;
        for row in rows {
            let (k, v) = (get(&row, 0)? as i64, get(&row, 1)?);
            match k {
                META_GENERATION => m.generation = v,
                META_PAGE_SIZE => {
                    m.page_size = (v & 0xFFFF_FFFF) as u32;
                    m.format = (v >> 32) as u32;
                }
                META_NEXT_ID => m.next_id = v,
                META_TRUNK_EPOCH => m.trunk_epoch = v,
                META_TRUNK_CHILDREN => m.trunk_children = v,
                META_LEASE_NOW => m.lease_now_ms = v,
                META_ARENA_HW => m.arena_hw = v as u32,
                META_IN_USE => m.in_use = v,
                META_STATES => m.states = v,
                _ => return Err(LimboError::Corrupt(format!("branch catalog: meta key {k}"))),
            }
            seen += 1;
        }
        if seen != 9 {
            return Err(LimboError::Corrupt(format!(
                "branch catalog: meta row has {seen} of 9 keys"
            )));
        }
        Ok(Some(m))
    }

    /// The file's shape (see `CatalogShape`): `PRAGMA page_count` and `freelist_count`, each
    /// table's rows, and each B-tree's pages per level, walked from its root through the interior
    /// pages' child pointers (SQLite's file format: an interior page, type 2 or 5, lists a left child
    /// in the first four bytes of each cell and its right-most child at header offset 8; page 1's
    /// header starts at byte 100). Pages are read through this connection's pager inside one read
    /// transaction (the snapshot a `BEGIN` and a first read take), so the WAL's latest versions
    /// count and nothing moves pages mid-walk; `sqlite_dbpage` would do the same but is built only
    /// with the `cli_only` feature. Unprepared statements, outside `counters`: an instrument. It
    /// writes nothing: the transaction only reads, and is rolled back.
    pub(crate) fn shape(&self) -> Result<super::CatalogShape> {
        self.conn.execute("BEGIN")?;
        let shape = self.shape_in_read_tx();
        let _ = self.conn.execute("ROLLBACK");
        shape
    }

    fn shape_in_read_tx(&self) -> Result<super::CatalogShape> {
        let bad = |what: String| LimboError::Corrupt(format!("branch catalog shape: {what}"));
        let one_int = |sql: &str| -> Result<u64> {
            let rows = self.conn.prepare(sql)?.run_collect_rows()?;
            rows.first()
                .and_then(|r| r.first())
                .and_then(Value::as_int)
                .map(|v| v as u64)
                .ok_or_else(|| bad(format!("{sql} returned no integer")))
        };
        // A table read first: it takes the transaction's read snapshot.
        let mut rows = Vec::new();
        for table in ["meta", "branch", "cur", "ret", "free"] {
            rows.push((table.to_string(), one_int(&format!("SELECT count(*) FROM {table}"))?));
        }
        let page_count = one_int("PRAGMA page_count")?;
        let freelist_count = one_int("PRAGMA freelist_count")?;
        let mut roots = vec![("sqlite_schema".to_string(), 1u64)];
        for row in self
            .conn
            .prepare("SELECT name, rootpage FROM sqlite_schema WHERE rootpage > 0 ORDER BY name")?
            .run_collect_rows()?
        {
            match (row.first(), row.get(1).and_then(Value::as_int)) {
                (Some(Value::Text(name)), Some(root)) => roots.push((name.as_str().to_string(), root as u64)),
                _ => return Err(bad("an unreadable sqlite_schema row".to_string())),
            }
        }
        let pager = self.conn.get_pager();
        let page = |pgno: u64| -> Result<Vec<u8>> {
            let (page_ref, completion) = pager.io.block(|| pager.read_page(pgno as i64))?;
            if let Some(c) = completion {
                pager.io.wait_for_completion(c)?;
            }
            Ok(page_ref.get_contents().as_slice().to_vec())
        };
        let u16_at = |d: &[u8], at: usize| -> Result<usize> {
            d.get(at..at + 2)
                .map(|b| u16::from_be_bytes([b[0], b[1]]) as usize)
                .ok_or_else(|| bad(format!("a u16 past a page's end at {at}")))
        };
        let u32_at = |d: &[u8], at: usize| -> Result<u64> {
            d.get(at..at + 4)
                .map(|b| u64::from(u32::from_be_bytes([b[0], b[1], b[2], b[3]])))
                .ok_or_else(|| bad(format!("a u32 past a page's end at {at}")))
        };
        let mut trees = Vec::new();
        let mut census = Vec::new();
        let mut page_size = 0u64;
        let mut in_trees = 0u64;
        for (name, root) in roots {
            let (mut levels, mut frontier) = (Vec::new(), vec![root]);
            let mut leaves = super::LeafCensus::default();
            while !frontier.is_empty() {
                levels.push(frontier.len() as u64);
                in_trees += frontier.len() as u64;
                let mut next = Vec::new();
                for pgno in frontier {
                    let d = page(pgno)?;
                    page_size = d.len() as u64;
                    let h = if pgno == 1 { 100 } else { 0 };
                    match d.get(h).copied() {
                        Some(0x02) | Some(0x05) => {
                            for i in 0..u16_at(&d, h + 3)? {
                                next.push(u32_at(&d, u16_at(&d, h + 12 + 2 * i)?)?);
                            }
                            next.push(u32_at(&d, h + 8)?);
                        }
                        Some(t @ (0x0A | 0x0D)) => census_leaf(&d, h, t == 0x0D, &mut leaves)
                            .ok_or_else(|| bad(format!("a cell of leaf page {pgno} of {name} does not parse")))?,
                        other => return Err(bad(format!("page {pgno} of {name} has type {other:?}"))),
                    }
                }
                frontier = next;
            }
            census.push((name.clone(), leaves));
            trees.push((name, levels));
        }
        Ok(super::CatalogShape {
            page_count,
            freelist_count,
            rows,
            trees,
            unaccounted: page_count as i64 - in_trees as i64 - freelist_count as i64,
            census,
            page_size,
        })
    }

    pub(crate) fn begin(&mut self) -> Result<()> {
        self.conn.execute("BEGIN")?;
        Ok(())
    }

    pub(crate) fn commit(&mut self) -> Result<()> {
        self.conn.execute("COMMIT")?;
        Ok(())
    }

    /// Checkpoint the catalog's WAL into its file and truncate it (r11-restart lane, fix v2, PREREG
    /// A7). Turso's WAL restarts only when a writer finds it fully backfilled with no read mark in
    /// use, which this catalog's pattern never meets, so without this the WAL keeps every frame ever
    /// written and an open recovers all of them. Returns the pragma's row (busy, log, checkpointed).
    pub(crate) fn truncate_wal(&mut self) -> Result<Vec<i64>> {
        let rows = self
            .conn
            .prepare("PRAGMA wal_checkpoint(TRUNCATE)")?
            .run_collect_rows()?;
        Ok(rows
            .first()
            .map(|r| r.iter().map(|v| v.as_int().unwrap_or(-1)).collect())
            .unwrap_or_default())
    }

    /// One integer column of `sql`'s rows, on this handle's connection (r12-catload's prewarm test).
    #[cfg(test)]
    pub(crate) fn ints(&self, sql: &str) -> Result<Vec<u64>> {
        self.conn
            .prepare(sql)?
            .run_collect_rows()?
            .iter()
            .map(|row| get(row, 0))
            .collect()
    }

    /// `EXPLAIN QUERY PLAN` of every query the store runs on the catalog, for the test that
    /// proves none of them walks a table or sorts (r11-restart lane): `(sql, plan lines)`.
    #[cfg(test)]
    pub(crate) fn plans(&self) -> Result<Vec<(&'static str, Vec<String>)>> {
        let mut out = Vec::new();
        for sql in LOOKUPS {
            let rows = self
                .conn
                .prepare(format!("EXPLAIN QUERY PLAN {sql}"))?
                .run_collect_rows()?;
            let lines = rows
                .iter()
                .map(|r| r.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(" "))
                .collect();
            out.push((*sql, lines));
        }
        Ok(out)
    }

    pub(crate) fn rollback(&mut self) {
        let _ = self.conn.execute("ROLLBACK");
    }

    pub(crate) fn put_meta(&mut self, m: &Meta) -> Result<()> {
        for (k, v) in [
            (META_GENERATION, m.generation),
            (META_PAGE_SIZE, (u64::from(m.format) << 32) | u64::from(m.page_size)),
            (META_NEXT_ID, m.next_id),
            (META_TRUNK_EPOCH, m.trunk_epoch),
            (META_TRUNK_CHILDREN, m.trunk_children),
            (META_LEASE_NOW, m.lease_now_ms),
            (META_ARENA_HW, m.arena_hw as u64),
            (META_IN_USE, m.in_use),
            (META_STATES, m.states),
        ] {
            self.meta_put
                .exec(&[Value::from_i64(k), int(v)], &mut self.counters)?;
        }
        Ok(())
    }

    /// Pin a read snapshot on this connection (F-FZ phase 1): a deferred BEGIN takes its snapshot
    /// at its first read, so one is made now, before the checkpoint's writer can commit. Every read
    /// on this connection until [`Catalog::end_read_snapshot`] sees the catalog as of here.
    pub(crate) fn begin_read_snapshot(&mut self) -> Result<()> {
        self.conn.execute("BEGIN")?;
        if let Err(e) = self.meta_all.rows(&[], &mut self.counters) {
            self.rollback();
            return Err(e);
        }
        Ok(())
    }

    /// End the snapshot [`Catalog::begin_read_snapshot`] pinned (F-FZ phase 3). It wrote nothing,
    /// so a failure to end it is a failure to release a read mark only.
    pub(crate) fn end_read_snapshot(&mut self) {
        if let Err(e) = self.conn.execute("COMMIT") {
            tracing::warn!("branch catalog read snapshot not ended: {e}");
            self.rollback();
        }
    }

    /// A PASSIVE checkpoint of the catalog's WAL: backfill what no reader's mark holds back, block
    /// nobody (F-FZ phase 4, before the TRUNCATE attempt).
    pub(crate) fn wal_passive(&mut self) -> Result<()> {
        self.conn
            .prepare("PRAGMA wal_checkpoint(PASSIVE)")?
            .run_collect_rows()?;
        Ok(())
    }

    /// One branch, whole: its row, its current map and its retained versions.
    pub(crate) fn load_branch(&mut self, id: u64) -> Result<Option<CatBranch>> {
        let rows = self.branch_get.rows(&[int(id)], &mut self.counters)?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let lease = match row.get(4) {
            Some(Value::Null) => None,
            _ => Some(get(row, 4)?),
        };
        let mut b = CatBranch {
            id,
            parent: get(row, 0)?,
            fork_epoch: get(row, 1)?,
            epoch: get(row, 2)?,
            released: false,
            held_open: false,
            lease,
            n_children: get(row, 5)?,
            current: Vec::new(),
            retained: Vec::new(),
            name: None,
        };
        (b.released, b.held_open) = match get(row, 3)? {
            0 => (false, false),
            1 => (true, false),
            2 => (true, true),
            other => {
                return Err(LimboError::Corrupt(format!(
                    "branch catalog: branch {id} has released = {other}"
                )))
            }
        };
        let lo = cur_key(id, 0)?;
        let hi = lo + (1i64 << 32);
        for row in self
            .cur_range
            .rows(&[Value::from_i64(lo), Value::from_i64(hi)], &mut self.counters)?
        {
            let k = get(&row, 0)?;
            b.current.push((
                (k & 0xFFFF_FFFF) as u32,
                get(&row, 1)? as Slot,
                get(&row, 2)?,
                get(&row, 3)? as u32,
            ));
        }
        for row in self.ret_owner.rows(&[int(id)], &mut self.counters)? {
            b.retained.push((
                get(&row, 0)? as u32,
                get(&row, 1)?,
                get(&row, 2)?,
                get(&row, 3)? as Slot,
                get(&row, 4)? as u32,
            ));
        }
        b.name = self.name_of(id)?;
        Ok(Some(b))
    }

    /// The unreleased branch named `name`, as of the last checkpoint.
    pub(crate) fn name_get(&mut self, name: &str) -> Result<Option<u64>> {
        let rows = self.name_get.rows(&[Value::from_text(name.to_string())], &mut self.counters)?;
        rows.first().map(|row| get(row, 0)).transpose()
    }

    /// The name of branch `id`, as of the last checkpoint.
    pub(crate) fn name_of(&mut self, id: u64) -> Result<Option<String>> {
        let rows = self.name_of.rows(&[int(id)], &mut self.counters)?;
        match rows.first().and_then(|row| row.first()) {
            None => Ok(None),
            Some(Value::Text(t)) => Ok(Some(t.as_str().to_string())),
            Some(other) => Err(LimboError::Corrupt(format!(
                "branch catalog: branch {id}'s name is {other:?}"
            ))),
        }
    }

    /// Name `id` (replacing any row the name had).
    pub(crate) fn name_put(&mut self, name: &str, id: u64) -> Result<()> {
        self.name_put
            .exec(&[Value::from_text(name.to_string()), int(id)], &mut self.counters)
    }

    /// Free `name`.
    pub(crate) fn name_del(&mut self, name: &str) -> Result<()> {
        self.name_del.exec(&[Value::from_text(name.to_string())], &mut self.counters)
    }

    /// Rewrite the mutable columns of a branch row the catalog already holds (parent and fork
    /// epoch change only in a splice, which `rekey` writes; this leaves the children index alone).
    pub(crate) fn update_row(&mut self, b: &CatBranch) -> Result<()> {
        self.branch_update.exec(
            &[
                int(b.id),
                int(b.epoch),
                released_code(b),
                b.lease.map_or(Value::Null, int),
                int(b.n_children),
            ],
            &mut self.counters,
        )
    }

    /// Move a branch row to a new (parent, fork epoch) key: a splice put it in its spliced-out
    /// parent's place (r11-ever's F7 durable port, finding U6). The `branch_children` index follows
    /// the row, so the parent's children are listed under the key they are queried by.
    pub(crate) fn rekey(&mut self, b: &CatBranch) -> Result<()> {
        self.branch_rekey
            .exec(&[int(b.id), int(b.parent), int(b.fork_epoch)], &mut self.counters)
    }

    /// Replace a branch's `cur` rows.
    pub(crate) fn put_cur(&mut self, b: &CatBranch) -> Result<()> {
        let lo = cur_key(b.id, 0)?;
        self.cur_del_range.exec(
            &[Value::from_i64(lo), Value::from_i64(lo + (1i64 << 32))],
            &mut self.counters,
        )?;
        for &(page, slot, born, crc) in &b.current {
            self.cur_put.exec(
                &[
                    Value::from_i64(cur_key(b.id, page)?),
                    int(slot as u64),
                    int(born),
                    int(crc as u64),
                ],
                &mut self.counters,
            )?;
        }
        Ok(())
    }

    /// Replace a branch's retained versions.
    pub(crate) fn put_ret(&mut self, b: &CatBranch) -> Result<()> {
        self.ret_del_owner.exec(&[int(b.id)], &mut self.counters)?;
        for &(page, born, died, slot, crc) in &b.retained {
            self.ret_put.exec(
                &[
                    int(b.id),
                    int(page as u64),
                    int(born),
                    int(died),
                    int(slot as u64),
                    int(crc as u64),
                ],
                &mut self.counters,
            )?;
        }
        Ok(())
    }

    /// Write one branch whole, replacing whatever the catalog held for it.
    pub(crate) fn put_branch(&mut self, b: &CatBranch) -> Result<()> {
        self.branch_put.exec(
            &[
                int(b.id),
                int(b.parent),
                int(b.fork_epoch),
                int(b.epoch),
                released_code(b),
                b.lease.map_or(Value::Null, int),
                int(b.n_children),
            ],
            &mut self.counters,
        )?;
        let lo = cur_key(b.id, 0)?;
        self.cur_del_range.exec(
            &[Value::from_i64(lo), Value::from_i64(lo + (1i64 << 32))],
            &mut self.counters,
        )?;
        for &(page, slot, born, crc) in &b.current {
            self.cur_put.exec(
                &[
                    Value::from_i64(cur_key(b.id, page)?),
                    int(slot as u64),
                    int(born),
                    int(crc as u64),
                ],
                &mut self.counters,
            )?;
        }
        self.ret_del_owner.exec(&[int(b.id)], &mut self.counters)?;
        for &(page, born, died, slot, crc) in &b.retained {
            self.ret_put.exec(
                &[
                    int(b.id),
                    int(page as u64),
                    int(born),
                    int(died),
                    int(slot as u64),
                    int(crc as u64),
                ],
                &mut self.counters,
            )?;
        }
        Ok(())
    }

    pub(crate) fn delete_branch(&mut self, id: u64) -> Result<()> {
        self.branch_del.exec(&[int(id)], &mut self.counters)?;
        self.name_del_id.exec(&[int(id)], &mut self.counters)?;
        let lo = cur_key(id, 0)?;
        self.cur_del_range.exec(
            &[Value::from_i64(lo), Value::from_i64(lo + (1i64 << 32))],
            &mut self.counters,
        )?;
        self.ret_del_owner.exec(&[int(id)], &mut self.counters)?;
        Ok(())
    }

    /// The trunk's version of `page` with the greatest `born <= at` — the only one that can hold a
    /// child forked at `at`, since a page's versions are disjoint — or, with `at = u64::MAX`, the
    /// page's last version (whose `died` is the trunk's last write of the page). One index probe
    /// (`ret_page`, descending): the multiversion B-tree's "version of key k at time t" (C-P).
    pub(crate) fn trunk_pred(&mut self, page: u32, at: u64) -> Result<Option<(u64, u64, Slot, u32)>> {
        let rows = self
            .ret_pred
            .rows(&[int(0), int(page as u64), int(at)], &mut self.counters)?;
        rows.first()
            .map(|row| Ok((get(row, 0)?, get(row, 1)?, get(row, 2)? as Slot, get(row, 3)? as u32)))
            .transpose()
    }

    /// Up to `limit` trunk versions born in `(lo, hi]` (`lo = None`: unbounded below), ascending by
    /// `born`: F2's by-birth range, read in place from `ret_born`. (page, born, died, slot, crc).
    pub(crate) fn trunk_born_range(
        &mut self,
        lo: Option<u64>,
        hi: u64,
        limit: u64,
    ) -> Result<Vec<(u32, u64, u64, Slot, u32)>> {
        let lo = lo.map_or(Value::from_i64(-1), int);
        let rows = self
            .ret_born_range
            .rows(&[int(0), lo, int(hi), int(limit)], &mut self.counters)?;
        rows.iter().map(|r| ret_row(r)).collect()
    }

    /// Up to `limit` trunk versions that died in `(lo, hi]` (`hi = None`: unbounded above),
    /// ascending by `died`: F2's by-death range, read in place from `ret_died`.
    pub(crate) fn trunk_died_range(
        &mut self,
        lo: u64,
        hi: Option<u64>,
        limit: u64,
    ) -> Result<Vec<(u32, u64, u64, Slot, u32)>> {
        let hi = hi.map_or(Value::from_i64(i64::MAX), int);
        let rows = self
            .ret_died_range
            .rows(&[int(0), int(lo), hi, int(limit)], &mut self.counters)?;
        rows.iter().map(|r| ret_row(r)).collect()
    }

    /// Delete one trunk version (a checkpoint, for a version reaped since the last one).
    pub(crate) fn trunk_delete(&mut self, page: u32, born: u64) -> Result<()> {
        self.ret_del_one
            .exec(&[int(0), int(page as u64), int(born)], &mut self.counters)
    }

    /// Insert one trunk version (a checkpoint, for a version retained since the last one).
    pub(crate) fn trunk_insert(&mut self, page: u32, born: u64, died: u64, slot: Slot, crc: u32) -> Result<()> {
        self.ret_put.exec(
            &[int(0), int(page as u64), int(born), int(died), int(slot as u64), int(crc as u64)],
            &mut self.counters,
        )
    }

    /// Trunk versions in the catalog (an instrument's count, not a per-operation lookup).
    pub(crate) fn trunk_count(&mut self) -> Result<u64> {
        let rows = self.ret_count.rows(&[int(0)], &mut self.counters)?;
        rows.first().map_or(Ok(0), |r| get(r, 0))
    }

    pub(crate) fn any_retained(&mut self) -> Result<bool> {
        Ok(!self.ret_any.rows(&[], &mut self.counters)?.is_empty())
    }

    /// Children of `parent` forked in `[from, to)`, ascending, at most `limit`.
    pub(crate) fn children_in(&mut self, parent: u64, from: u64, to: u64, limit: usize) -> Result<Vec<u64>> {
        self.child_in
            .rows(
                &[int(parent), int(from), int(to), int(limit as u64)],
                &mut self.counters,
            )?
            .iter()
            .map(|row| get(row, 0))
            .collect()
    }

    /// Children of `parent` forked before `f`, nearest first, at most `limit`.
    pub(crate) fn children_below(&mut self, parent: u64, f: u64, limit: usize) -> Result<Vec<u64>> {
        self.child_below
            .rows(&[int(parent), int(f), int(limit as u64)], &mut self.counters)?
            .iter()
            .map(|row| get(row, 0))
            .collect()
    }

    /// Children of `parent` forked after `f`, nearest first, at most `limit`.
    pub(crate) fn children_above(&mut self, parent: u64, f: u64, limit: usize) -> Result<Vec<u64>> {
        self.child_above
            .rows(&[int(parent), int(f), int(limit as u64)], &mut self.counters)?
            .iter()
            .map(|row| get(row, 0))
            .collect()
    }

    /// F-EXP: up to `limit` rows `(id, deadline)` with a deadline at or before `now`, in (deadline,
    /// id) order after the keyset cursor `after`, and whether the rows due ran out before `limit`.
    /// Two range seeks on `branch_lease`, so a page costs what it returns, however many are due.
    pub(crate) fn lease_due_page(
        &mut self,
        now: u64,
        after: Option<(u64, u64)>,
        limit: usize,
    ) -> Result<(Vec<(u64, u64)>, bool)> {
        let mut out = Vec::new();
        let floor = match after {
            Some((lease, id)) => {
                for row in self
                    .lease_tie
                    .rows(&[int(lease), int(id), int(limit as u64)], &mut self.counters)?
                {
                    out.push((get(&row, 0)?, lease));
                }
                int(lease)
            }
            None => Value::from_i64(-1),
        };
        if out.len() < limit {
            let rest = (limit - out.len()) as u64;
            for row in self
                .lease_page
                .rows(&[int(now), floor, int(rest)], &mut self.counters)?
            {
                out.push((get(&row, 0)?, get(&row, 1)?));
            }
        }
        let complete = out.len() < limit;
        Ok((out, complete))
    }

    pub(crate) fn lease_min(&mut self) -> Result<Option<u64>> {
        let rows = self.lease_min.rows(&[], &mut self.counters)?;
        match rows.first().and_then(|r| r.first()) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => Ok(v.as_int().map(|v| v as u64)),
        }
    }

    /// The earliest lease deadline after `now`, as a floor for the next expiry query.
    pub(crate) fn lease_min_after(&mut self, now: u64) -> Result<Option<u64>> {
        let rows = self.lease_min_after.rows(&[int(now)], &mut self.counters)?;
        match rows.first().and_then(|r| r.first()) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => Ok(v.as_int().map(|v| v as u64)),
        }
    }

    /// The child `parent` forked at `f`, if the catalog lists one (its id: the child queries above
    /// return only fork epochs).
    pub(crate) fn child_at(&mut self, parent: u64, f: u64) -> Result<Option<u64>> {
        let rows = self.child_at.rows(&[int(parent), int(f)], &mut self.counters)?;
        rows.first().map(|row| get(row, 0)).transpose()
    }

    /// Branches released while a connection held them open: what an open collects (see the
    /// `branch_released` index).
    pub(crate) fn released_ids(&mut self) -> Result<Vec<u64>> {
        self.released
            .rows(&[], &mut self.counters)?
            .iter()
            .map(|row| get(row, 0))
            .collect()
    }

    pub(crate) fn unreleased_ids(&mut self) -> Result<Vec<u64>> {
        self.unreleased
            .rows(&[], &mut self.counters)?
            .iter()
            .map(|row| get(row, 0))
            .collect()
    }

    /// Up to `limit` free slots above `after` (`None`: from the start), ascending.
    pub(crate) fn free_batch(&mut self, after: Option<Slot>, limit: usize) -> Result<Vec<Slot>> {
        let rows = match after {
            Some(a) => self
                .free_after
                .rows(&[int(a as u64), int(limit as u64)], &mut self.counters)?,
            None => self.free_first.rows(&[int(limit as u64)], &mut self.counters)?,
        };
        rows.iter().map(|row| get(row, 0).map(|s| s as Slot)).collect()
    }

    pub(crate) fn free_has(&mut self, slot: Slot) -> Result<bool> {
        Ok(!self
            .free_has
            .rows(&[int(slot as u64)], &mut self.counters)?
            .is_empty())
    }

    pub(crate) fn free_all(&mut self) -> Result<Vec<Slot>> {
        self.free_all
            .rows(&[], &mut self.counters)?
            .iter()
            .map(|row| get(row, 0).map(|s| s as Slot))
            .collect()
    }

    pub(crate) fn free_delete_upto(&mut self, slot: Slot) -> Result<()> {
        self.free_del_upto.exec(&[int(slot as u64)], &mut self.counters)
    }

    pub(crate) fn free_delete(&mut self, slot: Slot) -> Result<()> {
        self.free_del.exec(&[int(slot as u64)], &mut self.counters)
    }

    pub(crate) fn free_put(&mut self, slot: Slot) -> Result<()> {
        self.free_put.exec(&[int(slot as u64)], &mut self.counters)
    }
}

/// r12-catload: a bare catalog handle for the catalog-only load arm (the harness's `catload`, on a
/// `catalog_only_fixture` or a store's catalog file): open the catalog, read its meta row, prewarm it
/// as `R12_PREWARM` says (the files part only when `R12_PREWARM_FILES` includes the catalog), then
/// load branches one at a time, each as a store's first touch does (`Catalog::load_branch`), with no
/// store around it, so the harness can time and count each load alone.
#[doc(hidden)]
pub struct CatalogProbe {
    cat: Catalog,
}

impl CatalogProbe {
    pub fn open(path: &Path) -> Result<CatalogProbe> {
        let mut cat = Catalog::open(path, SyncClass::Off)?;
        cat.meta()?;
        let (mode, targets) = super::prewarm::from_env()?;
        if !mode.warms_files() || targets.catalog {
            cat.prewarm(path, mode)?;
        }
        Ok(CatalogProbe { cat })
    }

    /// Load branch `id` whole, as a first touch does; `Ok(false)`: the catalog has no such branch.
    pub fn load(&mut self, id: u64) -> Result<bool> {
        Ok(self.cat.load_branch(id)?.is_some())
    }

    /// `(catalog queries, rows read)` since open.
    pub fn counters(&self) -> (u64, u64) {
        (self.cat.counters.queries, self.cat.counters.rows_read)
    }

    /// The open's prewarm, as `Database::branch_prewarm` reports it.
    pub fn prewarm(&self) -> (&'static str, u64, u64, u64, u64, u64, u64, u64) {
        let p = self.cat.prewarm;
        (
            p.mode.name(),
            p.files,
            p.bytes,
            p.advised,
            p.pages,
            p.interior,
            p.cache_pages,
            p.ns,
        )
    }
}

/// A13 amended (r11-restart-r2): the catalog checkpoint's own cost against the catalog's size, with
/// no branch store and no arena (a 10^8-branch arena would be 400 GB; the checkpoint's cost is
/// catalog work). `measure = false` BUILDS a catalog at `path` holding branches `1..=n`, one `cur`
/// row each on one of 1,000 pages, in transactions of 100,000 rows. `measure = true` runs, on such a
/// catalog, the statements a checkpoint issues for `d` distinct random dirty branches (one `put_cur`
/// each, as fix v3 writes a committed branch) and the meta row in ONE transaction, then the WAL
/// backfill, and returns a line of counters. Exposed for the `branch_restart catonly` harness only.
#[doc(hidden)]
pub fn catalog_only_fixture(path: &Path, n: u64, d: u64, measure: bool, seed: u64) -> Result<String> {
    let page = |id: u64| 2 + (id % 1000) as u32;
    if !measure {
        let mut cat = Catalog::open(path, SyncClass::Off)?;
        let mut lo = 1u64;
        while lo <= n {
            let hi = (lo + 99_999).min(n);
            cat.begin()?;
            let bulk = cat
                .conn
                .execute(format!(
                    "INSERT INTO branch(id, parent, fork_epoch, epoch, released, lease, n_children) \
                     SELECT value, 0, value - 1, 0, 0, NULL, 0 FROM generate_series({lo}, {hi})"
                ))
                .and_then(|_| {
                    cat.conn.execute(format!(
                        "INSERT INTO cur(k, slot, born, crc) SELECT (value << 32) | (2 + value % 1000), \
                         value, 0, 0 FROM generate_series({lo}, {hi})"
                    ))
                });
            if bulk.is_err() {
                // No table-valued generate_series here: the same rows, one statement each.
                cat.rollback();
                cat.begin()?;
                for id in lo..=hi {
                    let b = CatBranch {
                        id,
                        fork_epoch: id - 1,
                        current: vec![(page(id), id as Slot, 0, 0)],
                        ..CatBranch::default()
                    };
                    cat.put_branch(&b)?;
                }
            }
            cat.commit()?;
            lo = hi + 1;
        }
        cat.truncate_wal()?;
        let rows = cat.conn.prepare("SELECT count(*) FROM cur")?.run_collect_rows()?;
        let count = rows.first().map_or(Ok(0), |r| get(r, 0))?;
        if count != n {
            return Err(LimboError::InternalError(format!(
                "catalog-only fixture: {count} cur rows, expected {n}"
            )));
        }
        return Ok(format!("CATONLY_BUILD\tn={n}\tcur_rows={count}"));
    }
    let mut cat = Catalog::open(path, SyncClass::Fsync)?;
    let mut x = seed | 1;
    let mut ids = std::collections::HashSet::new();
    while (ids.len() as u64) < d.min(n) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        ids.insert(1 + x % n);
    }
    let io0 = super::page_io();
    let bf0 = super::backfill_io();
    let t = std::time::Instant::now();
    cat.begin()?;
    for &id in &ids {
        let b = CatBranch {
            id,
            current: vec![(page(id), id as Slot, 1, 1)],
            ..CatBranch::default()
        };
        cat.put_cur(&b)?;
    }
    cat.put_meta(&Meta {
        generation: 2,
        ..Meta::default()
    })?;
    cat.commit()?;
    let commit_us = t.elapsed().as_secs_f64() * 1e6;
    let io1 = super::page_io();
    let t = std::time::Instant::now();
    let truncated = cat.truncate_wal()?;
    let backfill_us = t.elapsed().as_secs_f64() * 1e6;
    let io2 = super::page_io();
    let bf2 = super::backfill_io();
    Ok(format!(
        "CATONLY\tn={n}\td={}\tcommit_us={commit_us:.1}\tbackfill_us={backfill_us:.1}\t\
         commit_db_page_reads={}\tcommit_wal_frame_writes={}\tbackfill_db_page_writes={}\t\
         backfill_db_page_reads={}\tbackfill_wal_frame_reads={}\tbackfill_cache_hits={}\t\
         backfill_wal_reads_issued={}\ttruncate={truncated:?}\tcache_pages={}",
        ids.len(),
        io1[0] - io0[0],
        io1[3] - io0[3],
        io2[1] - io1[1],
        io2[0] - io1[0],
        io2[2] - io1[2],
        bf2[0] - bf0[0],
        bf2[1] - bf0[1],
        cat_cache_pages()
    ))
}
