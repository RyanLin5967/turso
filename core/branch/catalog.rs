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
//! the snapshot's rename rule, with the catalog commit as the commit point. The arena is synced
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
//!   not only the changed pages.

use std::num::NonZero;
use std::path::Path;

use super::arena::Slot;
use crate::sync::Arc;
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
    // Released branches with no live child: only these can be collected at an open (a released
    // interior with children is retired and stays), so an open never walks retired interiors.
    "CREATE INDEX IF NOT EXISTS branch_released ON branch(released, n_children)",
    // k = branch << 32 | page: one B-tree lookup per (branch, page), one range per branch.
    "CREATE TABLE IF NOT EXISTS cur(k INTEGER PRIMARY KEY, slot INTEGER NOT NULL, \
     born INTEGER NOT NULL, crc INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS ret(owner INTEGER NOT NULL, page INTEGER NOT NULL, \
     born INTEGER NOT NULL, died INTEGER NOT NULL, slot INTEGER NOT NULL, crc INTEGER NOT NULL)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ret_page ON ret(owner, page, born)",
    "CREATE INDEX IF NOT EXISTS ret_born ON ret(owner, born)",
    "CREATE TABLE IF NOT EXISTS free(slot INTEGER PRIMARY KEY)",
];

/// The catalog's per-operation lookups, as prepared below (for the plan test).
#[cfg(test)]
const LOOKUPS: &[&str] = &[
    "SELECT parent, fork_epoch, epoch, released, lease, n_children FROM branch WHERE id = ?1",
    "SELECT k, slot, born, crc FROM cur WHERE k >= ?1 AND k < ?2",
    "SELECT page, born, died, slot, crc FROM ret WHERE owner = ?1",
    "SELECT born, died, slot, crc FROM ret WHERE owner = ?1 AND page = ?2",
    "SELECT page FROM ret WHERE owner = ?1 AND born > ?2 AND born <= ?3",
    "SELECT fork_epoch, id FROM branch WHERE parent = ?1 AND fork_epoch >= ?2 AND fork_epoch < ?3 ORDER BY fork_epoch ASC LIMIT ?4",
    "SELECT fork_epoch, id FROM branch WHERE parent = ?1 AND fork_epoch < ?2 ORDER BY fork_epoch DESC LIMIT ?3",
    "SELECT fork_epoch, id FROM branch WHERE parent = ?1 AND fork_epoch > ?2 ORDER BY fork_epoch ASC LIMIT ?3",
    "SELECT id FROM branch WHERE lease > -1 AND lease <= ?1",
    "SELECT lease FROM branch WHERE lease > -1 ORDER BY lease ASC LIMIT 1",
    "SELECT lease FROM branch WHERE lease > ?1 ORDER BY lease ASC LIMIT 1",
    "SELECT id FROM branch WHERE released = 1 AND n_children = 0",
    "SELECT slot FROM free WHERE slot > ?1 ORDER BY slot ASC LIMIT ?2",
    "SELECT slot FROM free WHERE slot > -1 ORDER BY slot ASC LIMIT ?1",
    "SELECT 1 FROM free WHERE slot = ?1",
    "DELETE FROM cur WHERE k >= ?1 AND k < ?2",
    "DELETE FROM ret WHERE owner = ?1",
    "DELETE FROM ret WHERE owner = ?1 AND page = ?2",
    "DELETE FROM ret WHERE owner = ?1 AND page = ?2 AND born = ?3",
    "DELETE FROM free WHERE slot <= ?1",
    "DELETE FROM free WHERE slot = ?1",
    "DELETE FROM branch WHERE id = ?1",
    "UPDATE branch SET epoch = ?2, released = ?3, lease = ?4, n_children = ?5 WHERE id = ?1",
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
}

/// One branch as the catalog holds it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CatBranch {
    pub(crate) id: u64,
    pub(crate) parent: u64,
    pub(crate) fork_epoch: u64,
    pub(crate) epoch: u64,
    pub(crate) released: bool,
    pub(crate) lease: Option<u64>,
    pub(crate) n_children: u64,
    /// (page, slot, born, crc)
    pub(crate) current: Vec<(u32, Slot, u64, u32)>,
    /// (page, born, died, slot, crc)
    pub(crate) retained: Vec<(u32, u64, u64, Slot, u32)>,
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

pub(crate) struct Catalog {
    _db: Arc<Database>,
    conn: Arc<Connection>,
    pub(crate) counters: CatalogCounters,
    meta_all: Stmt,
    meta_put: Stmt,
    branch_get: Stmt,
    branch_put: Stmt,
    branch_update: Stmt,
    branch_del: Stmt,
    cur_range: Stmt,
    cur_del_range: Stmt,
    cur_put: Stmt,
    ret_owner: Stmt,
    ret_page: Stmt,
    ret_pages_born: Stmt,
    ret_del_owner: Stmt,
    ret_del_page: Stmt,
    /// C-FIX (githost-shape): one trunk version by its key, for per-version checkpoint deltas.
    ret_del_version: Stmt,
    ret_put: Stmt,
    ret_any: Stmt,
    child_in: Stmt,
    child_below: Stmt,
    child_above: Stmt,
    lease_due: Stmt,
    lease_min: Stmt,
    lease_min_after: Stmt,
    trunk_counts: Stmt,
    released: Stmt,
    unreleased: Stmt,
    free_after: Stmt,
    free_first: Stmt,
    free_has: Stmt,
    free_all: Stmt,
    free_del_upto: Stmt,
    free_del: Stmt,
    free_put: Stmt,
}

impl Catalog {
    /// Open (creating if absent) the catalog at `path`. `sync` selects `synchronous = FULL`, so a
    /// checkpoint's commit is durable before the log starts over; without it, OFF (a measurement
    /// arm, like `Durable { sync: false }`).
    pub(crate) fn open(path: &Path, sync: bool) -> Result<Catalog> {
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
        conn.execute(if sync {
            "PRAGMA synchronous = FULL"
        } else {
            "PRAGMA synchronous = OFF"
        })?;
        for sql in SCHEMA {
            conn.execute(*sql)?;
        }
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
            branch_del: p("DELETE FROM branch WHERE id = ?1")?,
            cur_range: p("SELECT k, slot, born, crc FROM cur WHERE k >= ?1 AND k < ?2")?,
            cur_del_range: p("DELETE FROM cur WHERE k >= ?1 AND k < ?2")?,
            cur_put: p("INSERT OR REPLACE INTO cur(k, slot, born, crc) VALUES (?1, ?2, ?3, ?4)")?,
            ret_owner: p("SELECT page, born, died, slot, crc FROM ret WHERE owner = ?1")?,
            ret_page: p("SELECT born, died, slot, crc FROM ret WHERE owner = ?1 AND page = ?2")?,
            ret_pages_born: p(
                "SELECT page FROM ret WHERE owner = ?1 AND born > ?2 AND born <= ?3",
            )?,
            ret_del_owner: p("DELETE FROM ret WHERE owner = ?1")?,
            ret_del_page: p("DELETE FROM ret WHERE owner = ?1 AND page = ?2")?,
            ret_del_version: p("DELETE FROM ret WHERE owner = ?1 AND page = ?2 AND born = ?3")?,
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
            // Range seeks on `branch_lease`: NULL sorts first in the index, so `lease > -1` skips
            // every unleased row instead of walking them (deadlines are never negative).
            lease_due: p("SELECT id FROM branch WHERE lease > -1 AND lease <= ?1")?,
            lease_min: p("SELECT lease FROM branch WHERE lease > -1 ORDER BY lease ASC LIMIT 1")?,
            lease_min_after: p(
                "SELECT lease FROM branch WHERE lease > ?1 ORDER BY lease ASC LIMIT 1",
            )?,
            trunk_counts: p("SELECT page, count(*) FROM ret WHERE owner = 0 GROUP BY page")?,
            released: p("SELECT id FROM branch WHERE released = 1 AND n_children = 0")?,
            unreleased: p("SELECT id FROM branch WHERE released = 0")?,
            free_after: p("SELECT slot FROM free WHERE slot > ?1 ORDER BY slot ASC LIMIT ?2")?,
            free_first: p("SELECT slot FROM free WHERE slot > -1 ORDER BY slot ASC LIMIT ?1")?,
            free_has: p("SELECT 1 FROM free WHERE slot = ?1")?,
            free_all: p("SELECT slot FROM free")?,
            free_del_upto: p("DELETE FROM free WHERE slot <= ?1")?,
            free_del: p("DELETE FROM free WHERE slot = ?1")?,
            free_put: p("INSERT OR REPLACE INTO free(slot) VALUES (?1)")?,
            counters: CatalogCounters::default(),
            conn,
            _db: db,
        })
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
                META_PAGE_SIZE => m.page_size = v as u32,
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
            (META_PAGE_SIZE, m.page_size as u64),
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
            released: get(row, 3)? != 0,
            lease,
            n_children: get(row, 5)?,
            current: Vec::new(),
            retained: Vec::new(),
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
        Ok(Some(b))
    }

    /// Rewrite the mutable columns of a branch row the catalog already holds (parent and fork
    /// epoch never change, so the children index is not touched).
    pub(crate) fn update_row(&mut self, b: &CatBranch) -> Result<()> {
        self.branch_update.exec(
            &[
                int(b.id),
                int(b.epoch),
                Value::from_i64(b.released as i64),
                b.lease.map_or(Value::Null, int),
                int(b.n_children),
            ],
            &mut self.counters,
        )
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
                Value::from_i64(b.released as i64),
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
        let lo = cur_key(id, 0)?;
        self.cur_del_range.exec(
            &[Value::from_i64(lo), Value::from_i64(lo + (1i64 << 32))],
            &mut self.counters,
        )?;
        self.ret_del_owner.exec(&[int(id)], &mut self.counters)?;
        Ok(())
    }

    /// The trunk's retained versions of `page`: (born, died, slot, crc).
    pub(crate) fn trunk_page(&mut self, page: u32) -> Result<Vec<(u64, u64, Slot, u32)>> {
        self.ret_page
            .rows(&[int(0), int(page as u64)], &mut self.counters)?
            .iter()
            .map(|row| {
                Ok((
                    get(row, 0)?,
                    get(row, 1)?,
                    get(row, 2)? as Slot,
                    get(row, 3)? as u32,
                ))
            })
            .collect()
    }

    /// Superseded by per-version deltas (githost-shape C-FIX: `put_trunk_version`,
    /// `delete_trunk_version`); no caller left. Kept, with its statement, so the plan test still
    /// covers what an older catalog's code ran.
    #[allow(dead_code)]
    pub(crate) fn put_trunk_page(&mut self, page: u32, versions: &[(u64, u64, Slot, u32)]) -> Result<()> {
        self.ret_del_page
            .exec(&[int(0), int(page as u64)], &mut self.counters)?;
        for &(born, died, slot, crc) in versions {
            self.ret_put.exec(
                &[
                    int(0),
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

    /// C-FIX (githost-shape): insert one trunk version (a version is immutable once retained).
    pub(crate) fn put_trunk_version(
        &mut self,
        page: u32,
        born: u64,
        died: u64,
        slot: Slot,
        crc: u32,
    ) -> Result<()> {
        self.ret_put.exec(
            &[
                int(0),
                int(page as u64),
                int(born),
                int(died),
                int(slot as u64),
                int(crc as u64),
            ],
            &mut self.counters,
        )
    }

    /// C-FIX (githost-shape): delete one trunk version by its key `(0, page, born)`.
    pub(crate) fn delete_trunk_version(&mut self, page: u32, born: u64) -> Result<()> {
        self.ret_del_version
            .exec(&[int(0), int(page as u64), int(born)], &mut self.counters)
    }

    /// Trunk pages holding a retained version born in `(lo, hi]` (`lo = None`: unbounded below).
    pub(crate) fn trunk_pages_born_in(&mut self, lo: Option<u64>, hi: u64) -> Result<Vec<u32>> {
        let lo = lo.map_or(Value::from_i64(-1), int);
        let mut pages: Vec<u32> = self
            .ret_pages_born
            .rows(&[int(0), lo, int(hi)], &mut self.counters)?
            .iter()
            .map(|row| get(row, 0).map(|p| p as u32))
            .collect::<Result<_>>()?;
        pages.sort_unstable();
        pages.dedup();
        Ok(pages)
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

    pub(crate) fn lease_due(&mut self, now: u64) -> Result<Vec<u64>> {
        self.lease_due
            .rows(&[int(now)], &mut self.counters)?
            .iter()
            .map(|row| get(row, 0))
            .collect()
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

    /// `(page, versions)` for every trunk page with retained versions (an instrument's scan).
    pub(crate) fn trunk_version_counts(&mut self) -> Result<Vec<(u32, u64)>> {
        self.trunk_counts
            .rows(&[], &mut self.counters)?
            .iter()
            .map(|row| Ok((get(row, 0)? as u32, get(row, 1)?)))
            .collect()
    }

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
