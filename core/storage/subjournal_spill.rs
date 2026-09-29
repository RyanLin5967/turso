//! A statement journal that lives in memory up to a spill threshold and in a temporary file past
//! it: SQLite's `SQLITE_CONFIG_STMTJRNL_SPILL` ("Statement journals are held in memory until their
//! size (in bytes) exceeds this threshold, at which point they are written to disk. Or if the
//! threshold is -1, statement journals are always held exclusively in memory", sqlite.org
//! c3ref/c_config_covering_index_scan.html), as `memjrnlWrite` in SQLite's memjournal.c spills
//! once a write would end past `nSpill`.
//!
//! Why: the subjournal keeps each page's pre-image once per statement, so a statement that writes
//! D pages holds about D x 4,100 bytes. Held in [`crate::MemoryIO`], all of it stays resident until
//! the statement ends and is then freed in one go (4.1 GB at D = 10^6). Spilled, it costs the
//! kernel's page cache rather than the heap, and the truncate at the statement's end is a file
//! truncate.
//!
//! Every operation completes before it returns, as [`crate::MemoryIO`]'s do: the pager's savepoint
//! paths assert that the journal's I/O completes immediately and read it back synchronously.
//! The journal is never needed after a crash (the transaction itself is in the WAL), so `sync` is a
//! no-op and the spill file is an unnamed temporary file the OS removes when it is closed.

use std::fs::File as StdFile;

use crate::error::io_error;
use crate::io::{Buffer, Completion, File, FileSyncType};
use crate::sync::{Arc, Mutex};
use crate::Result;

/// The spill threshold SQLite's documentation recommends ("a value such as 64KiB").
pub(crate) const DEFAULT_SPILL_BYTES: u64 = 64 * 1024;

/// The spill threshold: `TURSO_STMTJRNL_SPILL` bytes, read once per process; a negative value means
/// never spill, as SQLite's `-1`. Unset, or not a number, means [`DEFAULT_SPILL_BYTES`].
pub(crate) fn spill_threshold() -> Option<u64> {
    static THRESHOLD: std::sync::OnceLock<Option<u64>> = std::sync::OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        let Ok(raw) = std::env::var("TURSO_STMTJRNL_SPILL") else {
            return Some(DEFAULT_SPILL_BYTES);
        };
        match raw.trim().parse::<i64>() {
            Ok(n) if n < 0 => None,
            Ok(n) => Some(n as u64),
            Err(_) => {
                tracing::warn!(
                    "TURSO_STMTJRNL_SPILL={raw:?} is not a number; using {DEFAULT_SPILL_BYTES}"
                );
                Some(DEFAULT_SPILL_BYTES)
            }
        }
    })
}

enum Backing {
    /// Below the threshold: the journal's bytes.
    Memory(Vec<u8>),
    /// Past it: an unnamed temporary file and the journal's length in it.
    Disk { file: StdFile, len: u64 },
}

/// A statement journal file, in memory until it would grow past its threshold.
pub(crate) struct SpillJournal {
    /// `None`: never spill.
    threshold: Option<u64>,
    backing: Mutex<Backing>,
}

crate::assert::assert_sync!(SpillJournal);

impl SpillJournal {
    pub(crate) fn new(threshold: Option<u64>) -> Self {
        Self {
            threshold,
            backing: Mutex::new(Backing::Memory(Vec::new())),
        }
    }

    /// The journal has moved to its temporary file.
    #[cfg(test)]
    pub(crate) fn is_spilled(&self) -> bool {
        matches!(*self.backing.lock(), Backing::Disk { .. })
    }

    fn len(&self) -> u64 {
        match &*self.backing.lock() {
            Backing::Memory(mem) => mem.len() as u64,
            Backing::Disk { len, .. } => *len,
        }
    }

    fn write_at(&self, pos: u64, data: &[u8]) -> Result<usize> {
        let end = pos + data.len() as u64;
        let mut backing = self.backing.lock();
        if let Backing::Memory(mem) = &mut *backing {
            if self.threshold.is_none_or(|t| end <= t) {
                if (mem.len() as u64) < end {
                    mem.resize(end as usize, 0);
                }
                mem[pos as usize..end as usize].copy_from_slice(data);
                super::page_cache::note_subjournal_mem(mem.len() as u64);
                return Ok(data.len());
            }
            // Spill: the in-memory prefix (at most the threshold) moves to the file once, and every
            // later write goes to the file.
            let file = temp_file()?;
            write_all_at(&file, mem, 0).map_err(|e| io_error(e, "statement journal spill"))?;
            let len = mem.len() as u64;
            *backing = Backing::Disk { file, len };
            super::page_cache::count_subjournal_spill();
        }
        let Backing::Disk { file, len } = &mut *backing else {
            unreachable!("the journal spilled above");
        };
        write_all_at(file, data, pos).map_err(|e| io_error(e, "statement journal write"))?;
        *len = (*len).max(end);
        Ok(data.len())
    }

    /// Read `pos..` into `dst`; a short count at the end of the journal.
    fn read_into(&self, pos: u64, dst: &mut [u8]) -> Result<usize> {
        match &*self.backing.lock() {
            Backing::Memory(mem) => {
                let len = mem.len() as u64;
                if pos >= len {
                    return Ok(0);
                }
                let n = dst.len().min((len - pos) as usize);
                dst[..n].copy_from_slice(&mem[pos as usize..pos as usize + n]);
                Ok(n)
            }
            Backing::Disk { file, len } => {
                if pos >= *len {
                    return Ok(0);
                }
                let want = dst.len().min((*len - pos) as usize);
                let mut done = 0;
                while done < want {
                    let n = read_at(file, &mut dst[done..want], pos + done as u64)
                        .map_err(|e| io_error(e, "statement journal read"))?;
                    if n == 0 {
                        break;
                    }
                    done += n;
                }
                Ok(done)
            }
        }
    }

    fn truncate_to(&self, new_len: u64) -> Result<()> {
        let mut backing = self.backing.lock();
        match &mut *backing {
            Backing::Memory(mem) => mem.resize(new_len as usize, 0),
            Backing::Disk { .. } if new_len == 0 => {
                // The statement is over: the file closes (and vanishes), and the next statement
                // starts in memory again.
                *backing = Backing::Memory(Vec::new());
            }
            Backing::Disk { file, len } => {
                file.set_len(new_len)
                    .map_err(|e| io_error(e, "statement journal truncate"))?;
                *len = new_len;
            }
        }
        Ok(())
    }
}

/// An unnamed temporary file in `TURSO_TMPDIR`, else `SQLITE_TMPDIR`, else the system's temporary
/// directory: the precedence `Connection::create_tempdir` uses for the temp database.
fn temp_file() -> Result<StdFile> {
    let made = match std::env::var_os("TURSO_TMPDIR").or_else(|| std::env::var_os("SQLITE_TMPDIR"))
    {
        Some(dir) => tempfile::tempfile_in(dir),
        None => tempfile::tempfile(),
    };
    made.map_err(|e| io_error(e, "statement journal tempfile"))
}

#[cfg(unix)]
fn write_all_at(file: &StdFile, buf: &[u8], pos: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, pos)
}

#[cfg(unix)]
fn read_at(file: &StdFile, buf: &mut [u8], pos: u64) -> std::io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.read_at(buf, pos)
}

#[cfg(windows)]
fn write_all_at(file: &StdFile, mut buf: &[u8], mut pos: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        let n = file.seek_write(buf, pos)?;
        if n == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        buf = &buf[n..];
        pos += n as u64;
    }
    Ok(())
}

#[cfg(windows)]
fn read_at(file: &StdFile, buf: &mut [u8], pos: u64) -> std::io::Result<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_read(buf, pos)
}

impl File for SpillJournal {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let n = {
            let buf = c.as_read().buf();
            self.read_into(pos, buf.as_mut_slice())?
        };
        c.complete(n as i32);
        Ok(c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        let n = self.write_at(pos, buffer.as_slice())?;
        c.complete(n as i32);
        Ok(c)
    }

    fn sync(&self, c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
        // Never read after a crash (see the module comment): nothing to make durable.
        c.complete(0);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        Ok(self.len())
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.truncate_to(len)?;
        c.complete(0);
        Ok(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{PlatformIO, IO};
    use crate::{Database, DatabaseOpts, OpenFlags, SqliteDialect};

    const REC: usize = 4100;

    fn record(tag: u8) -> Vec<u8> {
        (0..REC).map(|i| tag.wrapping_add(i as u8)).collect()
    }

    fn read(j: &SpillJournal, pos: u64, len: usize) -> Vec<u8> {
        let mut out = vec![0u8; len];
        let n = j.read_into(pos, &mut out).unwrap();
        out.truncate(n);
        out
    }

    #[test]
    fn a_journal_below_the_threshold_stays_in_memory_and_reads_back() {
        let j = SpillJournal::new(Some(4 * REC as u64));
        for k in 0..3u8 {
            assert_eq!(j.write_at(k as u64 * REC as u64, &record(k)).unwrap(), REC);
        }
        assert!(!j.is_spilled());
        assert_eq!(j.len(), 3 * REC as u64);
        for k in 0..3u8 {
            assert_eq!(read(&j, k as u64 * REC as u64, REC), record(k));
        }
    }

    #[test]
    fn crossing_the_threshold_spills_every_byte_to_a_file_and_reads_back_identically() {
        let j = SpillJournal::new(Some(3 * REC as u64));
        for k in 0..10u8 {
            j.write_at(k as u64 * REC as u64, &record(k)).unwrap();
            assert_eq!(j.is_spilled(), k >= 3, "spilled after record {k}");
        }
        assert_eq!(j.len(), 10 * REC as u64);
        for k in 0..10u8 {
            assert_eq!(read(&j, k as u64 * REC as u64, REC), record(k), "record {k}");
        }
        // A read running past the end is short, as MemoryIO's is.
        assert_eq!(read(&j, 9 * REC as u64 + 100, REC).len(), REC - 100);
        assert!(read(&j, 10 * REC as u64, REC).is_empty());
    }

    #[test]
    fn truncating_a_spilled_journal_to_zero_returns_it_to_memory() {
        let j = SpillJournal::new(Some(2 * REC as u64));
        for k in 0..5u8 {
            j.write_at(k as u64 * REC as u64, &record(k)).unwrap();
        }
        assert!(j.is_spilled());
        j.truncate_to(3 * REC as u64).unwrap();
        assert!(j.is_spilled());
        assert_eq!(j.len(), 3 * REC as u64);
        assert!(read(&j, 3 * REC as u64, REC).is_empty());
        j.truncate_to(0).unwrap();
        assert!(!j.is_spilled());
        assert_eq!(j.len(), 0);
        j.write_at(0, &record(7)).unwrap();
        assert!(!j.is_spilled());
        assert_eq!(read(&j, 0, REC), record(7));
    }

    #[test]
    fn a_journal_that_never_spills_keeps_everything_in_memory() {
        let j = SpillJournal::new(None);
        for k in 0..100u8 {
            j.write_at(k as u64 * REC as u64, &record(k)).unwrap();
        }
        assert!(!j.is_spilled());
        assert_eq!(read(&j, 99 * REC as u64, REC), record(99));
    }

    fn open_db() -> (tempfile::TempDir, crate::sync::Arc<Database>) {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("stmtjrnl.db");
        let io: crate::sync::Arc<dyn IO> = crate::sync::Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path.to_str().unwrap(),
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            crate::sync::Arc::new(SqliteDialect),
        )
        .unwrap();
        (dir, db)
    }

    fn int(conn: &crate::sync::Arc<crate::Connection>, sql: &str) -> i64 {
        let rows = conn.prepare(sql).unwrap().run_collect_rows().unwrap();
        rows[0][0]
            .as_int()
            .unwrap_or_else(|| panic!("{sql}: expected an integer, got {:?}", rows[0][0]))
    }

    /// A multi-row UPDATE in an explicit transaction whose LAST row fails a CHECK: the statement
    /// journal (one 3,000-byte row per page, so about 200 pages, far past 64 KiB) has spilled to its
    /// file, and the statement rollback must read every pre-image back from it. The transaction then
    /// goes on and commits.
    #[test]
    fn a_statement_rollback_restores_every_row_from_a_spilled_journal() {
        let rows = 200i64;
        let (_dir, db) = open_db();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER CHECK (n < 1000))")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        for id in 1..=rows {
            conn.execute(format!(
                "INSERT INTO t VALUES ({id}, 'o' || replace(hex(zeroblob(1500)), '0', 'a'), {id})"
            ))
            .unwrap();
        }
        conn.execute("COMMIT").unwrap();
        let sum_before = int(&conn, "SELECT sum(n) FROM t");
        let pages_before = crate::storage::page_cache::cache_work().subjournal_pages;
        let spills_before = super::super::page_cache::subjournal_spills_on_this_thread();

        conn.execute("BEGIN").unwrap();
        // n + 800 < 1000 holds for every id < 200 and fails for the last, id = 200.
        let failed = conn.execute(format!(
            "UPDATE t SET v = 'X' || substr(v, 2), n = n + 800 WHERE id <= {rows}"
        ));
        assert!(failed.is_err(), "the UPDATE whose last row breaks the CHECK succeeded");
        // Premise: the statement journal took the pre-images, and it spilled on this thread.
        let journalled = crate::storage::page_cache::cache_work().subjournal_pages - pages_before;
        assert!(journalled >= (rows - 1) as u64, "only {journalled} pages were journalled");
        assert!(
            super::super::page_cache::subjournal_spills_on_this_thread() > spills_before,
            "the statement journal never spilled: the rollback below did not read a file"
        );
        assert_eq!(int(&conn, "SELECT count(*) FROM t WHERE substr(v, 1, 1) = 'X'"), 0);
        assert_eq!(int(&conn, "SELECT sum(n) FROM t"), sum_before);
        conn.execute("UPDATE t SET n = n + 1 WHERE id = 1").unwrap();
        conn.execute("COMMIT").unwrap();

        drop(conn);
        let conn = db.connect().unwrap();
        assert_eq!(int(&conn, "SELECT count(*) FROM t WHERE substr(v, 1, 1) = 'o'"), rows);
        assert_eq!(int(&conn, "SELECT sum(n) FROM t"), sum_before + 1);
        assert_eq!(int(&conn, "SELECT n FROM t WHERE id = 1"), 2);
    }
}
