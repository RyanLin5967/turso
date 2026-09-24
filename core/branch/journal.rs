//! The durable form of the branch store: an operation log, a snapshot, and the file I/O they
//! share with the arena file. ⚠ UNBUILT when written (no-local-compute rule).
//!
//! # Files, next to the database file `<db>`
//!
//! * `<db>-branch-arena` — page slots; see [`super::arena`].
//! * `<db>-branch-log` — a 32-byte header, then frames `[payload_len u32][crc32c(payload) u32]
//!   [payload]`, all little-endian. Each payload is one [`Record`]. Replay stops at the first frame
//!   that is short or fails its CRC and truncates the file there: a torn append is exactly a crash
//!   before that record was durable (LevelDB's log reader; Redis `aof-load-truncated`).
//! * `<db>-branch-snap` — the whole live state (see [`SnapshotState`]) with a generation number,
//!   written to a temp file, fsynced and renamed into place. A log belongs to the snapshot with the
//!   same generation; a log with an OLDER generation is what a crash between the rename and the log
//!   reset leaves behind, and is ignored (LevelDB MANIFEST rollover, Redis RDB + AOF rewrite).
//!
//! # Why a log of OPERATIONS
//!
//! Records name what happened (a fork, a commit's page→slot pairs, a trunk pre-image, a release),
//! not the resulting state. Epochs, children and every branch-side retain/free decision are
//! re-derived by replaying them through the same store code that made them the first time, so the
//! log is O(change) per operation — the opposite of the whole-map rewrite ferrodb's D79 measured as
//! its persistence wall.
//!
//! # The ordering rule
//!
//! [`Journal::flush`] syncs the arena BEFORE it writes and syncs the buffered records, and records
//! are only ever written by a flush. A record on disk therefore never names a slot whose bytes are
//! not already durable, even if the OS writes the log page early.
//!
//! # One store per set of files
//!
//! A journal holds an exclusive `flock` on the log for its whole life (review N1; LevelDB's `LOCK`
//! file), taken before it reads, truncates or discards anything, so a second store over the same
//! files — in this process or another — is refused at open instead of interleaving its appends
//! with the first one's. See `lock_exclusive` for why `flock` and for its blind spots.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use super::arena::{Arena, Slot};
use crate::error::io_error;
use crate::{LimboError, Result};

const LOG_MAGIC: &[u8; 8] = b"TFBRLOG1";
const SNAP_MAGIC: &[u8; 8] = b"TFBRSNP1";
/// 2: `SnapBranch::lease_deadline_ms` became deadline + 1, with 0 meaning "no lease" (review R7).
/// Version 1 stored the deadline itself and could not tell a deadline of 0 from none, so a version
/// 1 snapshot read by this code would come back with every deadline 1 ms early. Another version is
/// refused, never reinterpreted.
const FORMAT_VERSION: u32 = 2;
const LOG_HEADER_LEN: usize = 32;
const FRAME_HEADER_LEN: usize = 8;
/// Compact once the log is larger than this and larger than twice the last snapshot, so the log is
/// never more than a constant factor of the live state and compaction work is amortised O(1).
const COMPACT_MIN_LOG_BYTES: u64 = 1 << 20;

/// The three files of a durable branch store.
#[derive(Debug, Clone)]
pub(crate) struct BranchFiles {
    pub(crate) arena: PathBuf,
    pub(crate) log: PathBuf,
    pub(crate) snap: PathBuf,
}

impl BranchFiles {
    pub(crate) fn for_db(db_path: &str) -> Self {
        Self {
            arena: PathBuf::from(format!("{db_path}-branch-arena")),
            log: PathBuf::from(format!("{db_path}-branch-log")),
            snap: PathBuf::from(format!("{db_path}-branch-snap")),
        }
    }

    /// Whether this database has ever had durable branch state.
    pub(crate) fn exist(&self) -> bool {
        self.log.exists() || self.snap.exists()
    }

    fn snap_tmp(&self) -> PathBuf {
        let mut name = self.snap.clone().into_os_string();
        name.push(".tmp");
        PathBuf::from(name)
    }
}

/// One entry of the operation log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Record {
    Fork {
        child: u64,
        parent: u64,
    },
    /// A branch commit: every dirty page and the fresh slot it was written to.
    Commit {
        branch: u64,
        pages: Vec<(u32, Slot, u32)>,
    },
    /// A trunk pre-image kept for the children forked in `[born, died)`.
    TrunkRetain {
        page: u32,
        born: u64,
        died: u64,
        slot: Slot,
        crc: u32,
    },
    Release {
        branch: u64,
    },
    /// A branch's lease deadline on the lease clock, and the clock when it was set.
    Lease {
        branch: u64,
        deadline_ms: u64,
        now_ms: u64,
    },
    /// The lease clock, stamped so a reopen resumes it instead of restarting it at zero.
    Clock {
        now_ms: u64,
    },
}

const TAG_FORK: u8 = 1;
const TAG_COMMIT: u8 = 2;
const TAG_TRUNK_RETAIN: u8 = 3;
const TAG_RELEASE: u8 = 4;
const TAG_LEASE: u8 = 5;
const TAG_CLOCK: u8 = 6;

impl Record {
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Record::Fork { child, parent } => {
                out.push(TAG_FORK);
                put_u64(out, *child);
                put_u64(out, *parent);
            }
            Record::Commit { branch, pages } => {
                out.push(TAG_COMMIT);
                put_u64(out, *branch);
                put_u32(out, pages.len() as u32);
                for &(page, slot, crc) in pages {
                    put_u32(out, page);
                    put_u32(out, slot);
                    put_u32(out, crc);
                }
            }
            Record::TrunkRetain {
                page,
                born,
                died,
                slot,
                crc,
            } => {
                out.push(TAG_TRUNK_RETAIN);
                put_u32(out, *page);
                put_u64(out, *born);
                put_u64(out, *died);
                put_u32(out, *slot);
                put_u32(out, *crc);
            }
            Record::Release { branch } => {
                out.push(TAG_RELEASE);
                put_u64(out, *branch);
            }
            Record::Lease {
                branch,
                deadline_ms,
                now_ms,
            } => {
                out.push(TAG_LEASE);
                put_u64(out, *branch);
                put_u64(out, *deadline_ms);
                put_u64(out, *now_ms);
            }
            Record::Clock { now_ms } => {
                out.push(TAG_CLOCK);
                put_u64(out, *now_ms);
            }
        }
    }

    fn decode(payload: &[u8]) -> Option<Record> {
        let mut r = Reader::new(payload);
        let record = match r.u8()? {
            TAG_FORK => Record::Fork {
                child: r.u64()?,
                parent: r.u64()?,
            },
            TAG_COMMIT => {
                let branch = r.u64()?;
                let n = r.u32()? as usize;
                let mut pages = Vec::with_capacity(n.min(1 << 16));
                for _ in 0..n {
                    pages.push((r.u32()?, r.u32()?, r.u32()?));
                }
                Record::Commit { branch, pages }
            }
            TAG_TRUNK_RETAIN => Record::TrunkRetain {
                page: r.u32()?,
                born: r.u64()?,
                died: r.u64()?,
                slot: r.u32()?,
                crc: r.u32()?,
            },
            TAG_RELEASE => Record::Release { branch: r.u64()? },
            TAG_LEASE => Record::Lease {
                branch: r.u64()?,
                deadline_ms: r.u64()?,
                now_ms: r.u64()?,
            },
            TAG_CLOCK => Record::Clock { now_ms: r.u64()? },
            _ => return None,
        };
        // Trailing bytes inside a frame whose CRC matched are a format error, not a torn tail.
        r.at_end().then_some(record)
    }
}

/// The whole live state, as a snapshot holds it. Epochs and retained sets are explicit here —
/// unlike the log, a snapshot is not replayed through the store's decisions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SnapshotState {
    pub(crate) next_id: u64,
    pub(crate) trunk_epoch: u64,
    /// The lease clock when the snapshot was taken.
    pub(crate) lease_now_ms: u64,
    /// (page, born, died, slot, crc)
    pub(crate) trunk_retained: Vec<(u32, u64, u64, Slot, u32)>,
    pub(crate) branches: Vec<SnapBranch>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SnapBranch {
    pub(crate) id: u64,
    pub(crate) parent: u64,
    pub(crate) fork_epoch: u64,
    pub(crate) epoch: u64,
    pub(crate) released: bool,
    /// The lease deadline on the lease clock PLUS ONE; 0 = no lease (a real deadline may be 0).
    pub(crate) lease_deadline_ms: u64,
    /// (page, slot, born, crc)
    pub(crate) current: Vec<(u32, Slot, u64, u32)>,
    /// (page, born, died, slot, crc)
    pub(crate) retained: Vec<(u32, u64, u64, Slot, u32)>,
}

impl SnapshotState {
    fn encode(&self, out: &mut Vec<u8>) {
        put_u64(out, self.next_id);
        put_u64(out, self.trunk_epoch);
        put_u64(out, self.lease_now_ms);
        put_retained(out, &self.trunk_retained);
        put_u64(out, self.branches.len() as u64);
        for b in &self.branches {
            put_u64(out, b.id);
            put_u64(out, b.parent);
            put_u64(out, b.fork_epoch);
            put_u64(out, b.epoch);
            out.push(b.released as u8);
            put_u64(out, b.lease_deadline_ms);
            put_u64(out, b.current.len() as u64);
            for &(page, slot, born, crc) in &b.current {
                put_u32(out, page);
                put_u32(out, slot);
                put_u64(out, born);
                put_u32(out, crc);
            }
            put_retained(out, &b.retained);
        }
    }

    fn decode(body: &[u8]) -> Option<SnapshotState> {
        let mut r = Reader::new(body);
        let next_id = r.u64()?;
        let trunk_epoch = r.u64()?;
        let lease_now_ms = r.u64()?;
        let trunk_retained = get_retained(&mut r)?;
        let n = r.u64()? as usize;
        let mut branches = Vec::with_capacity(n.min(1 << 20));
        for _ in 0..n {
            let id = r.u64()?;
            let parent = r.u64()?;
            let fork_epoch = r.u64()?;
            let epoch = r.u64()?;
            let released = match r.u8()? {
                0 => false,
                1 => true,
                _ => return None,
            };
            let lease_deadline_ms = r.u64()?;
            let m = r.u64()? as usize;
            let mut current = Vec::with_capacity(m.min(1 << 16));
            for _ in 0..m {
                current.push((r.u32()?, r.u32()?, r.u64()?, r.u32()?));
            }
            let retained = get_retained(&mut r)?;
            branches.push(SnapBranch {
                id,
                parent,
                fork_epoch,
                epoch,
                released,
                lease_deadline_ms,
                current,
                retained,
            });
        }
        r.at_end().then_some(SnapshotState {
            next_id,
            trunk_epoch,
            lease_now_ms,
            trunk_retained,
            branches,
        })
    }
}

fn put_retained(out: &mut Vec<u8>, retained: &[(u32, u64, u64, Slot, u32)]) {
    put_u64(out, retained.len() as u64);
    for &(page, born, died, slot, crc) in retained {
        put_u32(out, page);
        put_u64(out, born);
        put_u64(out, died);
        put_u32(out, slot);
        put_u32(out, crc);
    }
}

fn get_retained(r: &mut Reader<'_>) -> Option<Vec<(u32, u64, u64, Slot, u32)>> {
    let n = r.u64()? as usize;
    let mut v = Vec::with_capacity(n.min(1 << 16));
    for _ in 0..n {
        v.push((r.u32()?, r.u64()?, r.u64()?, r.u32()?, r.u32()?));
    }
    Some(v)
}

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let bytes = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(bytes)
    }

    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn at_end(&self) -> bool {
        self.pos == self.buf.len()
    }
}

fn corrupt(what: &str) -> LimboError {
    LimboError::Corrupt(format!("branch store: {what}"))
}

/// What reopening the files recovered: the snapshot (if any) and the log records written after
/// it, in order, plus the journal positioned to append after the last whole record.
pub(crate) struct Recovered {
    pub(crate) page_size: usize,
    pub(crate) snapshot: Option<SnapshotState>,
    pub(crate) records: Vec<Record>,
    pub(crate) journal: Journal,
}

pub(crate) struct Journal {
    file: File,
    files: BranchFiles,
    page_size: u32,
    generation: u64,
    /// Bytes of the log that hold whole, durable-or-written records (header included).
    len: u64,
    /// Encoded frames not yet written: records wait here until a flush has synced the arena.
    pending: Vec<u8>,
    /// Arena slots named by records still in `pending` (the failpoint reports them as orphans).
    pub(crate) pending_slots: Vec<Slot>,
    snapshot_len: u64,
    sync: bool,
    /// Set by an I/O failure, or a failpoint standing in for a crash. From then on nothing more is
    /// written: the next process recovers from what is on disk, and nothing this one does can make
    /// that worse. (Fail-stop on I/O error — the post-"fsyncgate" rule.)
    poisoned: bool,
}

impl Journal {
    /// Start a fresh durable store: a new log at generation 0, and no snapshot.
    ///
    /// Refused while another journal holds the log (review N1), and over files that hold state:
    /// the caller found nothing recoverable when it opened, so state here was written since by
    /// another store instance, and starting it over would destroy that store's branches.
    pub(crate) fn create(files: &BranchFiles, page_size: usize, sync: bool) -> Result<Journal> {
        Self::create_with(files, page_size, sync, false)
    }

    /// `create`, with the `CreateFailsAfterHeader` failpoint.
    pub(crate) fn create_with(
        files: &BranchFiles,
        page_size: usize,
        sync: bool,
        fail_after_header: bool,
    ) -> Result<Journal> {
        // Lock before touching anything, so a refused create has truncated nothing.
        let mut file = open_rw(&files.log, false)?;
        lock_exclusive(&file, &files.log)?;
        let existing = read_all(&mut file)?;
        if files.snap.exists() || !matches!(parse_log_header(&existing), Ok(None)) {
            return Err(LimboError::LockingError(format!(
                "branch files next to {} gained state after this branch store opened: another \
                 store instance wrote them",
                files.log.display()
            )));
        }
        let mut journal = Journal {
            file,
            files: files.clone(),
            page_size: page_size as u32,
            generation: 0,
            len: 0,
            pending: Vec::new(),
            pending_slots: Vec::new(),
            snapshot_len: 0,
            sync,
            poisoned: false,
        };
        journal.reset_log(0)?;
        if fail_after_header {
            return Err(LimboError::InternalError(
                "failpoint: branch log creation stopped after its header".to_string(),
            ));
        }
        if sync {
            fsync_dir_of(&files.log)?;
        }
        Ok(journal)
    }

    /// Reopen an existing store. `Ok(None)` means the files hold no state at all (a crash while
    /// the log was being created, before its header was durable): the store starts empty.
    pub(crate) fn recover(files: &BranchFiles, sync: bool) -> Result<Option<Recovered>> {
        // Lock before reading or discarding anything (review N1): the temp snapshot removed below
        // could be a live store's compaction in flight.
        let mut file = open_rw(&files.log, false)?;
        lock_exclusive(&file, &files.log)?;
        let snapshot = if files.snap.exists() {
            Some(read_snapshot(&files.snap)?)
        } else {
            None
        };
        // A stale temp snapshot is a compaction that never reached its rename: discard it.
        let tmp = files.snap_tmp();
        if tmp.exists() {
            std::fs::remove_file(&tmp).map_err(|e| io_error(e, "remove stale branch snapshot"))?;
        }
        let bytes = read_all(&mut file)?;
        let header = parse_log_header(&bytes)?;

        let (page_size, generation, snapshot_state, snapshot_len) = match (snapshot, header) {
            (None, None) => return Ok(None),
            (Some((ps, g, state, len)), _) => (ps, g, Some(state), len),
            (None, Some((ps, g))) => {
                if g != 0 {
                    return Err(corrupt("log generation is past 0 but there is no snapshot"));
                }
                (ps, 0, None, 0)
            }
        };

        let mut journal = Journal {
            file,
            files: files.clone(),
            page_size,
            generation,
            len: 0,
            pending: Vec::new(),
            pending_slots: Vec::new(),
            snapshot_len,
            sync,
            poisoned: false,
        };

        let mut records = Vec::new();
        match header {
            Some((log_ps, log_gen)) if log_gen == generation => {
                if log_ps != page_size {
                    return Err(corrupt("log and snapshot disagree on the page size"));
                }
                let mut pos = LOG_HEADER_LEN;
                loop {
                    let Some(frame) = bytes.get(pos..pos + FRAME_HEADER_LEN) else {
                        break;
                    };
                    let len = u32::from_le_bytes(frame[0..4].try_into().unwrap()) as usize;
                    let crc = u32::from_le_bytes(frame[4..8].try_into().unwrap());
                    let start = pos + FRAME_HEADER_LEN;
                    let Some(payload) = bytes.get(start..start + len) else {
                        break;
                    };
                    if crc32c::crc32c(payload) != crc {
                        break;
                    }
                    let record =
                        Record::decode(payload).ok_or_else(|| corrupt("undecodable log record"))?;
                    records.push(record);
                    pos = start + len;
                }
                journal.len = pos as u64;
                if (pos as u64) < bytes.len() as u64 {
                    // The torn tail: a record that was never durable. Cut it off so appends resume
                    // at a frame boundary.
                    journal
                        .file
                        .set_len(pos as u64)
                        .map_err(|e| io_error(e, "truncate branch log"))?;
                    if sync {
                        fsync_file(&journal.file)?;
                    }
                }
            }
            Some((_, log_gen)) if log_gen > generation => {
                return Err(corrupt("log generation is ahead of the snapshot"));
            }
            // An older generation (a compaction crashed after its rename, before the log reset)
            // or a torn header: nothing in it is newer than the snapshot. (A whole header of
            // another format version never gets here: `parse_log_header` refused it.)
            _ => journal.reset_log(generation)?,
        }
        Ok(Some(Recovered {
            page_size: page_size as usize,
            snapshot: snapshot_state,
            records,
            journal,
        }))
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub(crate) fn poison(&mut self) {
        self.poisoned = true;
    }

    fn check_live(&self) -> Result<()> {
        if self.poisoned {
            return Err(LimboError::InternalError(
                "branch store is fail-stopped after an I/O failure or a crash failpoint; reopen \
                 the database to recover it from disk"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Queue a record. It reaches the file only through [`Journal::flush`].
    pub(crate) fn buffer(&mut self, record: &Record) -> Result<()> {
        self.check_live()?;
        let mut payload = Vec::with_capacity(32);
        record.encode(&mut payload);
        put_u32(&mut self.pending, payload.len() as u32);
        put_u32(&mut self.pending, crc32c::crc32c(&payload));
        self.pending.extend_from_slice(&payload);
        match record {
            Record::TrunkRetain { slot, .. } => self.pending_slots.push(*slot),
            Record::Commit { pages, .. } => self.pending_slots.extend(pages.iter().map(|p| p.1)),
            _ => {}
        }
        Ok(())
    }

    /// Make every buffered record durable: arena first, then the records, then the log.
    pub(crate) fn flush(&mut self, arena: &mut Arena) -> Result<()> {
        self.check_live()?;
        if self.pending.is_empty() {
            return Ok(());
        }
        match self.write_pending(arena) {
            Ok(()) => {
                self.len += self.pending.len() as u64;
                self.pending.clear();
                self.pending_slots.clear();
                Ok(())
            }
            Err(e) => {
                self.poisoned = true;
                Err(e)
            }
        }
    }

    fn write_pending(&self, arena: &mut Arena) -> Result<()> {
        // Append only where this journal believes the log ends. A log that is longer than that was
        // written by someone else: writing at the stale offset would cut their records off at the
        // next recovery, so refuse — reading the file's state, not trusting the in-memory length
        // (review R8). On unix the log lock keeps a second JOURNAL out (N1); this check remains
        // the guard on other targets and against a writer that never asks for the lock.
        let on_disk = self
            .file
            .metadata()
            .map_err(|e| io_error(e, "stat branch log"))?
            .len();
        if on_disk != self.len {
            return Err(corrupt(
                "the branch log changed under this journal; another store instance wrote it",
            ));
        }
        if self.sync {
            arena.sync()?;
        }
        write_at(&self.file, &self.pending, self.len)?;
        if self.sync {
            fsync_file(&self.file)?;
        }
        Ok(())
    }

    pub(crate) fn wants_compaction(&self) -> bool {
        self.len > COMPACT_MIN_LOG_BYTES.max(2 * self.snapshot_len)
    }

    /// Replace the log with a snapshot of `state`. `fail_after_rename` is the crash failpoint.
    pub(crate) fn compact(
        &mut self,
        state: &SnapshotState,
        arena: &mut Arena,
        fail_after_rename: bool,
    ) -> Result<()> {
        self.check_live()?;
        // The snapshot names slots that buffered-but-unwritten records also name; they must be
        // durable before the snapshot is.
        if self.sync {
            arena.sync()?;
        }
        let generation = self.generation + 1;
        let mut out = Vec::with_capacity(64);
        out.extend_from_slice(SNAP_MAGIC);
        put_u32(&mut out, FORMAT_VERSION);
        put_u32(&mut out, self.page_size);
        put_u64(&mut out, generation);
        state.encode(&mut out);
        let crc = crc32c::crc32c(&out);
        put_u32(&mut out, crc);

        let tmp = self.files.snap_tmp();
        {
            let f = open_rw(&tmp, true)?;
            write_at(&f, &out, 0)?;
            if self.sync {
                fsync_file(&f)?;
            }
        }
        std::fs::rename(&tmp, &self.files.snap).map_err(|e| {
            self.poisoned = true;
            io_error(e, "rename branch snapshot")
        })?;
        // From here the snapshot is the truth; the old log is stale by generation.
        if self.sync {
            if let Err(e) = fsync_dir_of(&self.files.snap) {
                self.poisoned = true;
                return Err(e);
            }
        }
        if fail_after_rename {
            self.poisoned = true;
            return Err(LimboError::InternalError(
                "failpoint: branch compaction stopped after the snapshot rename".to_string(),
            ));
        }
        self.pending.clear();
        self.pending_slots.clear();
        self.snapshot_len = out.len() as u64;
        if let Err(e) = self.reset_log(generation) {
            self.poisoned = true;
            return Err(e);
        }
        Ok(())
    }

    /// Truncate the log to a bare header for `generation`.
    fn reset_log(&mut self, generation: u64) -> Result<()> {
        let mut header = Vec::with_capacity(LOG_HEADER_LEN);
        header.extend_from_slice(LOG_MAGIC);
        put_u32(&mut header, FORMAT_VERSION);
        put_u32(&mut header, self.page_size);
        put_u64(&mut header, generation);
        let crc = crc32c::crc32c(&header);
        put_u32(&mut header, crc);
        put_u32(&mut header, 0);
        debug_assert_eq!(header.len(), LOG_HEADER_LEN);
        self.file
            .set_len(0)
            .map_err(|e| io_error(e, "truncate branch log"))?;
        write_at(&self.file, &header, 0)?;
        if self.sync {
            fsync_file(&self.file)?;
        }
        self.generation = generation;
        self.len = LOG_HEADER_LEN as u64;
        Ok(())
    }

    pub(crate) fn log_path(&self) -> &Path {
        &self.files.log
    }
}

/// `Some((page_size, generation))` if the header is whole and valid, `None` if it is torn (a crash
/// before it was durable). A whole header of ANOTHER format version is an error, not a torn one:
/// taking it for torn would start an empty store over it, or reset its log.
fn parse_log_header(bytes: &[u8]) -> Result<Option<(u32, u64)>> {
    let Some(h) = bytes.get(..LOG_HEADER_LEN) else {
        return Ok(None);
    };
    if &h[0..8] != LOG_MAGIC {
        return Ok(None);
    }
    let field = |at: usize| u32::from_le_bytes(h[at..at + 4].try_into().unwrap());
    if crc32c::crc32c(&h[..24]) != field(24) {
        return Ok(None);
    }
    let version = field(8);
    if version != FORMAT_VERSION {
        return Err(corrupt(&format!(
            "log format version {version}; this build reads version {FORMAT_VERSION}"
        )));
    }
    let generation = u64::from_le_bytes(h[16..24].try_into().unwrap());
    Ok(Some((field(12), generation)))
}

/// Hold an exclusive advisory lock on the branch log for as long as `file` stays open (review N1):
/// one branch store per set of branch files, in this process or another. A second store is
/// refused at open instead of racing the first one's appends.
///
/// `flock(2)`, not `fcntl`: an `fcntl` lock belongs to the PROCESS, so two stores in one process
/// (`Database::do_open`, which skips the registry, or a reopen inside the registry's `Weak`
/// window) would both get it. An `flock` lock belongs to the open file description.
///
/// BLIND SPOTS: non-unix targets take no lock, and `write_pending`'s length check is their only
/// guard. The lock is advisory, so a writer that never asks is not stopped (the length check
/// again). On Linux over NFS, `flock` is emulated with `fcntl` locks (flock(2), recalled, not
/// verified here), which two stores in one process would share.
fn lock_exclusive(file: &File, path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: the descriptor is owned by `file` and open for the duration of the call.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let err = std::io::Error::last_os_error();
            return Err(LimboError::LockingError(
                if err.kind() == std::io::ErrorKind::WouldBlock {
                    format!(
                        "branch log {} is held by another branch store (a second Database over \
                         the same file, in this process or another); close that one first",
                        path.display()
                    )
                } else {
                    format!("cannot lock branch log {}: {err}", path.display())
                },
            ));
        }
    }
    #[cfg(not(unix))]
    let _ = (file, path);
    Ok(())
}

/// `(page_size, generation, state, file_len)`. A snapshot is only ever put in place by a rename
/// of a complete, synced file, so any defect in one is corruption, not a torn write.
fn read_snapshot(path: &Path) -> Result<(u32, u64, SnapshotState, u64)> {
    let mut file = File::open(path).map_err(|e| io_error(e, "open branch snapshot"))?;
    let bytes = read_all(&mut file)?;
    const HEAD: usize = 8 + 4 + 4 + 8;
    if bytes.len() < HEAD + 4 || &bytes[0..8] != SNAP_MAGIC {
        return Err(corrupt("snapshot header"));
    }
    let (body, trailer) = bytes.split_at(bytes.len() - 4);
    if crc32c::crc32c(body) != u32::from_le_bytes(trailer.try_into().unwrap()) {
        return Err(corrupt("snapshot checksum"));
    }
    if u32::from_le_bytes(body[8..12].try_into().unwrap()) != FORMAT_VERSION {
        return Err(corrupt("snapshot format version"));
    }
    let page_size = u32::from_le_bytes(body[12..16].try_into().unwrap());
    let generation = u64::from_le_bytes(body[16..24].try_into().unwrap());
    let state = SnapshotState::decode(&body[HEAD..]).ok_or_else(|| corrupt("snapshot body"))?;
    Ok((page_size, generation, state, bytes.len() as u64))
}

pub(crate) fn open_rw(path: &Path, truncate: bool) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(truncate)
        .open(path)
        .map_err(|e| io_error(e, "open branch file"))
}

fn read_all(file: &mut File) -> Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    file.seek(SeekFrom::Start(0))
        .map_err(|e| io_error(e, "seek branch file"))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|e| io_error(e, "read branch file"))?;
    Ok(bytes)
}

pub(crate) fn write_at(file: &File, bytes: &[u8], offset: u64) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(bytes, offset)
            .map_err(|e| io_error(e, "write branch file"))
    }
    #[cfg(not(unix))]
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = file;
        f.seek(SeekFrom::Start(offset))
            .map_err(|e| io_error(e, "seek branch file"))?;
        f.write_all(bytes)
            .map_err(|e| io_error(e, "write branch file"))
    }
}

pub(crate) fn read_at(file: &File, out: &mut [u8], offset: u64) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(out, offset)
            .map_err(|e| io_error(e, "read branch file"))
    }
    #[cfg(not(unix))]
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = file;
        f.seek(SeekFrom::Start(offset))
            .map_err(|e| io_error(e, "seek branch file"))?;
        f.read_exact(out)
            .map_err(|e| io_error(e, "read branch file"))
    }
}

/// `fsync(2)`, as Turso's own `FileSyncType::Fsync` — deliberately NOT `F_FULLFSYNC`, which std's
/// `sync_all` uses on Apple platforms: branch state gets the durability class the trunk gets under
/// default settings, no stronger and no weaker.
pub(crate) fn fsync_file(file: &File) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: the descriptor is owned by `file` and open for the duration of the call.
        if unsafe { libc::fsync(file.as_raw_fd()) } != 0 {
            return Err(io_error(std::io::Error::last_os_error(), "fsync branch file"));
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        file.sync_all()
            .map_err(|e| io_error(e, "fsync branch file"))
    }
}

/// Make a file's creation or rename durable: on POSIX that is an fsync of its directory.
pub(crate) fn fsync_dir_of(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let dir = match path.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir,
            _ => Path::new("."),
        };
        let d = File::open(dir).map_err(|e| io_error(e, "open branch directory"))?;
        fsync_file(&d)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_record_round_trips() {
        let records = [
            Record::Fork { child: 7, parent: 0 },
            Record::Commit {
                branch: 7,
                pages: vec![(1, 2, 3), (40, 41, 0xDEAD_BEEF)],
            },
            Record::Commit {
                branch: 8,
                pages: vec![],
            },
            Record::TrunkRetain {
                page: 9,
                born: 1,
                died: 4,
                slot: 12,
                crc: 99,
            },
            Record::Release { branch: 7 },
        ];
        for record in records {
            let mut payload = Vec::new();
            record.encode(&mut payload);
            assert_eq!(Record::decode(&payload), Some(record.clone()));
            // One byte short, and one byte extra, are both rejected.
            assert_eq!(Record::decode(&payload[..payload.len() - 1]), None);
            payload.push(0);
            assert_eq!(Record::decode(&payload), None);
        }
    }

    /// R8. A second journal over the same log (a store that outlived its database past a reopen)
    /// must not append at its own stale offset, which would cut the other's later records off at
    /// the next recovery: the stale writer is refused.
    /// ⚠ Non-unix only since review N1: on unix the second journal below is refused at recover by
    /// the log lock, so this schedule cannot arise there. Its guard, the length check, is kept
    /// under test on unix by `a_journal_refuses_to_append_after_its_log_grew_under_it`.
    #[cfg(not(unix))]
    #[test]
    fn a_stale_journal_does_not_write_over_a_newer_one() {
        let dir = tempfile::TempDir::new().unwrap();
        let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
        let mut arena = Arena::new(512);
        let mut stale = Journal::create(&files, 512, false).unwrap();
        stale.buffer(&Record::Fork { child: 1, parent: 0 }).unwrap();
        stale.flush(&mut arena).unwrap();

        let mut fresh = Journal::recover(&files, false).unwrap().expect("state");
        fresh.journal.buffer(&Record::Release { branch: 1 }).unwrap();
        fresh.journal.flush(&mut arena).unwrap();

        stale.buffer(&Record::Fork { child: 2, parent: 0 }).unwrap();
        assert!(stale.flush(&mut arena).is_err(), "a stale journal wrote over a newer one");
        let records = Journal::recover(&files, false).unwrap().expect("state").records;
        assert_eq!(
            records,
            vec![
                Record::Fork { child: 1, parent: 0 },
                Record::Release { branch: 1 }
            ]
        );
    }

    /// R8's length check where N1's lock leaves it reachable: a writer that never asks for the
    /// lock appends a whole frame. The journal must refuse to append over it.
    #[test]
    fn a_journal_refuses_to_append_after_its_log_grew_under_it() {
        let dir = tempfile::TempDir::new().unwrap();
        let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
        let mut arena = Arena::new(512);
        let mut journal = Journal::create(&files, 512, false).unwrap();
        journal.buffer(&Record::Fork { child: 1, parent: 0 }).unwrap();
        journal.flush(&mut arena).unwrap();

        let mut payload = Vec::new();
        Record::Release { branch: 1 }.encode(&mut payload);
        let mut frame = Vec::new();
        put_u32(&mut frame, payload.len() as u32);
        put_u32(&mut frame, crc32c::crc32c(&payload));
        frame.extend_from_slice(&payload);
        {
            use std::io::Write;
            let mut foreign = OpenOptions::new().append(true).open(&files.log).unwrap();
            foreign.write_all(&frame).unwrap();
        }

        journal.buffer(&Record::Fork { child: 2, parent: 0 }).unwrap();
        assert!(journal.flush(&mut arena).is_err(), "a journal appended over bytes it did not write");
        drop(journal);
        let records = Journal::recover(&files, false).unwrap().expect("state").records;
        assert_eq!(
            records,
            vec![
                Record::Fork { child: 1, parent: 0 },
                Record::Release { branch: 1 }
            ]
        );
    }

    /// Review 3 F1. `flock` belongs to the open file description, and fork(2) shares it: a forked
    /// child's copy of a live journal is under the parent's lock. It must refuse to write — or the
    /// two processes interleave appends under one lock, and a torn frame cuts every later record.
    #[cfg(unix)]
    #[test]
    fn a_forked_child_cannot_write_through_an_inherited_journal() {
        use std::os::fd::AsRawFd;
        let dir = tempfile::TempDir::new().unwrap();
        let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
        let mut arena = Arena::new(512);
        let mut journal = Journal::create(&files, 512, false).unwrap();
        journal.buffer(&Record::Fork { child: 1, parent: 0 }).unwrap();
        journal.flush(&mut arena).unwrap();
        // Buffered before the fork, so the child only has to flush it.
        journal.buffer(&Record::Fork { child: 2, parent: 0 }).unwrap();
        let keep = journal.file.as_raw_fd();
        // SAFETY: the child runs only the flush (syscalls and small allocations, which the
        // platform allocators make fork-safe) and then `_exit`; it never returns into the harness.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            // Drop the child's copies of every other descriptor first: another test's journal
            // lock must not outlive its owner through this child (the F1 wedge, on a neighbour).
            let max = unsafe { libc::getdtablesize() }.min(1 << 16);
            for fd in 3..max {
                if fd != keep {
                    unsafe { libc::close(fd) };
                }
            }
            let code = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if journal.flush(&mut arena).is_ok() {
                    0
                } else {
                    1
                }
            }))
            .unwrap_or(2);
            unsafe { libc::_exit(code) };
        }
        let mut status = 0;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            let reaped = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
            if reaped == pid {
                break;
            }
            assert_eq!(reaped, 0, "waitpid failed");
            if std::time::Instant::now() > deadline {
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                    libc::waitpid(pid, &mut status, 0);
                }
                panic!("the forked child did not exit");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(libc::WIFEXITED(status), "the child did not exit normally: status {status}");
        assert_eq!(
            libc::WEXITSTATUS(status),
            1,
            "a forked child wrote through its parent's journal (0), or panicked (2)"
        );
        // The parent is unaffected, and the log holds only what the parent wrote.
        journal.flush(&mut arena).unwrap();
        drop(journal);
        let records = Journal::recover(&files, false).unwrap().expect("state").records;
        assert_eq!(
            records,
            vec![
                Record::Fork { child: 1, parent: 0 },
                Record::Fork { child: 2, parent: 0 }
            ]
        );
    }

    /// Review 3 F6: the other half of R8's length check. A log another writer SHRANK must not be
    /// appended to at the stale offset either — that would leave a hole of zeros that stops replay.
    #[test]
    fn a_journal_refuses_to_append_after_its_log_shrank_under_it() {
        let dir = tempfile::TempDir::new().unwrap();
        let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
        let mut arena = Arena::new(512);
        let mut journal = Journal::create(&files, 512, false).unwrap();
        journal.buffer(&Record::Fork { child: 1, parent: 0 }).unwrap();
        journal.flush(&mut arena).unwrap();
        let after_first = std::fs::metadata(&files.log).unwrap().len();
        journal.buffer(&Record::Release { branch: 1 }).unwrap();
        journal.flush(&mut arena).unwrap();
        OpenOptions::new()
            .write(true)
            .open(&files.log)
            .unwrap()
            .set_len(after_first)
            .unwrap();

        journal.buffer(&Record::Fork { child: 2, parent: 0 }).unwrap();
        assert!(journal.flush(&mut arena).is_err(), "a journal appended past a log that shrank");
        drop(journal);
        let records = Journal::recover(&files, false).unwrap().expect("state").records;
        assert_eq!(records, vec![Record::Fork { child: 1, parent: 0 }]);
    }

    /// N1. Two journals must never be live on one log at once — not even for the window between a
    /// Database's last reference and its pager's drop, nor through `Database::do_open`, which skips
    /// the registry. The second is refused at open while the first lives, and admitted after.
    #[test]
    fn a_second_journal_on_one_log_is_refused_while_the_first_lives() {
        let dir = tempfile::TempDir::new().unwrap();
        let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
        let mut arena = Arena::new(512);
        let mut first = Journal::create(&files, 512, false).unwrap();
        first.buffer(&Record::Fork { child: 1, parent: 0 }).unwrap();
        first.flush(&mut arena).unwrap();

        assert!(Journal::recover(&files, false).is_err(), "a second journal recovered a live log");
        assert!(Journal::create(&files, 512, false).is_err(), "a second journal re-created a live log");
        // The refused create must not have truncated the live log on its way to being refused.
        first.buffer(&Record::Release { branch: 1 }).unwrap();
        first.flush(&mut arena).unwrap();
        drop(first);
        let records = Journal::recover(&files, false).unwrap().expect("state").records;
        assert_eq!(
            records,
            vec![
                Record::Fork { child: 1, parent: 0 },
                Record::Release { branch: 1 }
            ]
        );
    }

    /// R7 made a snapshot field mean deadline + 1. A log or snapshot of another format version
    /// must be REFUSED, not read with the wrong meaning — and not taken for a torn header, which
    /// would silently start an empty store over it.
    #[test]
    fn a_log_or_snapshot_of_another_format_version_is_refused() {
        let dir = tempfile::TempDir::new().unwrap();
        let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
        let other = FORMAT_VERSION.wrapping_sub(1);
        let mut header = Vec::new();
        header.extend_from_slice(LOG_MAGIC);
        put_u32(&mut header, other);
        put_u32(&mut header, 512);
        put_u64(&mut header, 0);
        let crc = crc32c::crc32c(&header);
        put_u32(&mut header, crc);
        put_u32(&mut header, 0);
        std::fs::write(&files.log, &header).unwrap();
        assert!(Journal::recover(&files, false).is_err(), "a log of another version was accepted");

        std::fs::remove_file(&files.log).unwrap();
        let mut snap = Vec::new();
        snap.extend_from_slice(SNAP_MAGIC);
        put_u32(&mut snap, other);
        put_u32(&mut snap, 512);
        put_u64(&mut snap, 1);
        SnapshotState::default().encode(&mut snap);
        let crc = crc32c::crc32c(&snap);
        put_u32(&mut snap, crc);
        std::fs::write(&files.snap, &snap).unwrap();
        assert!(Journal::recover(&files, false).is_err(), "a snapshot of another version was accepted");
    }

    #[test]
    fn lease_and_clock_records_round_trip() {
        for record in [
            Record::Lease {
                branch: 3,
                deadline_ms: 10_000,
                now_ms: 2_500,
            },
            Record::Clock { now_ms: u64::MAX - 1 },
        ] {
            let mut payload = Vec::new();
            record.encode(&mut payload);
            assert_eq!(Record::decode(&payload), Some(record.clone()));
            assert_eq!(Record::decode(&payload[..payload.len() - 1]), None);
        }
    }

    #[test]
    fn a_snapshot_state_round_trips() {
        let state = SnapshotState {
            next_id: 11,
            trunk_epoch: 5,
            lease_now_ms: 90_000,
            trunk_retained: vec![(3, 0, 2, 17, 1234)],
            branches: vec![
                SnapBranch {
                    id: 1,
                    parent: 0,
                    fork_epoch: 0,
                    epoch: 2,
                    released: false,
                    lease_deadline_ms: 120_000,
                    current: vec![(4, 5, 1, 6), (9, 10, 0, 11)],
                    retained: vec![(4, 0, 1, 3, 77)],
                },
                SnapBranch {
                    id: 10,
                    parent: 1,
                    fork_epoch: 1,
                    epoch: 0,
                    released: true,
                    lease_deadline_ms: 0,
                    current: vec![],
                    retained: vec![],
                },
            ],
        };
        let mut body = Vec::new();
        state.encode(&mut body);
        assert_eq!(SnapshotState::decode(&body), Some(state));
        assert_eq!(SnapshotState::decode(&body[..body.len() - 1]), None);
    }
}
