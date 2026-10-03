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
//! file), taken before it reads, truncates or discards anything. What that does and does not
//! exclude (review 3 F2 corrected an overclaim here):
//!
//! * A second store that finds branch state at open is refused at open, in this process or
//!   another.
//! * A store that opened BEFORE any branch file existed holds no lock yet. It is refused at its
//!   first fork instead: `create` refuses files that gained state after it opened.
//! * The lock fences branch LOGS, not trunk writers. A second `Database` over the same file
//!   (`Database::do_open`, which skips the registry) that never forks takes no lock, and its trunk
//!   writes retain no pre-image for the first one's branches. That is the multi-instance hazard
//!   Turso's registry exists to prevent; the branch store does not add a second fence for it.
//! * A fork(2) child shares the lock; its journal refuses every write (see `Journal::pid`).
//!
//! See `lock_exclusive` for why `flock` and for its blind spots.

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
///
/// 5 and 6 (r13-compose S-6): the composed store, with F7's epoch inheritance and its
/// `ReleaseOpen`/`Close` records (F7-durable's 3), and in the F7 SPLICE arm (its 4). F7-durable
/// alone wrote 3/4, no-force 3, the base 2 (which already carried F-FZ's `Checkpoint` record) and
/// the build before F7's merge 7, each with a different record set under the same magic, so the
/// composition takes two numbers none of its inputs wrote, and refuses every other, never
/// reinterprets it. A splice arm reads only its own (replay repeats every splice).
const FORMAT_VERSION: u32 = 5;
const SPLICE_FORMAT_VERSION: u32 = 6;

/// The format version a store in the splice arm (`true`) or not writes, and reads (its log and
/// snapshot headers, and a catalog's meta row).
pub(crate) fn format_version(splice: bool) -> u32 {
    if splice {
        SPLICE_FORMAT_VERSION
    } else {
        FORMAT_VERSION
    }
}
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
    /// The catalog of a catalog-mode store (`catalog.rs`), in place of `snap`.
    pub(crate) cat: PathBuf,
}

impl BranchFiles {
    /// Named from `base`, which the open computes ONCE for every sidecar, the WAL included
    /// (`database::sidecar_base`; review 4 C3 — this used to canonicalize on its own, so a
    /// symlinked open named its branch files and its WAL from two different paths). See there
    /// for the blind spots: hard links, bind mounts, delete-and-recreate, rename.
    pub(crate) fn for_db(base: &str) -> Self {
        let named = |suffix: &str| PathBuf::from(format!("{base}{suffix}"));
        Self {
            arena: named("-branch-arena"),
            log: named("-branch-log"),
            snap: named("-branch-snap"),
            cat: named("-branch-cat"),
        }
    }

    /// Whether these files hold durable branch state: a snapshot, or a log that is not EMPTY. An
    /// empty log with no snapshot holds nothing — `Journal::recover` says so — and must not make an
    /// open refuse: a creation that failed to lock leaves exactly that behind (the empty-log
    /// wedge). A log that cannot even be inspected counts as state: refuse rather than guess.
    pub(crate) fn exist(&self) -> bool {
        // An empty log holds nothing; a snapshot holds state whatever its size. A file whose
        // `stat` fails holds state unless the error says it cannot exist (review 6 item 5).
        let holds = |path: &Path, holds_nothing_up_to: Option<u64>| match std::fs::metadata(path) {
            Ok(meta) => holds_nothing_up_to.is_none_or(|n| meta.len() > n),
            Err(e) => !cannot_exist(&e),
        };
        holds(&self.snap, None) || holds(&self.cat, None) || holds(&self.log, Some(0))
    }

    fn snap_tmp(&self) -> PathBuf {
        let mut name = self.snap.clone().into_os_string();
        name.push(".tmp");
        PathBuf::from(name)
    }

    /// Where a catalog store's fuzzy checkpoint writes the log's kept suffix before renaming it
    /// over the log (r11-restart-r2, F-FZ).
    fn log_tmp(&self) -> PathBuf {
        let mut name = self.log.clone().into_os_string();
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
    /// The release of a branch that a connection still holds open: replay releases it and holds it,
    /// exactly as the live store does, until the matching `Close` (F7 durable port).
    ReleaseOpen {
        branch: u64,
    },
    /// The connection holding a released branch closed: replay collects the branch here, where the
    /// live store did, so a splice or a free happens at the same point in both (F7 durable port).
    Close {
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
    /// A catalog store's fuzzy checkpoint to `generation` captured the state as of here
    /// (r11-restart-r2, F-FZ; ARIES's begin-checkpoint record). If the catalog reaches `generation`
    /// while the log is still an older one, recovery replays only the records after this one.
    /// Replaying it changes nothing.
    Checkpoint {
        generation: u64,
    },
}

const TAG_FORK: u8 = 1;
const TAG_COMMIT: u8 = 2;
const TAG_TRUNK_RETAIN: u8 = 3;
const TAG_RELEASE: u8 = 4;
const TAG_LEASE: u8 = 5;
const TAG_CLOCK: u8 = 6;
const TAG_CHECKPOINT: u8 = 7;
/// r13-compose S-4: F7-durable's 7 and 8, renumbered past F-FZ's `Checkpoint` (7).
const TAG_RELEASE_OPEN: u8 = 8;
const TAG_CLOSE: u8 = 9;

/// Every record tag is distinct (r13-compose S-4): two equal tag constants would compile, with only an
/// unreachable-pattern warning, and decode one record as the other. Refused at compile time instead.
const _: () = {
    let tags = [
        TAG_FORK,
        TAG_COMMIT,
        TAG_TRUNK_RETAIN,
        TAG_RELEASE,
        TAG_LEASE,
        TAG_CLOCK,
        TAG_CHECKPOINT,
        TAG_RELEASE_OPEN,
        TAG_CLOSE,
    ];
    let mut i = 0;
    while i < tags.len() {
        let mut j = i + 1;
        while j < tags.len() {
            assert!(tags[i] != tags[j], "two branch log record tags are equal");
            j += 1;
        }
        i += 1;
    }
};

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
            Record::ReleaseOpen { branch } => {
                out.push(TAG_RELEASE_OPEN);
                put_u64(out, *branch);
            }
            Record::Close { branch } => {
                out.push(TAG_CLOSE);
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
            Record::Checkpoint { generation } => {
                out.push(TAG_CHECKPOINT);
                put_u64(out, *generation);
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
            TAG_RELEASE_OPEN => Record::ReleaseOpen { branch: r.u64()? },
            TAG_CLOSE => Record::Close { branch: r.u64()? },
            TAG_LEASE => Record::Lease {
                branch: r.u64()?,
                deadline_ms: r.u64()?,
                now_ms: r.u64()?,
            },
            TAG_CLOCK => Record::Clock { now_ms: r.u64()? },
            TAG_CHECKPOINT => Record::Checkpoint {
                generation: r.u64()?,
            },
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
    /// Released, and a connection still holds it open: kept whole until that connection's `Close`
    /// (encoded as `released` byte 2).
    pub(crate) held_open: bool,
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
            out.push(match (b.released, b.held_open) {
                (false, _) => 0,
                (true, false) => 1,
                (true, true) => 2,
            });
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
            let (released, held_open) = match r.u8()? {
                0 => (false, false),
                1 => (true, false),
                2 => (true, true),
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
                held_open,
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
    /// Bytes read from the snapshot and the log (r11-restart lane instrument).
    pub(crate) snap_bytes: u64,
    pub(crate) log_bytes: u64,
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
    /// The process that took the log's lock. A fork(2) child inherits the descriptor, and with it
    /// the SAME lock (flock belongs to the open file description), so the lock cannot keep the
    /// child out. This can: a journal whose process is not the one that locked it is fail-stopped,
    /// so no write path runs in the child (review 3 F1; SQLite's rule too — a connection must not
    /// be carried across fork()).
    pid: u32,
    /// See [`Journal::fail_next_write`].
    fail_next_write: bool,
    /// The format version this journal writes and was read at (`format_version`).
    format: u32,
}

impl Journal {
    /// Start a fresh durable store: a new log at generation 0, and no snapshot.
    ///
    /// Refused while another journal holds the log (review N1), and over files that hold state:
    /// the caller found nothing recoverable when it opened, so state here was written since by
    /// another store instance, and starting it over would destroy that store's branches.
    pub(crate) fn create(files: &BranchFiles, page_size: usize, sync: bool) -> Result<Journal> {
        let mut journal = Self::open_fresh(files, page_size, sync)?;
        journal.start(false)?;
        Ok(journal)
    }

    /// The first half of `create`: lock the log and check it holds no state. Writes nothing.
    pub(crate) fn open_fresh(files: &BranchFiles, page_size: usize, sync: bool) -> Result<Journal> {
        Self::open_fresh_with(files, page_size, sync, false)
    }

    /// `open_fresh`, with the `CreateLockFails` failpoint.
    pub(crate) fn open_fresh_with(
        files: &BranchFiles,
        page_size: usize,
        sync: bool,
        fail_lock: bool,
    ) -> Result<Journal> {
        // Lock before touching anything, so a refused create has truncated nothing.
        let mut file = open_rw(&files.log, false)?;
        if fail_lock {
            return Err(LimboError::LockingError(
                "failpoint: the branch log was created but could not be locked".to_string(),
            ));
        }
        lock_exclusive(&file, &files.log)?;
        let existing = read_all(&mut file)?;
        // A header of either arm's version is state (`parse_log_header` errs on the other one).
        if files.snap.exists()
            || files.cat.exists()
            || !matches!(parse_log_header(&existing, FORMAT_VERSION), Ok(None))
        {
            return Err(LimboError::LockingError(format!(
                "branch files next to {} gained state after this branch store opened: another \
                 store instance wrote them",
                files.log.display()
            )));
        }
        Ok(Journal {
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
            pid: std::process::id(),
            fail_next_write: false,
            format: FORMAT_VERSION,
        })
    }

    /// The format version this journal writes: set by a store in the splice arm before `start`.
    pub(crate) fn set_format(&mut self, format: u32) {
        self.format = format;
    }

    /// The second half of `create`: write the generation-0 header and make its directory entry
    /// durable. Any failure POISONS the journal and the caller keeps it (review 3 F3): its header
    /// may already be on disk, and a retry that dropped the journal would find that header and
    /// blame another store for it. `fail_after_header` is the `CreateFailsAfterHeader` failpoint.
    pub(crate) fn start(&mut self, fail_after_header: bool) -> Result<()> {
        let started = self.reset_log(0).and_then(|()| {
            if fail_after_header {
                return Err(LimboError::InternalError(
                    "failpoint: branch log creation stopped after its header".to_string(),
                ));
            }
            if self.sync {
                fsync_dir_of(&self.files.log)?;
            }
            Ok(())
        });
        if started.is_err() {
            self.poisoned = true;
        }
        started
    }

    /// Rewrite the header of a journal that holds NO record for another page size. Only for a
    /// journal kept from a first fork whose arena failed to open (review 3 F4): its header was
    /// written for that attempt's page size, and the retry's is the one the arena will use.
    pub(crate) fn restart(&mut self, page_size: usize) -> Result<()> {
        self.check_live()?;
        if self.len != LOG_HEADER_LEN as u64 || !self.pending.is_empty() {
            return Err(LimboError::InternalError(
                "branch log holds records; it cannot change page size".to_string(),
            ));
        }
        self.page_size = page_size as u32;
        if let Err(e) = self.reset_log(0) {
            self.poisoned = true;
            return Err(e);
        }
        Ok(())
    }

    /// `recover_as` at the default arm's format version: the tests' shorthand (the store opens
    /// through `recover_as`, in its own arm).
    #[cfg(test)]
    pub(crate) fn recover(files: &BranchFiles, sync: bool) -> Result<Option<Recovered>> {
        Self::recover_base(files, sync, None, FORMAT_VERSION)
    }

    /// Reopen an existing store, reading (and refusing anything but) the given format version.
    /// `Ok(None)` means the files hold no state at all (a crash while the log was being created,
    /// before its header was durable): the store starts empty.
    pub(crate) fn recover_as(
        files: &BranchFiles,
        sync: bool,
        format: u32,
    ) -> Result<Option<Recovered>> {
        Self::recover_base(files, sync, None, format)
    }

    /// `recover_as` for a catalog store: the catalog's meta row, `(page_size, generation)`, stands
    /// where the snapshot's header does, and no snapshot is read. `None`: the catalog has no meta
    /// row (a crash while the store was being created).
    ///
    /// A log of an OLDER generation than the catalog (r11-restart-r2, F-FZ): a checkpoint committed
    /// the catalog and the process stopped before the log was cut to what followed its capture.
    /// Only the records after that checkpoint's `Record::Checkpoint { generation }` are replayed
    /// (none if it never reached the disk: nothing written after it did either), and the log is
    /// rewritten to them under the catalog's generation before the open goes on.
    pub(crate) fn recover_catalog_as(
        files: &BranchFiles,
        sync: bool,
        base: Option<(u32, u64)>,
        format: u32,
    ) -> Result<Option<Recovered>> {
        Self::recover_base(files, sync, Some(base), format)
    }

    fn recover_base(
        files: &BranchFiles,
        sync: bool,
        catalog: Option<Option<(u32, u64)>>,
        format: u32,
    ) -> Result<Option<Recovered>> {
        // Lock before reading or discarding anything (review N1): the temp snapshot removed below
        // could be a live store's compaction in flight.
        let mut file = open_rw(&files.log, false)?;
        lock_exclusive(&file, &files.log)?;
        let snapshot = match catalog {
            None if files.snap.exists() => Some(read_snapshot(&files.snap, format)?),
            None => None,
            Some(base) => base.map(|(ps, g)| (ps, g, SnapshotState::default(), 0)),
        };
        // A stale temp snapshot is a compaction that never reached its rename: discard it. So is a
        // stale temp log: a fuzzy checkpoint's rewrite that never reached its rename (the log it
        // would have replaced is intact, and holds the checkpoint's marker).
        let tmp = files.snap_tmp();
        if tmp.exists() {
            std::fs::remove_file(&tmp).map_err(|e| io_error(e, "remove stale branch snapshot"))?;
        }
        let tmp = files.log_tmp();
        if tmp.exists() {
            std::fs::remove_file(&tmp).map_err(|e| io_error(e, "remove stale branch log rewrite"))?;
        }
        let catalog_store = catalog.is_some();
        let bytes = read_all(&mut file)?;
        let header = parse_log_header(&bytes, format)?;

        let (page_size, generation, snapshot_state, snapshot_len) = match (snapshot, header) {
            (None, None) => return Ok(None),
            (Some((ps, g, state, len)), _) => {
                (ps, g, catalog.is_none().then_some(state), len)
            }
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
            pid: std::process::id(),
            fail_next_write: false,
            format,
        };

        let mut records = Vec::new();
        // The end offset of each record in `records`, for the cut an older log needs (F-FZ).
        let mut ends: Vec<usize> = Vec::new();
        // Replayed: the log of the snapshot's or catalog's own generation; and in a catalog store,
        // an older one, cut at the checkpoint's marker below (F-FZ).
        let replayable = |log_gen: u64| log_gen == generation || (catalog_store && log_gen < generation);
        match header {
            Some((log_ps, log_gen)) if replayable(log_gen) => {
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
                    // A zero length is torn, never a record: every payload starts with its tag
                    // byte. It needs its own test, because crc32c of nothing is 0 — a zero-filled
                    // header (a file size made durable before its data) passes the CRC below and
                    // would be reported as corruption (review 4 O1).
                    //
                    // Torn ONLY if nothing valid follows (review 5 O1-1, the lead's decision): a
                    // zeroed hole with a whole frame after it means a later write survived and an
                    // earlier one did not — under macOS plain fsync, possibly an acknowledged one.
                    // That is Corrupt, loudly, and the log is left as it is.
                    if len == 0 {
                        if let Some(at) = first_whole_frame_after(&bytes, pos + 1) {
                            // No truncation is offered (review 7 item 1, the lead's decision): it
                            // is safe only if the records past the hole were never acknowledged,
                            // and that could be known only if the log marked where each
                            // acknowledged flush ends — it does not.
                            return Err(corrupt(&format!(
                                "branch log {log}: a zeroed region at byte {pos} is followed by a \
                                 whole record at byte {at}, so this is not a torn tail. The records \
                                 after the hole may have been acknowledged, and nothing in the log \
                                 tells acknowledged records from unacknowledged ones, so cutting \
                                 the log at the hole is not safe: it would drop trunk pre-image \
                                 records whose trunk commits are durable, and branches forked \
                                 before them would then read newer trunk pages without any error. \
                                 The safe remedy: keep a copy of all three branch files ({log}, \
                                 {arena}, {snap}) — a restore needs all three, and is safe only \
                                 while the trunk has not been written since they were moved aside \
                                 — then move all three aside. Every branch is lost; the trunk is \
                                 intact, because it never depends on branch files",
                                log = files.log.display(),
                                arena = files.arena.display(),
                                snap = files.snap.display()
                            )));
                        }
                        break;
                    }
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
                    ends.push(pos);
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
                if log_gen != generation {
                    // F-FZ crash state S1: the catalog holds everything up to its checkpoint's
                    // marker. Replay what follows it, and make that the log of the catalog's
                    // generation now, so appends and the next checkpoint see one log.
                    let marker = Record::Checkpoint { generation };
                    let cut = match records.iter().rposition(|r| *r == marker) {
                        Some(i) => {
                            let at = ends[i];
                            records.drain(..=i);
                            at
                        }
                        None => {
                            records.clear();
                            pos
                        }
                    };
                    journal.generation = log_gen;
                    journal.rewrite_from(cut as u64, generation)?;
                }
            }
            Some((_, log_gen)) if log_gen > generation => {
                return Err(corrupt("log generation is ahead of the snapshot"));
            }
            // An older generation in a snapshot store (a compaction crashed after its rename, before
            // the log reset) or a torn header: nothing in it is newer than the snapshot. (A whole header of
            // another format version never gets here: `parse_log_header` refused it.)
            _ => journal.reset_log(generation)?,
        }
        Ok(Some(Recovered {
            page_size: page_size as usize,
            snapshot: snapshot_state,
            records,
            journal,
            snap_bytes: snapshot_len,
            log_bytes: bytes.len() as u64,
        }))
    }

    /// Whether this journal may write nothing more: an I/O failure, a crash failpoint, or a fork.
    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned || self.forked()
    }

    fn forked(&self) -> bool {
        self.pid != std::process::id()
    }

    /// The process that opened this journal, when this process is a fork(2) child of it: the
    /// cause a refusal must name (review 4 C6), since a reopen cannot help while the child holds
    /// the inherited lock.
    pub(crate) fn fork_parent(&self) -> Option<u32> {
        self.forked().then_some(self.pid)
    }

    /// Fail the next log write as an I/O error would — the `StampFlushFails` failpoint. It fails
    /// INSIDE `flush`, so the poisoning a test then observes is `flush`'s own (review 4 C7).
    pub(crate) fn fail_next_write(&mut self) {
        self.fail_next_write = true;
    }

    pub(crate) fn page_size(&self) -> usize {
        self.page_size as usize
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// Bytes of the log holding whole records, header included (githost-shape instrument).
    pub(crate) fn log_len(&self) -> u64 {
        self.len
    }

    /// The log's logical end: the bytes in the file plus those still buffered. A fuzzy checkpoint
    /// captures the state as of this position (r11-restart-r2, F-FZ); it is a frame boundary.
    pub(crate) fn mark(&self) -> u64 {
        self.len + self.pending.len() as u64
    }

    /// A fuzzy catalog checkpoint at `generation` has committed, having captured the state as of
    /// the logical log position `from` (see [`Journal::mark`]; records buffered but not yet written
    /// count). Keep only the records after `from`, under a header of `generation`: the suffix is
    /// written to a temp file that this journal locks first, synced, and renamed over the log, so a
    /// crash leaves either the old log (whose `Record::Checkpoint` marks the cut) or the new one
    /// (F-FZ). Buffered records before `from` are dropped, their effects being in the catalog. A
    /// failure before the rename leaves the old log in use; one after it poisons the journal.
    ///
    /// Blind spot: POSIX rename over a file this process holds open. On Windows that rename fails,
    /// so a catalog store there cannot cut its log (nor reopen from crash state S1); catalog mode
    /// is unix-only as written.
    pub(crate) fn rewrite_from(&mut self, from: u64, generation: u64) -> Result<()> {
        self.check_live()?;
        let end = self.mark();
        if from < LOG_HEADER_LEN as u64 || from > end {
            return Err(LimboError::InternalError(format!(
                "branch log rewrite from byte {from}, outside the log's [{LOG_HEADER_LEN}, {end}]"
            )));
        }
        // The kept records already in the file.
        let file_from = from.min(self.len);
        let mut suffix = vec![0u8; (self.len - file_from) as usize];
        if !suffix.is_empty() {
            read_at(&self.file, &mut suffix, file_from)?;
        }
        let tmp = self.files.log_tmp();
        let written = (|| -> Result<File> {
            let f = open_rw(&tmp, true)?;
            // Locked before it can become the log: a second store that opens the log path after
            // the rename finds this lock, as it found the old one.
            lock_exclusive(&f, &tmp)?;
            // r13-compose S-3: the header carries THIS log's format (a splice-arm log stays 6).
            write_at(&f, &log_header(self.format, self.page_size, generation), 0)?;
            if !suffix.is_empty() {
                write_at(&f, &suffix, LOG_HEADER_LEN as u64)?;
            }
            if self.sync {
                fsync_file(&f)?;
            }
            Ok(f)
        })();
        let f = match written {
            Ok(f) => f,
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(e);
            }
        };
        if let Err(e) = std::fs::rename(&tmp, &self.files.log) {
            let _ = std::fs::remove_file(&tmp);
            return Err(io_error(e, "rename branch log rewrite"));
        }
        // From here the new file is the log.
        self.file = f;
        self.generation = generation;
        self.len = LOG_HEADER_LEN as u64 + suffix.len() as u64;
        self.snapshot_len = 0;
        let drop_pending = from.saturating_sub(file_from) as usize;
        if drop_pending > 0 {
            self.pending.drain(..drop_pending);
            self.pending_slots = slots_named(&self.pending);
        }
        if self.sync {
            if let Err(e) = fsync_dir_of(&self.files.log) {
                self.poisoned = true;
                return Err(e);
            }
        }
        Ok(())
    }

    /// The page size the next header or snapshot is written with. Only `restart_empty` changes it,
    /// immediately before the compaction that writes it.
    pub(crate) fn set_page_size(&mut self, page_size: usize) {
        self.page_size = page_size as u32;
    }

    pub(crate) fn poison(&mut self) {
        self.poisoned = true;
    }

    pub(crate) fn check_live(&self) -> Result<()> {
        if self.forked() {
            return Err(LimboError::InternalError(format!(
                "branch store is fail-stopped in this process: it was opened by process {}, and a \
                 branch store is not carried across fork(); open the database in this process",
                self.pid
            )));
        }
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
        // Taken first, so the failpoint is spent by exactly this call whatever it returns — it
        // cannot outlive the barrier that armed it (review 5 T-1).
        let fail_next_write = std::mem::take(&mut self.fail_next_write);
        self.check_live()?;
        if self.pending.is_empty() {
            return Ok(());
        }
        let written = if fail_next_write {
            Err(LimboError::InternalError(
                "failpoint: a branch log write failed".to_string(),
            ))
        } else {
            self.write_pending(arena)
        };
        match written {
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

    /// Twice the compaction threshold: while a fuzzy checkpoint is in flight, an operation that
    /// finds the log past this waits for its install (InnoDB's synchronous flush point beside its
    /// asynchronous one), so the log stays within twice the threshold plus the operations in
    /// flight (r11-restart-r2, F-FZ).
    pub(crate) fn past_hard_limit(&self) -> bool {
        self.len > 2 * COMPACT_MIN_LOG_BYTES.max(2 * self.snapshot_len)
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
        put_u32(&mut out, self.format);
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
        let header = log_header(self.format, self.page_size, generation);
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

/// A log header for `generation` at `page_size`.
fn log_header(format: u32, page_size: u32, generation: u64) -> Vec<u8> {
    // r13-compose §5 mutant (S-3): the header back to the build's default version.
    let format = if super::store::mutant("r13_log_header_fixed") {
        FORMAT_VERSION
    } else {
        format
    };
    let mut header = Vec::with_capacity(LOG_HEADER_LEN);
    header.extend_from_slice(LOG_MAGIC);
    put_u32(&mut header, format);
    put_u32(&mut header, page_size);
    put_u64(&mut header, generation);
    let crc = crc32c::crc32c(&header);
    put_u32(&mut header, crc);
    put_u32(&mut header, 0);
    debug_assert_eq!(header.len(), LOG_HEADER_LEN);
    header
}

/// The arena slots the records in `frames` (whole encoded frames, as `Journal::pending` holds
/// them) name: what `Journal::buffer` pushes to `pending_slots`, re-derived after a prefix of the
/// buffer was dropped (F-FZ). A frame that does not decode names nothing.
fn slots_named(frames: &[u8]) -> Vec<Slot> {
    let mut out = Vec::new();
    let mut pos = 0;
    while let Some(head) = frames.get(pos..pos + FRAME_HEADER_LEN) {
        let len = u32::from_le_bytes(head[0..4].try_into().expect("four bytes")) as usize;
        let start = pos + FRAME_HEADER_LEN;
        let Some(payload) = frames.get(start..start + len) else {
            break;
        };
        match Record::decode(payload) {
            Some(Record::TrunkRetain { slot, .. }) => out.push(slot),
            Some(Record::Commit { pages, .. }) => out.extend(pages.iter().map(|p| p.1)),
            _ => {}
        }
        pos = start + len;
    }
    out
}

/// Whether a failed `stat` of a branch sidecar means the file CANNOT be there (review 6 item 5,
/// the lead's decision), so it counts as absent:
/// * `NotFound`;
/// * a name too long for the filesystem: `ErrorKind::InvalidFilename`, which std maps from
///   ENAMETOOLONG and only that errno on unix (review 7 READ std `sys/io/error/unix.rs`; stable
///   since 1.87, the toolchain is 1.88), and from the long-name errors on Windows (recalled). A
///   database file name of 244–250 bytes fits NAME_MAX with its `-wal` and `-tshm` but not with
///   `-branch-snap`, which `exist` checks first (243–250 through a symlink, where the refusal also
///   checks `-branch-arena`), so those files cannot exist. The upper bound 250 holds only where
///   the multiprocess `-tshm` probe runs — a `host_shared_wal` build (64-bit unix or Windows), an
///   IO whose `supports_shared_wal_coordination` is true (unix, io_uring and iocp; the trait's
///   default is false), and a filesystem `path_allows_shared_wal_coordination` accepts: there a
///   251-byte name's `-tshm` is 256 bytes and the database does not open at all (review 8 F4).
///   Elsewhere a 251-byte name opens (its `-wal` is 255 bytes), and the range is 244–251 (243–251
///   through a symlink) (review 9 finding 7b). So is it where the probe runs, for a `do_open` with
///   a custom `wal_path` (the registry-aware opens refuse one; `do_open_async` takes its storage
///   ready-made and runs no probe): the probe names its coordination file from the WAL path it is
///   given (`coordination_path_for_wal_path`), not from the database name, so a short custom WAL
///   lets a 251-byte name pass it (review 10 F7).
/// * `Unsupported` WITHOUT an OS error: std's unsupported-platform error, which carries no errno.
///   wasm32-unknown-unknown's std `fs` is std's `unsupported` backend (READ: std `sys/fs/mod.rs`
///   selects it for every target that is neither unix, wasi, windows nor a listed OS), and that
///   error is `ErrorKind::Unsupported` (READ: `io::Error::UNSUPPORTED_PLATFORM`); that the
///   backend's `stat` returns it is recalled, not read. On unix `Unsupported` also comes from
///   ENOSYS or EOPNOTSUPP (a seccomp policy, a filesystem without getattr), which says nothing
///   about the file — so an OS error is excluded (review 7 item 5).
///
/// Every other error — permission denied, an I/O error, not-a-directory — cannot be told apart
/// from a file that holds state, so the caller refuses rather than guess.
pub(crate) fn cannot_exist(e: &std::io::Error) -> bool {
    match e.kind() {
        std::io::ErrorKind::NotFound | std::io::ErrorKind::InvalidFilename => true,
        std::io::ErrorKind::Unsupported => e.raw_os_error().is_none(),
        _ => false,
    }
}

/// The first offset at or after `from` where a whole, valid, decodable frame starts. Frames are
/// variable-length and the hole's length is unknown, so every offset is tried; an offset whose
/// length field is zero or runs past the end is rejected before any CRC is computed.
fn first_whole_frame_after(bytes: &[u8], from: usize) -> Option<usize> {
    let last = bytes.len().checked_sub(FRAME_HEADER_LEN)?;
    (from..=last).find(|&at| {
        let len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().unwrap());
        let start = at + FRAME_HEADER_LEN;
        len > 0
            && bytes.get(start..start.saturating_add(len)).is_some_and(|payload| {
                crc32c::crc32c(payload) == crc && Record::decode(payload).is_some()
            })
    })
}

/// `Some((page_size, generation))` if the header is whole and valid, `None` if it is torn (a crash
/// before it was durable). A whole header of ANOTHER format version is an error, not a torn one:
/// taking it for torn would start an empty store over it, or reset its log.
fn parse_log_header(bytes: &[u8], format: u32) -> Result<Option<(u32, u64)>> {
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
    if version != format {
        return Err(corrupt(&format!(
            "log format version {version}; this store reads version {format} (4 is the F7 \
             splice arm's: open with the same DatabaseOpts::with_branch_splice)"
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
/// verified here), which two stores in one process would share. And fork(2) without exec SHARES
/// the lock with the child (macOS flock(2) NOTES: descriptors duplicated "through dup(2) or
/// fork(2)" hold "multiple references to a single lock"): the child cannot write (`Journal::pid`
/// fail-stops it), but while it keeps the descriptor the parent cannot reopen the database — a
/// wedge inherent to flock and to OFD locks, refused loudly rather than raced. `exec` releases it
/// (std opens files close-on-exec; recalled, not verified here).
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
fn read_snapshot(path: &Path, format: u32) -> Result<(u32, u64, SnapshotState, u64)> {
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
    if u32::from_le_bytes(body[8..12].try_into().unwrap()) != format {
        return Err(corrupt("snapshot format version (4 is the F7 splice arm's)"));
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
            Record::ReleaseOpen { branch: 7 },
            Record::Close { branch: 7 },
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
    /// the log lock. (Review 3 F1: a fork(2) child shares that lock, so "cannot arise" was wrong
    /// until the pid check made the child's journal refuse every write.) Its guard, the length
    /// check, stays under test on unix: `a_journal_refuses_to_append_after_its_log_grew_under_it`
    /// and `…_shrank_under_it`.
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
    /// (Review 4 C8: it runs alone in a fresh process, so the fork duplicates no neighbour's lock.)
    /// Gate: `cfg(unix)` — see `fork_driver` for why no narrower gate is needed (Android included).
    #[cfg(unix)]
    #[test]
    fn a_forked_child_cannot_write_through_an_inherited_journal() {
        use crate::branch::fork_driver;
        let Some(sentinel) = fork_driver::alone(
            "branch::journal::tests::a_forked_child_cannot_write_through_an_inherited_journal",
        ) else {
            return;
        };
        let dir = tempfile::TempDir::new().unwrap();
        let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
        let mut arena = Arena::new(512);
        let mut journal = Journal::create(&files, 512, false).unwrap();
        journal.buffer(&Record::Fork { child: 1, parent: 0 }).unwrap();
        journal.flush(&mut arena).unwrap();
        // Buffered before the fork, so the child only has to flush it.
        journal.buffer(&Record::Fork { child: 2, parent: 0 }).unwrap();
        // SAFETY: the child runs only the flush (syscalls and small allocations, which the
        // platform allocators make fork-safe) and then `_exit`; it never returns into the harness.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            let code = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if journal.flush(&mut arena).is_ok() {
                    0
                } else {
                    1
                }
            }))
            .unwrap_or(2);
            // SAFETY: ends the child without running the harness or any destructor.
            unsafe { libc::_exit(code) };
        }
        assert_eq!(
            fork_driver::exit_code(pid),
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
        fork_driver::finished(&sentinel);
    }

    /// Review 4 O1. A crash can leave a file's new SIZE durable before its data, so the tail reads
    /// as zeros. A zeroed frame header says length 0 with crc 0 — and crc32c of nothing IS 0, so it
    /// passes the check. It is still a torn tail, and must be cut, not reported as corruption.
    #[test]
    fn a_zero_filled_tail_is_torn_not_corrupt() {
        let dir = tempfile::TempDir::new().unwrap();
        let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
        let mut arena = Arena::new(512);
        let mut journal = Journal::create(&files, 512, false).unwrap();
        journal.buffer(&Record::Fork { child: 1, parent: 0 }).unwrap();
        journal.flush(&mut arena).unwrap();
        drop(journal);
        let whole = std::fs::metadata(&files.log).unwrap().len();
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&files.log).unwrap();
            f.write_all(&[0u8; 64]).unwrap();
        }
        let recovered = Journal::recover(&files, false)
            .expect("a zero tail is a torn write, not corruption")
            .expect("state");
        assert_eq!(recovered.records, vec![Record::Fork { child: 1, parent: 0 }]);
        drop(recovered);
        assert_eq!(std::fs::metadata(&files.log).unwrap().len(), whole, "the zero tail was not cut");
    }

    /// Review 5 O1-1 (lead's decision): a zero length is torn only when nothing valid follows it.
    /// A zeroed hole with whole frames AFTER it means records written later survived and earlier
    /// ones did not — under macOS plain fsync, possibly acknowledged ones. That is Corrupt, loudly,
    /// and the log is left as it was for inspection.
    #[test]
    fn a_zeroed_hole_followed_by_whole_frames_is_corrupt() {
        let dir = tempfile::TempDir::new().unwrap();
        let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
        let mut arena = Arena::new(512);
        let mut journal = Journal::create(&files, 512, false).unwrap();
        journal.buffer(&Record::Fork { child: 1, parent: 0 }).unwrap();
        journal.flush(&mut arena).unwrap();
        drop(journal);
        let mut payload = Vec::new();
        Record::Release { branch: 1 }.encode(&mut payload);
        let mut frame = Vec::new();
        put_u32(&mut frame, payload.len() as u32);
        put_u32(&mut frame, crc32c::crc32c(&payload));
        frame.extend_from_slice(&payload);
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&files.log).unwrap();
            f.write_all(&[0u8; 25]).unwrap(); // a lost frame, read back as zeros
            f.write_all(&frame).unwrap(); // a later frame that survived
        }
        let before = std::fs::read(&files.log).unwrap();
        assert!(
            Journal::recover(&files, false).is_err(),
            "a zeroed hole before a whole frame was cut as a torn tail"
        );
        assert_eq!(std::fs::read(&files.log).unwrap(), before, "the refused recovery cut the log");
    }

    /// Review 6 item 6 (lead's decision): the Corrupt refusal of a zeroed hole names its remedy —
    /// which file, and where to cut it — not just byte offsets.
    #[test]
    fn a_zeroed_hole_refusal_names_its_remedy() {
        let dir = tempfile::TempDir::new().unwrap();
        let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
        let mut arena = Arena::new(512);
        let mut journal = Journal::create(&files, 512, false).unwrap();
        journal.buffer(&Record::Fork { child: 1, parent: 0 }).unwrap();
        journal.flush(&mut arena).unwrap();
        drop(journal);
        let hole_at = std::fs::metadata(&files.log).unwrap().len();
        let mut payload = Vec::new();
        Record::Release { branch: 1 }.encode(&mut payload);
        let mut frame = Vec::new();
        put_u32(&mut frame, payload.len() as u32);
        put_u32(&mut frame, crc32c::crc32c(&payload));
        frame.extend_from_slice(&payload);
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&files.log).unwrap();
            f.write_all(&[0u8; 25]).unwrap();
            f.write_all(&frame).unwrap();
        }
        let err = match Journal::recover(&files, false) {
            Ok(_) => panic!("a zeroed hole before a whole frame was cut as a torn tail"),
            Err(err) => err.to_string(),
        };
        // Review 7 item 1 (lead's decision) withdrew the remedy this test first pinned ("truncate it
        // to {pos}"): with acknowledged records after the hole, truncating drops trunk pre-images
        // whose commits are durable, and branches then read newer trunk pages silently. Nothing in
        // the log tells acknowledged records from unacknowledged ones. The safe remedy is to move
        // ALL THREE branch files aside, and a copy of all three is what a restore needs.
        // Review 8 F6 (lead's decision: cheap strength nits): no form of truncation, not only the
        // withdrawn phrase; and the hole's position as "byte {hole_at}", so a random temp-dir name
        // containing those digits cannot satisfy it.
        assert!(!err.contains("truncat"), "the refusal still recommends truncating: {err}");
        assert!(err.contains("aside"), "the refusal names no safe remedy: {err}");
        for file in [&files.log, &files.arena, &files.snap] {
            let file = file.display().to_string();
            assert!(err.contains(&file), "the refusal does not name {file}: {err}");
        }
        assert!(
            err.contains(&format!("byte {hole_at}")),
            "the refusal does not say where the hole is: {err}"
        );
        // Review 8 F3 (lead's decision): a restore of the three files is safe only while the trunk
        // has not been written since they were moved aside.
        assert!(
            err.contains("only while the trunk has not been written"),
            "the refusal gives no condition for a safe restore: {err}"
        );
    }

    /// The premise that makes a zero length torn: no record encodes to an empty payload, because
    /// every payload starts with its tag byte.
    #[test]
    fn no_record_encodes_to_an_empty_payload() {
        let records = [
            Record::Fork { child: 0, parent: 0 },
            Record::Commit {
                branch: 0,
                pages: vec![],
            },
            Record::TrunkRetain {
                page: 0,
                born: 0,
                died: 0,
                slot: 0,
                crc: 0,
            },
            Record::Release { branch: 0 },
            Record::ReleaseOpen { branch: 0 },
            Record::Close { branch: 0 },
            Record::Lease {
                branch: 0,
                deadline_ms: 0,
                now_ms: 0,
            },
            Record::Clock { now_ms: 0 },
            Record::Checkpoint { generation: 0 },
        ];
        for record in records {
            let mut payload = Vec::new();
            record.encode(&mut payload);
            assert!(!payload.is_empty(), "{record:?} encodes to an empty payload");
        }
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

    /// r11-ever amendment 15: the F7 splice arm writes version 4 and the default arm 3, and each
    /// reads only its own, because replay repeats every splice (a log written with splices and
    /// replayed without them would rebuild another tree). The log's check and the snapshot's are
    /// each made to fire alone: in the second half the log is of the READING arm's version.
    #[test]
    fn each_splice_arm_reads_only_its_own_format_version() {
        fn header(format: u32, generation: u64) -> Vec<u8> {
            let mut header = Vec::new();
            header.extend_from_slice(LOG_MAGIC);
            put_u32(&mut header, format);
            put_u32(&mut header, 512);
            put_u64(&mut header, generation);
            let crc = crc32c::crc32c(&header);
            put_u32(&mut header, crc);
            put_u32(&mut header, 0);
            header
        }
        fn snapshot(format: u32, generation: u64) -> Vec<u8> {
            let mut snap = Vec::new();
            snap.extend_from_slice(SNAP_MAGIC);
            put_u32(&mut snap, format);
            put_u32(&mut snap, 512);
            put_u64(&mut snap, generation);
            SnapshotState::default().encode(&mut snap);
            let crc = crc32c::crc32c(&snap);
            put_u32(&mut snap, crc);
            snap
        }
        assert_ne!(format_version(false), format_version(true));
        for written in [false, true] {
            let (own, other) = (format_version(written), format_version(!written));
            let dir = tempfile::TempDir::new().unwrap();
            let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());

            std::fs::write(&files.log, header(own, 0)).unwrap();
            assert!(
                Journal::recover_as(&files, false, other).is_err(),
                "splice={written}: the other arm read this arm's log"
            );
            let got = Journal::recover_as(&files, false, own).unwrap();
            assert!(got.is_some(), "splice={written}: its own arm found no state in its log");
            drop(got);

            std::fs::write(&files.snap, snapshot(own, 1)).unwrap();
            std::fs::write(&files.log, header(other, 1)).unwrap();
            assert!(
                Journal::recover_as(&files, false, other).is_err(),
                "splice={written}: the other arm read this arm's snapshot"
            );
            std::fs::write(&files.log, header(own, 1)).unwrap();
            let got = Journal::recover_as(&files, false, own).unwrap();
            assert!(
                got.is_some_and(|r| r.snapshot.is_some()),
                "splice={written}: its own arm did not read its snapshot"
            );
        }
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
                    held_open: false,
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
                    held_open: false,
                    lease_deadline_ms: 0,
                    current: vec![],
                    retained: vec![],
                },
                // Format 3: released while a connection holds it open.
                SnapBranch {
                    id: 12,
                    parent: 1,
                    fork_epoch: 1,
                    epoch: 2,
                    released: true,
                    held_open: true,
                    lease_deadline_ms: 0,
                    current: vec![(4, 20, 0, 21)],
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
