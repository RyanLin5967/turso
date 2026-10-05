//! The durable form of the branch store: an operation log, a snapshot, and the file I/O they
//! share with the arena file. ⚠ UNBUILT when written (no-local-compute rule).
//!
//! # Files, next to the database file `<db>`
//!
//! * `<db>-branch-arena` — page slots; see [`super::arena`].
//! * `<db>-branch-log` — a 40-byte header carrying the incarnation's nonce, then FLIGHTS: each one
//!   write of frames `[payload_len u32][crc32c(payload) u32][payload]` (each payload one
//!   [`Record`]) closed by a 17-byte end frame (its tag saying whether the flight was synced, its
//!   length and crc32c, both checksums seeded with the nonce), all little-endian. Replay keeps
//!   whole flights only: a flight cut short, torn or garbled anywhere was never acknowledged and is
//!   cut whole (LevelDB's log reader; ext4's journal commit block), unless a synced flight's end at
//!   or after the damage is followed by any whole frame, which proves the damaged write was
//!   acknowledged: then the store is refused. The last flight's arena slots are checked too.
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
//! A flight sends the arena's written slots to the device (a plain fsync) BEFORE it writes and
//! flushes its records, and records are only ever written by a flight; the log's flush drains the
//! device, slots included (ruling 85a032f01, on one device: `one_device`). A power cut before that
//! flush returns can keep the records and lose a slot, so recovery checks every slot the LAST
//! flight names and drops the flight if one fails (`last_flight_slots_hold`); an older flight's
//! slots were durable before the next flight was written.
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::arena::{Arena, Slot};
use super::SyncClass;
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
///
/// 7 and 8 (fastest-engine M1 item 4): 5 and 6 plus the `ForkNamed` record and a name in every
/// snapshot branch.
///
/// 9 and 10 (fastest-engine; r12-noforce's flight framing, 339c17b2a / 02c5ce7f0): 7 and 8 written
/// in flights, each ending in a `FlightEnd` frame, and read by the flight rule (see
/// `Journal::recover_base`). A 7 or 8 log has no end frames, and this rule would cut every record of
/// it as a torn flight, so it is refused, never reinterpreted.
///
/// 11 and 12 (fastest-engine, review 2 #1-3/#7/#8): a 40-byte log header carrying a random nonce
/// under its checksum — a new one for every incarnation of the log (each reset, cut and compaction),
/// so no flight of an older incarnation is ever taken for one of this — and a 17-byte end frame (a
/// frame-layer marker, no longer a record): a tag saying whether the flight was synced, the
/// flight's length, and its crc32c, both the end frame's own checksum and the flight's seeded with
/// the nonce. A 9 or 10 log ("written before the log nonce") is refused, never reinterpreted.
const FORMAT_VERSION: u32 = 11;
const SPLICE_FORMAT_VERSION: u32 = 12;

/// The format version a store in the splice arm (`true`) or not writes, and reads (its log and
/// snapshot headers, and a catalog's meta row).
pub(crate) fn format_version(splice: bool) -> u32 {
    if splice {
        SPLICE_FORMAT_VERSION
    } else {
        FORMAT_VERSION
    }
}
/// magic(8) format(4) page size(4) generation(8) nonce(4) crc32c of the 28 bytes before it(4),
/// then the raised class (`HEADER_RAISED_AT`, outside the checksum: rewritten in place) and 4
/// reserved bytes.
const LOG_HEADER_LEN: usize = 40;
const FRAME_HEADER_LEN: usize = 8;
/// The end frame's tags: a flight written in a class that does not sync, and one that does (or is
/// ordered ahead of a trunk commit's flush). No record tag takes either value.
const END_TAG: u8 = 0xE0;
const END_SYNCED_TAG: u8 = 0xE1;
/// tag(1) flight length(4) the flight's crc32c, seeded with the nonce(4).
const END_PAYLOAD_LEN: usize = 9;
/// An end frame: its frame header, whose checksum is seeded with the nonce too, and its payload.
const END_FRAME_LEN: usize = FRAME_HEADER_LEN + END_PAYLOAD_LEN;
/// Compact once the log is larger than this and larger than twice the last snapshot, so the log is
/// never more than a constant factor of the live state and compaction work is amortised O(1).
const COMPACT_MIN_LOG_BYTES: u64 = 1 << 20;

/// The compaction threshold: `COMPACT_MIN_LOG_BYTES`, or in test builds a smaller one a test sets
/// (`set_compact_threshold`) so a few operations reach it.
fn compact_min_log_bytes() -> u64 {
    #[cfg(test)]
    {
        let set = COMPACT_THRESHOLD.load(Ordering::Acquire);
        if set > 0 {
            return set;
        }
    }
    COMPACT_MIN_LOG_BYTES
}

#[cfg(test)]
static COMPACT_THRESHOLD: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Set the compaction threshold for this process's tests (0: the default). Tests that set it hold
/// their own file's serial lock and put it back.
#[cfg(test)]
pub(crate) fn set_compact_threshold(bytes: u64) {
    COMPACT_THRESHOLD.store(bytes, Ordering::Release);
}

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
    /// A NAMED server branch's fork (fastest-engine M1 item 4): detached, never leased, its name
    /// unique among unreleased branches. The name is in the record, so the fork and its name are
    /// durable together, and recovery rebuilds the name index from the log.
    ForkNamed {
        child: u64,
        parent: u64,
        name: String,
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
/// fastest-engine M1 item 4.
const TAG_FORK_NAMED: u8 = 10;

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
        TAG_FORK_NAMED,
        END_TAG,
        END_SYNCED_TAG,
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
            Record::ForkNamed {
                child,
                parent,
                name,
            } => {
                out.push(TAG_FORK_NAMED);
                put_u64(out, *child);
                put_u64(out, *parent);
                put_u32(out, name.len() as u32);
                out.extend_from_slice(name.as_bytes());
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
            TAG_FORK_NAMED => {
                let child = r.u64()?;
                let parent = r.u64()?;
                let len = r.u32()? as usize;
                let name = String::from_utf8(r.take(len)?.to_vec()).ok()?;
                Record::ForkNamed {
                    child,
                    parent,
                    name,
                }
            }
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
    /// A named server branch's name (fastest-engine M1 item 4); `None` for an unnamed one, and for
    /// a released one (a release frees its name).
    pub(crate) name: Option<String>,
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
            // A name is never empty, so length 0 is "no name".
            let name = b.name.as_deref().unwrap_or("");
            put_u32(out, name.len() as u32);
            out.extend_from_slice(name.as_bytes());
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
            let len = r.u32()? as usize;
            let name = match len {
                0 => None,
                _ => Some(String::from_utf8(r.take(len)?.to_vec()).ok()?),
            };
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
                name,
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
/// it, in order, plus the journal positioned to append after the last whole flight.
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
    sync: SyncClass,
    /// Set by an I/O failure, or a failpoint standing in for a crash. From then on nothing more is
    /// written: the next process recovers from what is on disk, and nothing this one does can make
    /// that worse. (Fail-stop on I/O error — the post-"fsyncgate" rule.) SHARED with the store's
    /// group (`Journal::share_fail_stop`): a flight that fails outside this journal fail-stops it in
    /// the same instant, so no path can write after a failure the journal has not heard of
    /// (fastest-engine review B-F1).
    poisoned: Arc<AtomicBool>,
    /// The process that took the log's lock. A fork(2) child inherits the descriptor, and with it
    /// the SAME lock (flock belongs to the open file description), so the lock cannot keep the
    /// child out. This can: a journal whose process is not the one that locked it is fail-stopped,
    /// so no write path runs in the child (review 3 F1; SQLite's rule too — a connection must not
    /// be carried across fork()).
    pid: u32,
    /// See [`Journal::fail_next_write`].
    fail_next_write: bool,
    /// See [`Journal::fail_next_take`].
    fail_next_take: bool,
    /// The format version this journal writes and was read at (`format_version`).
    format: u32,
    /// Frame bytes ever buffered by this journal: the log sequence number a group flight makes
    /// durable up to (fastest-engine M1 item 2, gc 389b474b4's `lsn`). Monotone across compactions
    /// and log rewrites, unlike `len`.
    lsn: u64,
    /// The strongest class a buffered record asked for (a trunk commit's pre-images under a
    /// stronger trunk class, `BranchStore::begin_trunk_commit`): the next flight syncs in it.
    pending_class: SyncClass,
    /// The strongest class any record of this store was ever made durable in: a record raised past
    /// `sync` stays that durable through every rewrite that replaces it (a compaction, a catalog
    /// checkpoint and its log cut), which therefore syncs in `rewrite_class` (fastest-engine review
    /// B-F3). Kept in the log header, so a restart keeps it too.
    raised: SyncClass,
    /// This incarnation of the log's nonce (format 11): in its header, and seeding every end
    /// frame's checksums, so recovery never takes an older incarnation's flight for one of this.
    nonce: u32,
    /// The header's raised-class field must be rewritten by the next write (`raised` grew).
    header_stale: bool,
    /// Every rewrite of the log file (a reset, a cut, a compaction) moves this: a cut prepared off
    /// the store mutex (`prepare_cut`) is installed only over the log it was prepared from.
    rewrites: u64,
    /// The log was renamed into place by a cut whose directory entry is not yet synced: the next
    /// flight syncs the directory before its own log sync, so nothing is acknowledged before the
    /// rename is durable (review 2 #5: no directory sync under the store mutex).
    dir_dirty: bool,
    /// After a failed checkpoint or compaction, no other is wanted until the log is past this
    /// length (review 2 #5: no retry storm, every operation starting one). 0 after a rewrite.
    compact_after: u64,
}

/// What a cut prepared off the store mutex copies (`Journal::cut_source`, read under it): the log's
/// bytes up to `upto`, which nothing writes while the cut is prepared (the group's `cutting`).
pub(crate) struct CutSource {
    log: File,
    upto: u64,
    rewrites: u64,
    format: u32,
    page_size: u32,
    raised: SyncClass,
    class: SyncClass,
    tmp: PathBuf,
    nonce: u32,
}

/// A cut prepared off the store mutex (`Journal::prepare_cut`): the new log, as a temp file holding
/// the header and the kept records `[from, upto)` of the old one as one flight, synced; locked.
pub(crate) struct CutPrep {
    file: File,
    from: u64,
    upto: u64,
    rewrites: u64,
    generation: u64,
    /// Bytes after the header.
    written: u64,
    /// The new log's nonce, and the old one's (the delta the install copies is framed in it).
    nonce: u32,
    old_nonce: u32,
}

impl Journal {
    /// Start a fresh durable store: a new log at generation 0, and no snapshot.
    ///
    /// Refused while another journal holds the log (review N1), and over files that hold state:
    /// the caller found nothing recoverable when it opened, so state here was written since by
    /// another store instance, and starting it over would destroy that store's branches.
    pub(crate) fn create(files: &BranchFiles, page_size: usize, sync: SyncClass) -> Result<Journal> {
        let mut journal = Self::open_fresh(files, page_size, sync)?;
        journal.start(false)?;
        Ok(journal)
    }

    /// The first half of `create`: lock the log and check it holds no state. Writes nothing.
    pub(crate) fn open_fresh(files: &BranchFiles, page_size: usize, sync: SyncClass) -> Result<Journal> {
        Self::open_fresh_with(files, page_size, sync, false)
    }

    /// `open_fresh`, with the `CreateLockFails` failpoint.
    pub(crate) fn open_fresh_with(
        files: &BranchFiles,
        page_size: usize,
        sync: SyncClass,
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
            poisoned: Arc::new(AtomicBool::new(false)),
            pid: std::process::id(),
            fail_next_write: false,
            fail_next_take: false,
            format: FORMAT_VERSION,
            lsn: 0,
            pending_class: SyncClass::Off,
            raised: SyncClass::Off,
            nonce: 0,
            header_stale: false,
            rewrites: 0,
            dir_dirty: false,
            compact_after: 0,
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
            if self.sync.syncs() {
                fsync_dir_of(&self.files.log, self.sync)?;
            }
            Ok(())
        });
        if started.is_err() {
            self.set_poisoned();
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
            self.set_poisoned();
            return Err(e);
        }
        Ok(())
    }

    /// `recover_as` at the default arm's format version: the tests' shorthand (the store opens
    /// through `recover_as`, in its own arm).
    #[cfg(test)]
    pub(crate) fn recover(files: &BranchFiles, sync: SyncClass) -> Result<Option<Recovered>> {
        Self::recover_base(files, sync, None, FORMAT_VERSION)
    }

    /// Reopen an existing store, reading (and refusing anything but) the given format version.
    /// `Ok(None)` means the files hold no state at all (a crash while the log was being created,
    /// before its header was durable): the store starts empty.
    pub(crate) fn recover_as(
        files: &BranchFiles,
        sync: SyncClass,
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
        sync: SyncClass,
        base: Option<(u32, u64)>,
        format: u32,
    ) -> Result<Option<Recovered>> {
        Self::recover_base(files, sync, Some(base), format)
    }

    fn recover_base(
        files: &BranchFiles,
        sync: SyncClass,
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
        // The strongest class records were ever made durable in survives a restart (review B-F3).
        let raised = if header.is_some() {
            header_raised(&bytes)
        } else {
            SyncClass::Off
        };

        let (page_size, generation, snapshot_state, snapshot_len) = match (snapshot, header) {
            (None, None) => return Ok(None),
            (Some((ps, g, state, len)), _) => {
                (ps, g, catalog.is_none().then_some(state), len)
            }
            (None, Some((ps, g, _))) => {
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
            poisoned: Arc::new(AtomicBool::new(false)),
            pid: std::process::id(),
            fail_next_write: false,
            fail_next_take: false,
            format,
            lsn: 0,
            pending_class: SyncClass::Off,
            raised,
            nonce: 0,
            header_stale: false,
            rewrites: 0,
            dir_dirty: false,
            compact_after: 0,
        };

        let mut records = Vec::new();
        // The end offset of each record in `records`, for the cut an older log needs (F-FZ).
        let mut ends: Vec<usize> = Vec::new();
        // Replayed: the log of the snapshot's or catalog's own generation; and in a catalog store,
        // an older one, cut at the checkpoint's marker below (F-FZ).
        let replayable = |log_gen: u64| log_gen == generation || (catalog_store && log_gen < generation);
        match header {
            Some((log_ps, log_gen, nonce)) if replayable(log_gen) => {
                if log_ps != page_size {
                    return Err(corrupt("log and snapshot disagree on the page size"));
                }
                journal.nonce = nonce;
                let mut pos = LOG_HEADER_LEN;
                // Where the last whole flight ends, and how many records precede it. The header is
                // a flight boundary: a log whose first flight is torn keeps no record.
                let mut whole = (LOG_HEADER_LEN, 0usize);
                // The last whole flight that holds records: where it starts, and its first record.
                let mut last_flight: Option<(usize, usize)> = None;
                // How the scan stopped: `None` at the clean end of the file, else what it met.
                let mut damage: Option<&str> = None;
                loop {
                    let Some(frame) = bytes.get(pos..pos + FRAME_HEADER_LEN) else {
                        if pos < bytes.len() {
                            damage = Some("a short frame");
                        }
                        break;
                    };
                    let len = u32::from_le_bytes(frame[0..4].try_into().unwrap()) as usize;
                    let crc = u32::from_le_bytes(frame[4..8].try_into().unwrap());
                    // A zero length is torn, never a record: every payload starts with its tag
                    // byte. It needs its own test, because crc32c of nothing is 0 — a zero-filled
                    // header (a file size made durable before its data) passes the CRC below and
                    // would be reported as corruption (review 4 O1).
                    if len == 0 {
                        damage = Some("a zeroed region");
                        break;
                    }
                    let start = pos + FRAME_HEADER_LEN;
                    let Some(payload) = bytes.get(start..start + len) else {
                        damage = Some("a short frame");
                        break;
                    };
                    // Mutants (test builds only, review 2 #11): `skip_frame_crc` trusts every
                    // frame's own checksum, `skip_flight_crc` every end frame's flight checksum,
                    // `skip_flight_start` every end frame's flight length.
                    let frame_ok = |seeded: bool| {
                        super::store::fe_mutant("skip_frame_crc")
                            || if seeded { scan_crc_seeded(nonce, payload) } else { scan_crc(payload) } == crc
                    };
                    if is_end(len, payload[0]) {
                        // An end frame of THIS incarnation (its checksum is seeded with the
                        // nonce), ending a flight that began where the last whole one ended and
                        // whose checksum covers every frame since. Anything else — a frame lost or
                        // replaced inside the flight, an end frame left from an older incarnation —
                        // is damage, as at any torn frame.
                        if !frame_ok(true) {
                            damage = Some("an end frame of another incarnation, or a damaged one");
                            break;
                        }
                        let flight_len = u32::from_le_bytes(payload[1..5].try_into().unwrap()) as usize;
                        let flight_crc = u32::from_le_bytes(payload[5..9].try_into().unwrap());
                        let starts_right =
                            pos - whole.0 == flight_len || super::store::fe_mutant("skip_flight_start");
                        let sums_right = super::store::fe_mutant("skip_flight_crc")
                            || bytes
                                .get(pos.saturating_sub(flight_len)..pos)
                                .is_some_and(|flight| scan_crc_seeded(nonce, flight) == flight_crc);
                        if !(starts_right && sums_right) {
                            damage = Some("a flight whose end frame does not match it");
                            break;
                        }
                        if records.len() > whole.1 {
                            last_flight = Some(whole);
                        }
                        pos = start + len;
                        whole = (pos, records.len());
                        continue;
                    }
                    if !frame_ok(false) {
                        damage = Some("a damaged frame");
                        break;
                    }
                    let record =
                        Record::decode(payload).ok_or_else(|| corrupt("undecodable log record"))?;
                    records.push(record);
                    pos = start + len;
                    ends.push(pos);
                }
                if let Some(what) = damage {
                    // Is the damage a torn flight — nothing acknowledged lost — or an acknowledged
                    // flight lost (possible under macOS plain fsync, or a device that reorders)? A
                    // flight is written only after the one before it was synced (one flight in the
                    // air at a time), so the damage was acknowledged exactly when a synced flight
                    // at or after it is followed by any whole frame (`synced_flight_over`; review 2
                    // #2). A flight written in a class that does not sync (D0) proves nothing, so
                    // under D0 every damage is a torn flight and cut. The flight the damage lies in
                    // was never acknowledged otherwise, whatever survived of it, and is cut below.
                    if let Some(at) = synced_flight_over(&bytes, pos, nonce) {
                        // No truncation is offered (review 7 item 1, the lead's decision): the
                        // flight was acknowledged.
                        let state = if catalog_store { &files.cat } else { &files.snap };
                        return Err(corrupt(&format!(
                            "branch log {log}: {what} at byte {pos} lies under a synced flight that \
                             ends at byte {at}, and a later frame follows it, so this is not a torn \
                             flight. The flight it lies in was acknowledged, and cutting the log \
                             there is not safe: it would drop trunk pre-image records whose trunk \
                             commits are durable, and branches forked before them would then read \
                             newer trunk pages without any error. The safe remedy: keep a copy of \
                             all three branch files ({log}, {arena}, {state}) — a restore needs \
                             all three, and is safe only while the trunk has not been written since \
                             they were moved aside — then move all three aside. Every branch is \
                             lost; the trunk is intact, because it never depends on branch files",
                            log = files.log.display(),
                            arena = files.arena.display(),
                            state = state.display()
                        )));
                    }
                }
                // The last flight's slots must hold what it says (ruling 85a032f01: the arena is
                // synced by plain fsync before the log's flush): one that does not was never
                // acknowledged, and goes like a torn one.
                // A last flight confirmed in the header (its sync returned) keeps its records whatever
                // its slots hold: a slot failing then is damage, refused when read, never a
                // silently older page.
                let confirmed = bytes
                    .get(HEADER_CONFIRM_AT as usize..HEADER_CONFIRM_AT as usize + 4)
                    .map(|f| u32::from_le_bytes(f.try_into().unwrap()));
                if let Some((start, first)) = last_flight.filter(|&(start, _)| start < whole.0) {
                    let last_crc = u32::from_le_bytes(bytes[whole.0 - 4..whole.0].try_into().unwrap());
                    if confirmed != Some(last_crc)
                        && !last_flight_slots_hold(files, page_size, &records[first..whole.1])
                    {
                        whole = (start, first);
                    }
                }
                // Only whole flights are kept: a flight is synced only after all of it is written,
                // so one cut short, torn or garbled anywhere was never acknowledged, and all of it
                // goes — not only the frames after the damage. Mutant M-h (`apply_torn_flight`,
                // PREREG v1 amendment 36; test builds only) keeps the whole records of a torn one.
                if whole.0 < pos && !super::store::fe_mutant("apply_torn_flight") {
                    records.truncate(whole.1);
                    ends.truncate(whole.1);
                    pos = whole.0;
                }
                if pos < bytes.len() {
                    tracing::warn!(
                        "branch log {}: a torn flight cut at byte {pos} ({} bytes)",
                        files.log.display(),
                        bytes.len() - pos
                    );
                }
                journal.len = pos as u64;
                // fastest-engine mutant `no_torn_tail_cut` (test builds only): the torn tail is
                // left in the file, so appends no longer resume at a flight boundary.
                if (pos as u64) < bytes.len() as u64
                    && !super::store::fe_mutant("no_torn_tail_cut")
                {
                    // The torn flight: never acknowledged. Cut it off so appends resume at a
                    // flight boundary.
                    set_file_len(&journal.file, pos as u64)?;
                }
                // What is kept is synced before anything follows it, cut or not: the last whole
                // flight may have been written and never synced by the process that died, and a
                // later flight's frames will prove it was.
                if sync.syncs() {
                    fsync_file(&journal.file, sync)?;
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
            Some((_, log_gen, _)) if log_gen > generation => {
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
        self.poisoned.load(Ordering::Acquire) || self.forked()
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

    /// Make the next `take_flight` fail as a failed descriptor duplication would (the
    /// `GroupFlightTakeFails` failpoint).
    pub(crate) fn fail_next_take(&mut self) {
        self.fail_next_take = true;
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
        let mut kept = vec![0u8; (self.len - file_from) as usize];
        if !kept.is_empty() {
            read_at(&self.file, &mut kept, file_from)?;
        }
        // Every kept record, raised ones included, stays as durable as it was (review B-F3).
        let class = self.rewrite_class();
        // Framed again for a new incarnation, every kept flight keeping its boundary (review 2
        // #3); the cut can fall inside a flight, whose kept part becomes a flight of its own.
        // Every flight is in the file whole here, so each new one is too once its sync returns.
        let nonce = fresh_nonce(self.nonce);
        let (suffix, last_crc) = reframe(&kept, self.nonce, nonce, class.syncs())?;
        let tmp = self.files.log_tmp();
        let written = (|| -> Result<File> {
            let f = open_rw(&tmp, true)?;
            // Locked before it can become the log: a second store that opens the log path after
            // the rename finds this lock, as it found the old one.
            lock_exclusive(&f, &tmp)?;
            // r13-compose S-3: the header carries THIS log's format (a splice-arm log stays 6).
            write_at(&f, &log_header(self.format, self.page_size, generation, nonce, self.raised), 0)?;
            if !suffix.is_empty() {
                write_at(&f, &suffix, LOG_HEADER_LEN as u64)?;
                // Synced with the rest below: the kept last flight is confirmed.
                write_at(&f, &last_crc.to_le_bytes(), HEADER_CONFIRM_AT)?;
            }
            if class.syncs() {
                fsync_file(&f, class)?;
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
        self.rewrites += 1;
        self.compact_after = 0;
        self.generation = generation;
        self.nonce = nonce;
        self.len = LOG_HEADER_LEN as u64 + suffix.len() as u64;
        self.header_stale = false;
        self.snapshot_len = 0;
        let drop_pending = from.saturating_sub(file_from) as usize;
        if drop_pending > 0 {
            self.pending.drain(..drop_pending);
            self.pending_slots = slots_named(&self.pending);
        }
        if class.syncs() {
            if let Err(e) = fsync_dir_of(&self.files.log, class) {
                self.set_poisoned();
                return Err(e);
            }
        }
        Ok(())
    }

    /// A cut's first phase, under the store mutex with no flight in the air and none to start (the
    /// group's `cutting`): what `prepare_cut` copies off the mutex. `None` when the records the cut
    /// keeps do not all lie in the file yet (some before `from` are still buffered): the install
    /// then cuts the whole way under the mutex (`rewrite_from`).
    pub(crate) fn cut_source(&self, from: u64) -> Result<Option<CutSource>> {
        self.check_live()?;
        if from < LOG_HEADER_LEN as u64 || from > self.len {
            return Ok(None);
        }
        let log = self.file.try_clone().map_err(|e| io_error(e, "dup branch log"))?;
        Ok(Some(CutSource {
            log,
            upto: self.len,
            rewrites: self.rewrites,
            format: self.format,
            page_size: self.page_size,
            raised: self.raised,
            class: self.rewrite_class(),
            tmp: self.files.log_tmp(),
            nonce: self.nonce,
        }))
    }

    /// A cut's second phase, holding NO lock (review 1 item 7, review 2 #5): the records in
    /// `[from, upto)` re-framed as one flight under a header of `generation`, written to the temp
    /// log, synced in the rewrite class, and locked before it can become the log.
    pub(crate) fn prepare_cut(src: CutSource, from: u64, generation: u64) -> Result<CutPrep> {
        let mut kept = vec![0u8; (src.upto - from) as usize];
        if !kept.is_empty() {
            read_at(&src.log, &mut kept, from)?;
        }
        let nonce = fresh_nonce(src.nonce);
        let (suffix, last_crc) = reframe(&kept, src.nonce, nonce, src.class.syncs())?;
        let written = (|| -> Result<File> {
            let f = open_rw(&src.tmp, true)?;
            lock_exclusive(&f, &src.tmp)?;
            write_at(&f, &log_header(src.format, src.page_size, generation, nonce, src.raised), 0)?;
            if !suffix.is_empty() {
                write_at(&f, &suffix, LOG_HEADER_LEN as u64)?;
                write_at(&f, &last_crc.to_le_bytes(), HEADER_CONFIRM_AT)?;
            }
            if src.class.syncs() {
                fsync_file(&f, src.class)?;
            }
            Ok(f)
        })();
        match written {
            Ok(file) => Ok(CutPrep {
                file,
                from,
                upto: src.upto,
                rewrites: src.rewrites,
                generation,
                written: suffix.len() as u64,
                nonce,
                old_nonce: src.nonce,
            }),
            Err(e) => {
                let _ = std::fs::remove_file(&src.tmp);
                Err(e)
            }
        }
    }

    /// A cut's last phase, under the store mutex: `rewrite_from(from, generation)` with what
    /// `prepare_cut` wrote. Only what reached the old log since (`[upto, len)`, written by a
    /// synchronous flush while the cut was prepared, which group flights never are) is copied and
    /// synced here; the rename's directory entry is synced by the next flight, before anything
    /// is acknowledged (`dir_dirty`). A prep that no longer matches the log — rewritten since, or
    /// for another cut — is dropped and the whole cut done here instead.
    pub(crate) fn finish_cut(&mut self, prep: CutPrep, from: u64, generation: u64) -> Result<()> {
        self.check_live()?;
        let usable = prep.rewrites == self.rewrites
            && prep.old_nonce == self.nonce
            && prep.from == from
            && prep.generation == generation
            && prep.upto <= self.len
            && !super::store::fe_mutant("cut_under_mutex");
        if !usable {
            drop(prep);
            let _ = std::fs::remove_file(self.files.log_tmp());
            return self.rewrite_from(from, generation);
        }
        let class = self.rewrite_class();
        let mut len = LOG_HEADER_LEN as u64 + prep.written;
        if self.len > prep.upto {
            let mut delta = vec![0u8; (self.len - prep.upto) as usize];
            read_at(&self.file, &mut delta, prep.upto)?;
            let (frames, last_crc) = reframe(&delta, self.nonce, prep.nonce, class.syncs())?;
            if !frames.is_empty() {
                write_at(&prep.file, &frames, len)?;
                write_at(&prep.file, &last_crc.to_le_bytes(), HEADER_CONFIRM_AT)?;
                if class.syncs() {
                    fsync_file(&prep.file, class)?;
                }
                len += frames.len() as u64;
            }
        }
        if let Err(e) = std::fs::rename(self.files.log_tmp(), &self.files.log) {
            let _ = std::fs::remove_file(self.files.log_tmp());
            return Err(io_error(e, "rename branch log rewrite"));
        }
        // From here the new file is the log.
        self.file = prep.file;
        self.rewrites += 1;
        self.compact_after = 0;
        self.generation = generation;
        self.nonce = prep.nonce;
        self.len = len;
        self.header_stale = false;
        self.snapshot_len = 0;
        self.dir_dirty = class.syncs();
        Ok(())
    }

    /// The page size the next header or snapshot is written with. Only `restart_empty` changes it,
    /// immediately before the compaction that writes it.
    pub(crate) fn set_page_size(&mut self, page_size: usize) {
        self.page_size = page_size as u32;
    }

    pub(crate) fn poison(&mut self) {
        self.set_poisoned();
    }

    fn set_poisoned(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    /// Make `flag` this journal's fail-stop flag (fastest-engine review B-F1): the store's group
    /// sets it when a flight fails outside this journal, and every write path here reads it. A
    /// journal already fail-stopped sets it first, so sharing never revives one.
    pub(crate) fn share_fail_stop(&mut self, flag: &Arc<AtomicBool>) {
        // fastest-engine mutant `split_fail_stop` (test builds only): the journal keeps its own.
        if super::store::fe_mutant("split_fail_stop") {
            return;
        }
        if self.poisoned.load(Ordering::Acquire) {
            flag.store(true, Ordering::Release);
        }
        self.poisoned = flag.clone();
    }

    /// The class every rewrite of the log's records syncs in: the store's own, or the strongest one
    /// any record was made durable in, if stronger (see `raised`).
    pub(crate) fn rewrite_class(&self) -> SyncClass {
        // fastest-engine mutant `rewrite_store_class` (test builds only): rewrites in the store's.
        if super::store::fe_mutant("rewrite_store_class") {
            return self.sync;
        }
        self.sync.max(self.raised)
    }

    /// A write in `class` is about to make records durable: remember a class stronger than any so
    /// far, and have the header say so (see `raised`).
    fn note_class(&mut self, class: SyncClass) {
        if class.syncs() && class > self.raised.max(self.sync) {
            self.raised = class;
            self.header_stale = true;
        }
    }

    /// The header's raised-class field, if it must be rewritten by the write being prepared.
    fn take_header_patch(&mut self) -> Option<[u8; 4]> {
        std::mem::take(&mut self.header_stale).then(|| (class_code(self.raised)).to_le_bytes())
    }

    pub(crate) fn check_live(&self) -> Result<()> {
        if self.forked() {
            return Err(LimboError::InternalError(format!(
                "branch store is fail-stopped in this process: it was opened by process {}, and a \
                 branch store is not carried across fork(); open the database in this process",
                self.pid
            )));
        }
        if self.poisoned.load(Ordering::Acquire) {
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
        self.lsn += (FRAME_HEADER_LEN + payload.len()) as u64;
        match record {
            Record::TrunkRetain { slot, .. } => self.pending_slots.push(*slot),
            Record::Commit { pages, .. } => self.pending_slots.extend(pages.iter().map(|p| p.1)),
            _ => {}
        }
        Ok(())
    }

    /// Make every buffered record durable as one flight: arena first, then the records and their end
    /// frame, then the log.
    pub(crate) fn flush(&mut self, arena: &mut Arena) -> Result<()> {
        self.flush_as(arena, self.sync)
    }

    /// `flush`, syncing in `class` instead of this journal's own: a trunk commit's pre-image barrier
    /// raises it to the class its WAL is synced in, when that is stronger (see [`SyncClass`]).
    fn flush_as(&mut self, arena: &mut Arena, class: SyncClass) -> Result<()> {
        // Taken first, so the failpoint is spent by exactly this call whatever it returns — it
        // cannot outlive the barrier that armed it (review 5 T-1).
        let fail_next_write = std::mem::take(&mut self.fail_next_write);
        self.check_live()?;
        if self.pending.is_empty() {
            return Ok(());
        }
        let class = class.max(self.pending_class);
        self.note_class(class);
        let header = self.take_header_patch();
        let dir = match self.take_dirty_dir(class) {
            Ok(dir) => dir,
            Err(e) => {
                self.set_poisoned();
                return Err(e);
            }
        };
        // One flight: the buffered frames and their end frame, in one write.
        let mut frames = Vec::with_capacity(self.pending.len() + END_FRAME_LEN);
        frames.extend_from_slice(&self.pending);
        frames.extend_from_slice(&end_frame(self.nonce, class.syncs(), &self.pending));
        let written = if fail_next_write {
            Err(LimboError::InternalError(
                "failpoint: a branch log write failed".to_string(),
            ))
        } else {
            self.write_frames(arena, class, &frames, header, dir.as_ref())
        };
        match written {
            Ok(()) => {
                self.len += frames.len() as u64;
                self.pending.clear();
                self.pending_slots.clear();
                self.pending_class = SyncClass::Off;
                Ok(())
            }
            Err(e) => {
                self.set_poisoned();
                Err(e)
            }
        }
    }

    fn write_frames(
        &self,
        arena: &mut Arena,
        class: SyncClass,
        frames: &[u8],
        header: Option<[u8; 4]>,
        dir: Option<&File>,
    ) -> Result<()> {
        // Append only where this journal believes the log ends. A log that is longer than that was
        // written by someone else: writing at the stale offset would cut their records off at the
        // next recovery, so refuse — reading the file's state, not trusting the in-memory length
        // (review R8). On unix the log lock keeps a second JOURNAL out (N1); this check remains
        // the guard on other targets and against a writer that never asks for the lock.
        let on_disk = file_len(&self.file)?;
        if on_disk != self.len {
            return Err(corrupt(
                "the branch log changed under this journal; another store instance wrote it",
            ));
        }
        // The slots before the log's sync, a plain fsync (ruling 85a032f01; see `Flight::write`).
        if class.syncs() && !super::store::fe_mutant("no_arena_sync") {
            arena.sync(SyncClass::Fsync)?;
        }
        if let Some(raised) = header {
            write_at(&self.file, &raised, HEADER_RAISED_AT)?;
        }
        write_at(&self.file, frames, self.len)?;
        if let Some(dir) = dir {
            fsync_file(dir, SyncClass::Fsync)?;
        }
        if class.syncs() {
            fsync_file(&self.file, class)?;
            if !super::store::fe_mutant("no_flight_confirm") {
                let crc = end_frame_crc(frames[frames.len() - END_FRAME_LEN..].try_into().unwrap());
                write_at(&self.file, &crc.to_le_bytes(), HEADER_CONFIRM_AT)?;
            }
        }
        Ok(())
    }

    /// The class this journal syncs in.
    pub(crate) fn sync_class(&self) -> SyncClass {
        self.sync
    }

    /// Frame bytes ever buffered (see the `lsn` field).
    pub(crate) fn lsn(&self) -> u64 {
        self.lsn
    }

    /// Frame bytes buffered and not yet taken by a flush or a flight.
    pub(crate) fn pending_len(&self) -> u64 {
        self.pending.len() as u64
    }

    /// The next flight syncs in at least `class` (see `pending_class`).
    pub(crate) fn raise_pending_class(&mut self, class: SyncClass) {
        self.pending_class = self.pending_class.max(class);
    }

    /// Take everything buffered as one [`Flight`], for a group flush written outside the store
    /// mutex (fastest-engine M1 item 2; gc 389b474b4's `take_flight`). The log region is reserved
    /// here, so the next flight goes after it; the caller guarantees no other flight is in the air
    /// (one at a time, `BranchStore`'s group), which is also what makes the on-disk length check
    /// exact. The arena descriptor comes along when slots were written since the last sync: those
    /// slots are named by frames in this flight (or a later one), and rule 1 wants them durable
    /// before the frames are. The flight syncs in the strongest of `class`, this journal's own and
    /// what the buffered records asked for. With nothing buffered, the flight is an UPGRADE when
    /// `upgrade` is set — a sync of the log alone in `class`, which an F_FULLFSYNC makes cover every
    /// write the device took before it — and otherwise empty.
    pub(crate) fn take_flight(
        &mut self,
        arena: &mut Arena,
        class: SyncClass,
        upgrade: bool,
    ) -> Result<Flight> {
        let fail = std::mem::take(&mut self.fail_next_write);
        self.check_live()?;
        let class = class.max(self.sync).max(self.pending_class);
        let end_lsn = self.lsn;
        if self.pending.is_empty() && !upgrade {
            return Ok(Flight {
                log: None,
                arena: None,
                bytes: Vec::new(),
                at: self.len,
                class,
                fail: false,
                end_lsn,
                header: None,
                ordered: false,
                dir: None,
                nonce: self.nonce,
            });
        }
        // A flight that cannot be taken fail-stops the journal (review B-F1): its operations are
        // applied in memory and will never be durable, so nothing may be written after them.
        let fail_take = std::mem::take(&mut self.fail_next_take);
        let taken = (|| -> Result<(File, Option<File>)> {
            if fail_take {
                return Err(LimboError::InternalError(
                    "failpoint: the flight's log descriptor could not be duplicated".to_string(),
                ));
            }
            let on_disk = file_len(&self.file)?;
            if on_disk != self.len {
                return Err(corrupt(
                    "the branch log changed under this journal; another store instance wrote it",
                ));
            }
            let log = self
                .file
                .try_clone()
                .map_err(|e| io_error(e, "dup branch log"))?;
            // A flight that syncs nothing (D0) leaves the arena's mark set, so a later flight in a
            // syncing class (an upgrade) still syncs the slots these frames name.
            let arena = if class.syncs() {
                arena.take_dirty_file()?
            } else {
                None
            };
            Ok((log, arena))
        })();
        let (log, arena) = match taken {
            Ok(taken) => taken,
            Err(e) => {
                self.set_poisoned();
                return Err(e);
            }
        };
        // A cut's rename is made durable by this flight, before its own log sync (`dir_dirty`).
        let dir = match self.take_dirty_dir(class) {
            Ok(dir) => dir,
            Err(e) => {
                self.set_poisoned();
                return Err(e);
            }
        };
        self.note_class(class);
        let header = self.take_header_patch();
        let mut bytes = std::mem::take(&mut self.pending);
        self.pending_slots.clear();
        self.pending_class = SyncClass::Off;
        let at = self.len;
        // The flight's end frame goes in the same write, built by `Flight::write` with no lock held
        // (review 2 #8); its room is reserved here. An upgrade has no frames and no end.
        if !bytes.is_empty() {
            bytes.reserve_exact(END_FRAME_LEN);
            self.len += END_FRAME_LEN as u64;
        }
        self.len += bytes.len() as u64;
        Ok(Flight {
            log: Some(log),
            arena,
            bytes,
            at,
            class,
            fail,
            end_lsn,
            header,
            ordered: false,
            dir,
            nonce: self.nonce,
        })
    }

    /// The log's directory, to sync before a log sync in `class` that follows a cut (`dir_dirty`).
    fn take_dirty_dir(&mut self, class: SyncClass) -> Result<Option<File>> {
        if !self.dir_dirty || !class.syncs() {
            return Ok(None);
        }
        let dir = match self.files.log.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir,
            _ => Path::new("."),
        };
        let d = File::open(dir).map_err(|e| io_error(e, "open branch directory"))?;
        self.dir_dirty = false;
        Ok(Some(d))
    }

    pub(crate) fn wants_compaction(&self) -> bool {
        // fastest-engine mutant `no_compaction_backoff` (test builds only).
        let backed_off = self.len <= self.compact_after && !super::store::fe_mutant("no_compaction_backoff");
        self.len > compact_min_log_bytes().max(2 * self.snapshot_len) && !backed_off
    }

    /// A checkpoint or compaction failed: the next is wanted only once another threshold's worth of
    /// log has been written (`compact_after`).
    pub(crate) fn defer_compaction(&mut self) {
        self.compact_after = self.len + compact_min_log_bytes();
    }

    /// Twice the compaction threshold: while a fuzzy checkpoint is in flight, an operation that
    /// finds the log past this waits for its install (InnoDB's synchronous flush point beside its
    /// asynchronous one), so the log stays within twice the threshold plus the operations in
    /// flight (r11-restart-r2, F-FZ).
    pub(crate) fn past_hard_limit(&self) -> bool {
        self.len > 2 * compact_min_log_bytes().max(2 * self.snapshot_len)
    }

    /// Replace the log with a snapshot of `state`. `fail_after_rename` is the crash failpoint.
    pub(crate) fn compact(
        &mut self,
        state: &SnapshotState,
        arena: &mut Arena,
        fail_after_rename: bool,
    ) -> Result<()> {
        self.check_live()?;
        // Every record the snapshot replaces stays as durable as it was, raised ones included
        // (review B-F3).
        let class = self.rewrite_class();
        // The snapshot names slots that buffered-but-unwritten records also name; they reach the
        // device before it, and its flush below makes them durable with it (ruling 85a032f01).
        if class.syncs() {
            arena.sync(SyncClass::Fsync)?;
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
            if class.syncs() {
                fsync_file(&f, class)?;
            }
        }
        std::fs::rename(&tmp, &self.files.snap).map_err(|e| {
            self.set_poisoned();
            io_error(e, "rename branch snapshot")
        })?;
        super::store::kill_point("compact.renamed");
        // From here the snapshot is the truth; the old log is stale by generation.
        if class.syncs() {
            if let Err(e) = fsync_dir_of(&self.files.snap, class) {
                self.set_poisoned();
                return Err(e);
            }
        }
        if fail_after_rename {
            self.set_poisoned();
            return Err(LimboError::InternalError(
                "failpoint: branch compaction stopped after the snapshot rename".to_string(),
            ));
        }
        self.pending.clear();
        self.pending_slots.clear();
        self.pending_class = SyncClass::Off;
        self.snapshot_len = out.len() as u64;
        if let Err(e) = self.reset_log(generation) {
            self.set_poisoned();
            return Err(e);
        }
        Ok(())
    }

    /// Truncate the log to a bare header for `generation`.
    ///
    /// A new incarnation, written whole beside the old log and renamed over it, then the directory
    /// synced (review 2 #1): a truncation in place could reach the disk after the new header, or
    /// never, and leave the old incarnation's flights behind it. Mutant `reset_in_place` (test
    /// builds only): the old in-place truncation. Blind spot: off unix (no rename over an open
    /// file) the reset is in place; the nonce still keeps the old flights out.
    fn reset_log(&mut self, generation: u64) -> Result<()> {
        let class = self.rewrite_class();
        let nonce = fresh_nonce(self.nonce);
        let header = log_header(self.format, self.page_size, generation, nonce, self.raised);
        self.rewrites += 1;
        self.compact_after = 0;
        if cfg!(unix) && !super::store::fe_mutant("reset_in_place") {
            let tmp = self.files.log_tmp();
            let written = (|| -> Result<File> {
                let f = open_rw(&tmp, true)?;
                lock_exclusive(&f, &tmp)?;
                write_at(&f, &header, 0)?;
                if class.syncs() {
                    fsync_file(&f, class)?;
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
                return Err(io_error(e, "rename branch log reset"));
            }
            self.file = f;
            if class.syncs() {
                fsync_dir_of(&self.files.log, class)?;
            }
        } else {
            set_file_len(&self.file, 0)?;
            write_at(&self.file, &header, 0)?;
            if class.syncs() {
                fsync_file(&self.file, class)?;
            }
        }
        self.generation = generation;
        self.nonce = nonce;
        self.len = LOG_HEADER_LEN as u64;
        self.header_stale = false;
        self.dir_dirty = false;
        Ok(())
    }

    pub(crate) fn log_path(&self) -> &Path {
        &self.files.log
    }

    /// The device the log's open file lives on (`fstat`'s `st_dev`); `None` where it cannot be read
    /// or off unix.
    pub(crate) fn device(&self) -> Option<u64> {
        file_device(&self.file)
    }
}

/// Where the header keeps the strongest class any record was made durable in (`Journal::raised`):
/// outside the checksum, so a flight that raises the class rewrites them in place and syncs them
/// with its own frames.
const HEADER_RAISED_AT: u64 = 32;
/// The nonce, and the header checksum after it (over every byte before it).
const HEADER_NONCE_AT: usize = 24;
const HEADER_CRC_AT: usize = 28;
/// The checksum of the last flight whose sync RETURNED, written (not synced) right after it: its
/// presence proves the flight was acknowledged, so a slot of it failing its CRC at recovery is
/// damage, not a slot that never reached the disk (`last_flight_slots_hold`). Outside the header
/// checksum; 0 when no flight is confirmed.
const HEADER_CONFIRM_AT: u64 = 36;

fn class_code(class: SyncClass) -> u32 {
    match class {
        SyncClass::Off => 0,
        SyncClass::Fsync => 1,
        SyncClass::FullFsync => 2,
    }
}

/// The header's raised class. Outside the checksum, so an unknown value reads as the strongest:
/// a rewrite in too strong a class costs time, one in too weak a class can lose a record.
fn header_raised(bytes: &[u8]) -> SyncClass {
    let Some(field) = bytes.get(HEADER_RAISED_AT as usize..HEADER_RAISED_AT as usize + 4) else {
        return SyncClass::Off;
    };
    match u32::from_le_bytes(field.try_into().unwrap()) {
        0 => SyncClass::Off,
        1 => SyncClass::Fsync,
        _ => SyncClass::FullFsync,
    }
}

/// A log header for `generation` at `page_size`, of the incarnation `nonce`, saying records were made
/// durable in `raised`.
fn log_header(format: u32, page_size: u32, generation: u64, nonce: u32, raised: SyncClass) -> Vec<u8> {
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
    put_u32(&mut header, nonce);
    debug_assert_eq!(header.len(), HEADER_CRC_AT);
    let crc = crc32c::crc32c(&header);
    put_u32(&mut header, crc);
    put_u32(&mut header, class_code(raised));
    put_u32(&mut header, 0);
    debug_assert_eq!(header.len(), LOG_HEADER_LEN);
    header
}

/// A nonce for a new incarnation of the log: random, never 0, never `old`.
fn fresh_nonce(old: u32) -> u32 {
    use std::hash::{BuildHasher, Hasher};
    static DRAWN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    loop {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u64(DRAWN.fetch_add(1, Ordering::Relaxed));
        h.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos()),
        );
        let nonce = h.finish() as u32;
        if nonce != 0 && nonce != old {
            return nonce;
        }
    }
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

/// One group flight (fastest-engine M1 item 2; gc 389b474b4): frames taken from a journal's buffer
/// (their end frame is built by `write`), the log offset they go to, the arena descriptor to sync first, and the class to sync in. Written
/// by [`Flight::write`] with no lock held; the journal already counts the region as written, so a
/// failed write must fail-stop it.
pub(crate) struct Flight {
    log: Option<File>,
    arena: Option<File>,
    bytes: Vec<u8>,
    at: u64,
    pub(crate) class: SyncClass,
    fail: bool,
    /// The journal's `lsn` at the end of these frames: what the flight makes durable.
    pub(crate) end_lsn: u64,
    /// The header's raised-class field, when this flight is the first in a stronger class.
    header: Option<[u8; 4]>,
    /// ORDERED, not synced (lead review 1 item 6): the log is barriered, not flushed, and a trunk
    /// commit's own F_FULLFSYNC of its WAL, on the same device, makes the flight durable.
    ordered: bool,
    /// The log's directory after a cut renamed the log into place (`Journal::dir_dirty`): synced
    /// before the log, so the flight's own flush makes the rename durable too.
    dir: Option<File>,
    /// The log incarnation's nonce, for the end frame `write` builds.
    nonce: u32,
}

impl Flight {
    /// Arena first, then the frames, then the log: the order `Journal::flush` keeps.
    pub(crate) fn write(self) -> Result<()> {
        self.write_with(|| {})
    }

    /// `write`, running `after_pwrite` between the frames' write and the log's sync (mutant M-b's
    /// seam: an acknowledgement there is an ack after the pwrite and before the sync).
    pub(crate) fn write_with(mut self, after_pwrite: impl FnOnce()) -> Result<()> {
        if self.fail {
            return Err(LimboError::InternalError(
                "failpoint: a branch log write failed".to_string(),
            ));
        }
        let Some(log) = self.log else {
            return Ok(());
        };
        // Mutant M-a (PREREG v1 amendment 36): the sync removed (the frames are written, never
        // synced). Caught by V1 (0 F_FULLFSYNC per create) and C1b, not by SIGKILL.
        let syncs = self.class.syncs() && !super::store::fe_mutant("no_flight_sync");
        // The slots these frames name reach the device before the log's sync, which drains it
        // (ruling 85a032f01): a plain fsync, not a flush of their own. A power cut before that sync
        // returns can keep the records and lose a slot, which recovery checks for in the last
        // flight (`last_flight_slots_hold`). An ORDERED flight has no flush after it before the trunk
        // commit's, so its slots are barriered ahead of its records instead. Mutant
        // `no_arena_sync` (test builds only) drops it.
        if syncs && !super::store::fe_mutant("no_arena_sync") {
            if let Some(arena) = &self.arena {
                if self.ordered {
                    barrier_file(arena, self.class)?;
                } else {
                    fsync_file(arena, SyncClass::Fsync)?;
                }
            }
        }
        super::store::kill_point("flight.arena_synced");
        if let Some(raised) = &self.header {
            write_at(&log, raised, HEADER_RAISED_AT)?;
        }
        if !self.bytes.is_empty() {
            // The end frame, built here with no lock held (review 2 #8), in the same write.
            let end = end_frame(self.nonce, self.class.syncs(), &self.bytes);
            self.bytes.extend_from_slice(&end);
            write_at(&log, &self.bytes, self.at)?;
        }
        super::store::kill_point("flight.log_written");
        after_pwrite();
        // After mutant M-b's early acknowledgement, before the log's sync.
        super::store::kill_point("flight.before_log_sync");
        if let Some(dir) = &self.dir {
            fsync_file(dir, SyncClass::Fsync)?;
        }
        if syncs && !self.ordered {
            fsync_file(&log, self.class)?;
            // The flush returned: confirm the flight (`HEADER_CONFIRM_AT`; not synced: its absence
            // proves nothing, its presence that this sync returned). Mutant `no_flight_confirm`.
            if !self.bytes.is_empty() && !super::store::fe_mutant("no_flight_confirm") {
                let crc = end_frame_crc(self.bytes[self.bytes.len() - END_FRAME_LEN..].try_into().unwrap());
                write_at(&log, &crc.to_le_bytes(), HEADER_CONFIRM_AT)?;
            }
        }
        // Mutant M-j (PREREG v1 amendment 36; test builds only): the barrier between the branch
        // log and the trunk's WAL removed. Caught by C1b.
        if syncs && self.ordered && !super::store::fe_mutant("no_log_barrier") {
            barrier_file(&log, self.class)?;
        }
        super::store::kill_point("flight.log_synced");
        Ok(())
    }

    /// Make this flight ORDERED instead of synced (see `ordered`): only for a trunk commit that will
    /// F_FULLFSYNC its WAL, on the branch files' device, after it.
    pub(crate) fn ordered(mut self) -> Self {
        self.ordered = true;
        self
    }

    /// Whether this flight writes or syncs anything.
    pub(crate) fn is_empty(&self) -> bool {
        self.log.is_none()
    }

    /// Frame bytes in this flight (observation only).
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }
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

/// crc32c of what a recovery scan checks, counted in test builds (review 2 #7's instrument: the
/// scan's CRC work must stay linear in the log).
fn scan_crc(data: &[u8]) -> u32 {
    #[cfg(test)]
    SCAN_CRC_BYTES.fetch_add(data.len() as u64, Ordering::Relaxed);
    crc32c::crc32c(data)
}

/// `scan_crc`, seeded (an end frame's checksums, with the incarnation's nonce).
fn scan_crc_seeded(seed: u32, data: &[u8]) -> u32 {
    #[cfg(test)]
    SCAN_CRC_BYTES.fetch_add(data.len() as u64, Ordering::Relaxed);
    crc32c::crc32c_append(seed, data)
}

#[cfg(test)]
pub(crate) static SCAN_CRC_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A flight's end frame (format 11): the tag (synced or not), the flight's length, and its crc32c,
/// the frame's own checksum and the flight's both seeded with the incarnation's `nonce`.
fn end_frame(nonce: u32, synced: bool, flight: &[u8]) -> [u8; END_FRAME_LEN] {
    let mut frame = [0u8; END_FRAME_LEN];
    let p = FRAME_HEADER_LEN;
    frame[p] = if synced { END_SYNCED_TAG } else { END_TAG };
    frame[p + 1..p + 5].copy_from_slice(&(flight.len() as u32).to_le_bytes());
    frame[p + 5..p + 9].copy_from_slice(&crc32c::crc32c_append(nonce, flight).to_le_bytes());
    frame[0..4].copy_from_slice(&(END_PAYLOAD_LEN as u32).to_le_bytes());
    let crc = crc32c::crc32c_append(nonce, &frame[p..]);
    frame[4..8].copy_from_slice(&crc.to_le_bytes());
    frame
}

/// Whether a frame of `len` payload bytes whose tag is `tag` is an end frame.
fn is_end(len: usize, tag: u8) -> bool {
    len == END_PAYLOAD_LEN && (tag == END_TAG || tag == END_SYNCED_TAG)
}

/// Whether the bytes at `at` could begin a whole frame, from its length, its tag and the count a
/// variable-length record carries alone: checked before any CRC, so a search over damage costs
/// O(bytes), not a checksum at every offset (review 2 #7). Returns the payload length.
fn plausible_frame(bytes: &[u8], at: usize) -> Option<usize> {
    let word = |o: usize| bytes.get(o..o + 4).map(|f| u32::from_le_bytes(f.try_into().unwrap()) as usize);
    let len = word(at)?;
    let p = at + FRAME_HEADER_LEN;
    let tag = *bytes.get(p)?;
    let ok = match tag {
        TAG_FORK => len == 17,
        TAG_FORK_NAMED => (21..=21 + 255).contains(&len) && word(p + 17) == Some(len - 21),
        TAG_COMMIT => len >= 13 && (len - 13) % 12 == 0 && word(p + 9) == Some((len - 13) / 12),
        TAG_TRUNK_RETAIN => len == 29,
        TAG_RELEASE | TAG_RELEASE_OPEN | TAG_CLOSE | TAG_CLOCK | TAG_CHECKPOINT => len == 9,
        TAG_LEASE => len == 25,
        END_TAG | END_SYNCED_TAG => len == END_PAYLOAD_LEN,
        _ => false,
    };
    (ok && p + len <= bytes.len()).then_some(len)
}

/// The first whole frame at or after `from`: `(offset, payload length, a synced end frame)`. An end
/// frame is whole only under this incarnation's `nonce`; a record frame by its own checksum and
/// decoding.
fn next_whole_frame(bytes: &[u8], from: usize, nonce: u32) -> Option<(usize, usize, bool)> {
    let last = bytes.len().checked_sub(FRAME_HEADER_LEN + 1)?;
    (from..=last).find_map(|at| {
        let len = plausible_frame(bytes, at)?;
        let crc = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().unwrap());
        let payload = &bytes[at + FRAME_HEADER_LEN..at + FRAME_HEADER_LEN + len];
        if is_end(len, payload[0]) {
            scan_crc_seeded(nonce, payload)
                .eq(&crc)
                .then_some((at, len, payload[0] == END_SYNCED_TAG))
        } else {
            (scan_crc(payload) == crc && Record::decode(payload).is_some()).then_some((at, len, false))
        }
    })
}

/// Whether the damage at `damage` lies in, or before, a flight known to have been synced (review 2
/// #2): a synced end frame of this incarnation after the damage, followed by any whole frame. A
/// flight is written only after the one before it was synced, so a frame after a synced flight's end
/// proves that sync returned: an acknowledged write was lost. Returns that end frame's offset.
///
/// Blind spot: a synced flight whose own end frame was lost too, with no frame after the next
/// flight's end, reads as a torn tail.
fn synced_flight_over(bytes: &[u8], damage: usize, nonce: u32) -> Option<usize> {
    // fastest-engine mutant `damage_always_torn` (test builds only): every damage is a torn flight.
    if super::store::fe_mutant("damage_always_torn") {
        return None;
    }
    // From the damage itself: an end frame whose own checksum holds but whose flight's does not
    // is where the scan stopped, and it is a synced flight covering the damage.
    let mut at = damage;
    let mut synced_end = None;
    while let Some((found, len, synced)) = next_whole_frame(bytes, at, nonce) {
        if synced_end.is_some() {
            return synced_end;
        }
        if synced {
            synced_end = Some(found);
        }
        at = found + FRAME_HEADER_LEN + len;
    }
    None
}

/// The record frames of `kept` (whole frames of an incarnation of nonce `old`, from a flight boundary
/// or a mark inside a flight, to a flight boundary) framed again for an incarnation of nonce `new`:
/// one end frame per kept flight, so every flight's boundary is kept (review 2 #3), and none for a
/// flight with no kept record. A frame that is not whole is refused: the region was written whole
/// before it was read. Returns the bytes and the last flight's checksum (for `HEADER_CONFIRM_AT`).
fn reframe(kept: &[u8], old: u32, new: u32, synced: bool) -> Result<(Vec<u8>, u32)> {
    let mut out = Vec::with_capacity(kept.len());
    let mut flight_at = 0;
    let mut last_crc = 0;
    let mut pos = 0;
    while pos < kept.len() {
        let whole = kept.get(pos..pos + FRAME_HEADER_LEN).and_then(|head| {
            let len = u32::from_le_bytes(head[0..4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(head[4..8].try_into().unwrap());
            let payload = kept.get(pos + FRAME_HEADER_LEN..pos + FRAME_HEADER_LEN + len)?;
            let end = len > 0 && is_end(len, payload[0]);
            let ok = if end {
                crc32c::crc32c_append(old, payload) == crc
            } else {
                len > 0 && crc32c::crc32c(payload) == crc && Record::decode(payload).is_some()
            };
            ok.then_some((len, end))
        });
        let Some((len, end)) = whole else {
            return Err(corrupt(&format!(
                "branch log rewrite met a frame that is not whole at byte {pos} of its kept suffix"
            )));
        };
        let next = pos + FRAME_HEADER_LEN + len;
        if !end {
            out.extend_from_slice(&kept[pos..next]);
        } else if out.len() > flight_at {
            let frame = end_frame(new, synced, &out[flight_at..]);
            last_crc = end_frame_crc(&frame);
            out.extend_from_slice(&frame);
            flight_at = out.len();
        }
        pos = next;
    }
    if out.len() > flight_at {
        let frame = end_frame(new, synced, &out[flight_at..]);
        last_crc = end_frame_crc(&frame);
        out.extend_from_slice(&frame);
    }
    Ok((out, last_crc))
}

/// The flight checksum an end frame carries.
fn end_frame_crc(frame: &[u8; END_FRAME_LEN]) -> u32 {
    let p = FRAME_HEADER_LEN;
    u32::from_le_bytes(frame[p + 5..p + 9].try_into().unwrap())
}

/// Whether every arena slot the records of the log's LAST flight name holds what they say (ruling
/// 85a032f01): the arena is synced only by plain fsync before the log's flush, so a power cut can
/// keep the last flight's records and lose a slot. Only the last flight's: an older flight's slot
/// may have been freed and reused since. A slot that cannot be read fails.
fn last_flight_slots_hold(files: &BranchFiles, page_size: u32, records: &[Record]) -> bool {
    let slots: Vec<(Slot, u32)> = records
        .iter()
        .flat_map(|r| match r {
            Record::Commit { pages, .. } => pages.iter().map(|&(_, slot, crc)| (slot, crc)).collect(),
            Record::TrunkRetain { slot, crc, .. } => vec![(*slot, *crc)],
            _ => Vec::new(),
        })
        .collect();
    if slots.is_empty() || super::store::fe_mutant("skip_slot_crc_check") {
        return true;
    }
    let Ok(arena) = File::open(&files.arena) else {
        return false;
    };
    let mut page = vec![0u8; page_size as usize];
    slots.iter().all(|&(slot, crc)| {
        read_at(&arena, &mut page, slot as u64 * page_size as u64).is_ok() && crc32c::crc32c(&page) == crc
    })
}

/// `Some((page_size, generation, nonce))` if the header is whole and valid, `None` if it is torn (a
/// crash before it was durable). A whole header of ANOTHER format version is an error, not a torn
/// one: taking it for torn would start an empty store over it, or reset its log.
fn parse_log_header(bytes: &[u8], format: u32) -> Result<Option<(u32, u64, u32)>> {
    // A header of a format before 11 is 32 bytes, its checksum at 24: read the version first, so
    // such a log is refused by name rather than taken for a torn header.
    let Some(h) = bytes.get(..16) else {
        return Ok(None);
    };
    if &h[0..8] != LOG_MAGIC {
        return Ok(None);
    }
    let field = |at: usize| bytes.get(at..at + 4).map(|f| u32::from_le_bytes(f.try_into().unwrap()));
    let version = field(8).expect("16 bytes read");
    let old = version < 11
        && bytes.len() >= 28
        && crc32c::crc32c(&bytes[..24]) == field(24).expect("28 bytes read");
    if old || (version != format && bytes.len() >= LOG_HEADER_LEN && crc32c::crc32c(&bytes[..HEADER_CRC_AT]) == field(HEADER_CRC_AT).expect("40 bytes")) {
        return Err(corrupt(&format!(
            "log format version {version}; this store reads version {format}{}",
            version_hint(version, format)
        )));
    }
    let Some(h) = bytes.get(..LOG_HEADER_LEN) else {
        return Ok(None);
    };
    if crc32c::crc32c(&h[..HEADER_CRC_AT]) != field(HEADER_CRC_AT).expect("40 bytes") {
        return Ok(None);
    }
    let generation = u64::from_le_bytes(h[16..24].try_into().unwrap());
    let nonce = field(HEADER_NONCE_AT).expect("40 bytes");
    Ok(Some((field(12).expect("40 bytes"), generation, nonce)))
}

/// What a refused format version is (review 2 #13): the other arm's, or one written before the log
/// nonce (format 11), or before flight framing (format 9), which this build cannot read.
pub(crate) fn version_hint(version: u32, format: u32) -> String {
    let other = if format == FORMAT_VERSION { SPLICE_FORMAT_VERSION } else { FORMAT_VERSION };
    if version == other {
        format!(" ({other} is the other F7 splice arm's: open with that arm's DatabaseOpts::with_branch_splice)")
    } else if version < 9 {
        " (written before flight framing; recreate the branch files)".to_string()
    } else if version < 11 {
        " (written before the log nonce; recreate the branch files)".to_string()
    } else {
        String::new()
    }
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
    // fastest-engine mutant `no_log_lock` (test builds only): no lock is taken.
    if super::store::fe_mutant("no_log_lock") {
        return Ok(());
    }
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
        return Err(corrupt("snapshot format version (8 is the F7 splice arm's)"));
    }
    let page_size = u32::from_le_bytes(body[12..16].try_into().unwrap());
    let generation = u64::from_le_bytes(body[16..24].try_into().unwrap());
    let state = SnapshotState::decode(&body[HEAD..]).ok_or_else(|| corrupt("snapshot body"))?;
    Ok((page_size, generation, state, bytes.len() as u64))
}

pub(crate) fn open_rw(path: &Path, truncate: bool) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(truncate)
        .open(path)
        .map_err(|e| io_error(e, "open branch file"))?;
    // A truncating open drops what the simulated power loss held for the file (test builds).
    #[cfg(all(test, unix))]
    if truncate {
        lose_unsynced::truncate(&file, 0);
    }
    Ok(file)
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
    #[cfg(all(test, unix))]
    if lose_unsynced::hold(file, bytes, offset) {
        return Ok(());
    }
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
    #[cfg(all(test, unix))]
    if lose_unsynced::read(file, out, offset)? {
        return Ok(());
    }
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

/// A branch file's length, counting writes the simulated power loss holds (test builds).
pub(crate) fn file_len(file: &File) -> Result<u64> {
    let real = file
        .metadata()
        .map_err(|e| io_error(e, "stat branch file"))?
        .len();
    #[cfg(all(test, unix))]
    return Ok(real.max(lose_unsynced::held_end(file)));
    #[cfg(not(all(test, unix)))]
    Ok(real)
}

/// Truncate (or extend) a branch file; held writes past the new end are dropped (test builds).
pub(crate) fn set_file_len(file: &File, len: u64) -> Result<()> {
    #[cfg(all(test, unix))]
    lose_unsynced::truncate(file, len);
    file.set_len(len).map_err(|e| io_error(e, "truncate branch file"))
}

/// fastest-engine C1, SIMULATED POWER LOSS of the branch files (TEST BUILDS ONLY, armed by
/// `FE_LOSE_UNSYNCED=1` in the process that writes): every write to a branch file (log, arena,
/// snapshot, a log rewrite's temp file) is held in this process's memory, per file (device, inode),
/// and reaches the file only when THAT file is synced (`fsync_file`); reads and lengths see the held
/// writes. A SIGKILL then loses exactly the writes no sync covered, as a power cut loses the OS page
/// cache. NOT simulated (stated, not assumed away): the trunk's WAL and database file and the
/// catalog (Turso's own IO, whose unsynced writes survive a SIGKILL); a write torn inside a page;
/// a drive persisting synced writes out of order; a directory entry made durable without its
/// directory's sync. So it can show a branch-file sync missing, never a trunk-side one.
#[cfg(all(test, unix))]
mod lose_unsynced {
    use super::*;
    use std::collections::HashMap;
    use std::os::unix::fs::{FileExt, MetadataExt};
    use std::sync::{Mutex, OnceLock};

    type Held = HashMap<(u64, u64), Vec<(u64, Vec<u8>)>>;

    fn armed() -> bool {
        static ON: OnceLock<bool> = OnceLock::new();
        *ON.get_or_init(|| std::env::var("FE_LOSE_UNSYNCED").is_ok_and(|v| v == "1"))
    }

    fn held() -> &'static Mutex<Held> {
        static HELD: OnceLock<Mutex<Held>> = OnceLock::new();
        HELD.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn key(file: &File) -> Option<(u64, u64)> {
        file.metadata().ok().map(|m| (m.dev(), m.ino()))
    }

    /// Hold a write instead of making it; `false` when not armed (the caller writes).
    pub(super) fn hold(file: &File, bytes: &[u8], offset: u64) -> bool {
        if !armed() {
            return false;
        }
        let Some(k) = key(file) else {
            return false;
        };
        held().lock().unwrap().entry(k).or_default().push((offset, bytes.to_vec()));
        true
    }

    /// Read through the held writes; `false` when not armed (the caller reads).
    pub(super) fn read(file: &File, out: &mut [u8], offset: u64) -> Result<bool> {
        if !armed() {
            return Ok(false);
        }
        let Some(k) = key(file) else {
            return Ok(false);
        };
        let held = held().lock().unwrap();
        let writes = held.get(&k);
        // The real bytes first (a short read past the real end is zeros, then overlaid).
        out.fill(0);
        let real_len = file.metadata().map_err(|e| io_error(e, "stat branch file"))?.len();
        if offset < real_len {
            let n = ((real_len - offset) as usize).min(out.len());
            file.read_exact_at(&mut out[..n], offset)
                .map_err(|e| io_error(e, "read branch file"))?;
        }
        let end = offset + out.len() as u64;
        let mut covered_to = real_len;
        for (at, bytes) in writes.into_iter().flatten() {
            let (w_lo, w_hi) = (*at, *at + bytes.len() as u64);
            covered_to = covered_to.max(w_hi);
            let (lo, hi) = (w_lo.max(offset), w_hi.min(end));
            if lo < hi {
                out[(lo - offset) as usize..(hi - offset) as usize]
                    .copy_from_slice(&bytes[(lo - w_lo) as usize..(hi - w_lo) as usize]);
            }
        }
        if end > covered_to {
            return Err(io_error(
                std::io::Error::from(std::io::ErrorKind::UnexpectedEof),
                "read branch file",
            ));
        }
        Ok(true)
    }

    /// A sync: the held writes reach the file, in order. Written while the held set is still
    /// locked and removed only after, so a concurrent read never finds the bytes in neither place
    /// (lead review 1 item 22d: an EOF, or a reused slot's old bytes).
    pub(super) fn apply(file: &File) -> Result<()> {
        if !armed() {
            return Ok(());
        }
        let Some(k) = key(file) else {
            return Ok(());
        };
        let mut held = held().lock().unwrap();
        for (at, bytes) in held.get(&k).into_iter().flatten() {
            file.write_all_at(bytes, *at)
                .map_err(|e| io_error(e, "write branch file"))?;
        }
        held.remove(&k);
        Ok(())
    }

    pub(super) fn held_end(file: &File) -> u64 {
        if !armed() {
            return 0;
        }
        let Some(k) = key(file) else {
            return 0;
        };
        held()
            .lock()
            .unwrap()
            .get(&k)
            .into_iter()
            .flatten()
            .map(|(at, b)| at + b.len() as u64)
            .max()
            .unwrap_or(0)
    }

    pub(super) fn truncate(file: &File, len: u64) {
        if !armed() {
            return;
        }
        let Some(k) = key(file) else {
            return;
        };
        if let Some(writes) = held().lock().unwrap().get_mut(&k) {
            writes.retain_mut(|(at, bytes)| {
                if *at >= len {
                    return false;
                }
                bytes.truncate((len - *at) as usize);
                true
            });
        }
    }
}

/// The device an open file lives on (`fstat`'s `st_dev`); `None` where it cannot be read or off
/// unix.
pub(crate) fn file_device(file: &File) -> Option<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        file.metadata().ok().map(|m| m.dev())
    }
    #[cfg(not(unix))]
    {
        let _ = file;
        None
    }
}

/// Ruling 85a032f01's guard: the arena is synced by plain fsync before the LOG's flush makes it
/// durable, which holds only on one device (a flush drains one device's cache). The log and the
/// arena, as opened, must be on the same one; a device that cannot be read is refused too.
pub(crate) fn one_device(log: Option<u64>, arena: Option<u64>) -> Result<()> {
    match (log, arena) {
        (Some(log), Some(arena)) if log == arena => Ok(()),
        _ if cfg!(not(unix)) => Ok(()),
        (log, arena) => Err(LimboError::InvalidArgument(format!(
            "the branch log (device {log:?}) and the branch arena (device {arena:?}) must be on one \
             device: the log's flush is what makes the arena durable"
        ))),
    }
}

/// Sync `file` in `class` (see [`SyncClass`]): `Fsync` is `fsync(2)`, as Turso's own
/// `FileSyncType::Fsync`; `FullFsync` is `fcntl(F_FULLFSYNC)` on Apple platforms, as Turso's
/// `FileSyncType::FullFsync` (`io/unix.rs`), and `fsync(2)` elsewhere. `Off` syncs nothing.
pub(crate) fn fsync_file(file: &File, class: SyncClass) -> Result<()> {
    if !class.syncs() {
        return Ok(());
    }
    // A sync makes the held writes reach the file first (simulated power loss, test builds).
    #[cfg(all(test, unix))]
    lose_unsynced::apply(file)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        #[cfg(target_vendor = "apple")]
        if class == SyncClass::FullFsync {
            // SAFETY: as below.
            if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) } == -1 {
                return Err(io_error(std::io::Error::last_os_error(), "F_FULLFSYNC branch file"));
            }
            crate::io::count_sync(true);
            return Ok(());
        }
        // SAFETY: the descriptor is owned by `file` and open for the duration of the call.
        if unsafe { libc::fsync(file.as_raw_fd()) } != 0 {
            return Err(io_error(std::io::Error::last_os_error(), "fsync branch file"));
        }
        crate::io::count_sync(false);
        Ok(())
    }
    #[cfg(not(unix))]
    {
        file.sync_all()
            .map_err(|e| io_error(e, "fsync branch file"))
    }
}

/// ORDER every write made to `file` so far before every write made after this returns, in `class`
/// (lead review 1 item 1): what the arena needs before the log record that names its slots, since
/// the log's own sync in `class` then makes both durable. On Apple at `FullFsync` that is
/// `fcntl(F_BARRIERFSYNC)` — the file's data reaches the device, ordered by a barrier, without
/// draining the device's cache — falling back to `F_FULLFSYNC` where the filesystem refuses it. In
/// every other case it is `fsync_file` (`fsync(2)` orders by making durable). `Off` does nothing.
///
/// Only a write that a LATER full sync in `class` covers may rest on this: the barrier alone makes
/// nothing durable.
pub(crate) fn barrier_file(file: &File, class: SyncClass) -> Result<()> {
    #[cfg(target_vendor = "apple")]
    if class == SyncClass::FullFsync {
        use std::os::fd::AsRawFd;
        // Simulated power loss treats a barrier as a sync of the file (a blind spot: an
        // acknowledgement resting on a barrier alone is C1b's to catch, not C1's).
        #[cfg(test)]
        lose_unsynced::apply(file)?;
        // SAFETY: the descriptor is owned by `file` and open for the duration of the call.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_BARRIERFSYNC) } != -1 {
            crate::io::count_barrier();
            return Ok(());
        }
        return fsync_file(file, class);
    }
    fsync_file(file, class)
}

/// Make a file's creation or rename durable: on POSIX that is a sync of its directory, in `class`.
pub(crate) fn fsync_dir_of(path: &Path, class: SyncClass) -> Result<()> {
    #[cfg(unix)]
    {
        let dir = match path.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir,
            _ => Path::new("."),
        };
        let d = File::open(dir).map_err(|e| io_error(e, "open branch directory"))?;
        fsync_file(&d, class)
    }
    #[cfg(not(unix))]
    {
        let _ = (path, class);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The nonce in the header of the log at `path` (format 11).
    pub(super) fn log_nonce(path: &Path) -> u32 {
        let bytes = std::fs::read(path).unwrap();
        u32::from_le_bytes(bytes[HEADER_NONCE_AT..HEADER_NONCE_AT + 4].try_into().unwrap())
    }

    /// An end frame for the record frames `flight`, as the log at `path` would write it.
    pub(super) fn forged_end(path: &Path, synced: bool, flight: &[u8]) -> [u8; END_FRAME_LEN] {
        end_frame(log_nonce(path), synced, flight)
    }

    /// Two whole SYNCED flights of `frames` each, as the log at `path` would write them: what proves,
    /// to recovery, that whatever was lost before them had been synced (review 2 #2).
    pub(super) fn forged_synced_flights(path: &Path, frames: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        for _ in 0..2 {
            out.extend_from_slice(frames);
            out.extend_from_slice(&forged_end(path, true, frames));
        }
        out
    }

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
        let mut stale = Journal::create(&files, 512, SyncClass::Off).unwrap();
        stale.buffer(&Record::Fork { child: 1, parent: 0 }).unwrap();
        stale.flush(&mut arena).unwrap();

        let mut fresh = Journal::recover(&files, SyncClass::Off).unwrap().expect("state");
        fresh.journal.buffer(&Record::Release { branch: 1 }).unwrap();
        fresh.journal.flush(&mut arena).unwrap();

        stale.buffer(&Record::Fork { child: 2, parent: 0 }).unwrap();
        assert!(stale.flush(&mut arena).is_err(), "a stale journal wrote over a newer one");
        let records = Journal::recover(&files, SyncClass::Off).unwrap().expect("state").records;
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
        let mut journal = Journal::create(&files, 512, SyncClass::Off).unwrap();
        journal.buffer(&Record::Fork { child: 1, parent: 0 }).unwrap();
        journal.flush(&mut arena).unwrap();

        let mut payload = Vec::new();
        Record::Release { branch: 1 }.encode(&mut payload);
        let mut frame = Vec::new();
        put_u32(&mut frame, payload.len() as u32);
        put_u32(&mut frame, crc32c::crc32c(&payload));
        frame.extend_from_slice(&payload);
        // fastest-engine format 9/11 (FLAGGED TEST EDIT, fixture only): the foreign write is a whole
        // flight, its frame and its end frame under the log's nonce. A bare frame is a torn
        // flight's remnant in a log written in flights, and recovery cuts it.
        let end = forged_end(&files.log, false, &frame);
        frame.extend_from_slice(&end);
        {
            use std::io::Write;
            let mut foreign = OpenOptions::new().append(true).open(&files.log).unwrap();
            foreign.write_all(&frame).unwrap();
        }

        journal.buffer(&Record::Fork { child: 2, parent: 0 }).unwrap();
        assert!(journal.flush(&mut arena).is_err(), "a journal appended over bytes it did not write");
        drop(journal);
        let records = Journal::recover(&files, SyncClass::Off).unwrap().expect("state").records;
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
        let mut journal = Journal::create(&files, 512, SyncClass::Off).unwrap();
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
        let records = Journal::recover(&files, SyncClass::Off).unwrap().expect("state").records;
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
        let mut journal = Journal::create(&files, 512, SyncClass::Off).unwrap();
        journal.buffer(&Record::Fork { child: 1, parent: 0 }).unwrap();
        journal.flush(&mut arena).unwrap();
        drop(journal);
        let whole = std::fs::metadata(&files.log).unwrap().len();
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&files.log).unwrap();
            f.write_all(&[0u8; 64]).unwrap();
        }
        let recovered = Journal::recover(&files, SyncClass::Off)
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
        let mut journal = Journal::create(&files, 512, SyncClass::Off).unwrap();
        journal.buffer(&Record::Fork { child: 1, parent: 0 }).unwrap();
        journal.flush(&mut arena).unwrap();
        drop(journal);
        let mut payload = Vec::new();
        Record::Release { branch: 1 }.encode(&mut payload);
        let mut frame = Vec::new();
        put_u32(&mut frame, payload.len() as u32);
        put_u32(&mut frame, crc32c::crc32c(&payload));
        frame.extend_from_slice(&payload);
        // fastest-engine format 9/11 (FLAGGED TEST EDIT, fixture only): what survived after the hole
        // is a whole SYNCED later flight with a frame after it (format 11's proof that the lost
        // write's sync had returned, review 2 #2). A bare frame after the hole is a torn flight's
        // remnant in a log written in flights, and recovery cuts it (`flight_tests` pins both).
        let frame = forged_synced_flights(&files.log, &frame);
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&files.log).unwrap();
            f.write_all(&[0u8; 25]).unwrap(); // a lost frame, read back as zeros
            f.write_all(&frame).unwrap(); // a later flight that survived
        }
        let before = std::fs::read(&files.log).unwrap();
        assert!(
            Journal::recover(&files, SyncClass::Off).is_err(),
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
        let mut journal = Journal::create(&files, 512, SyncClass::Off).unwrap();
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
        // fastest-engine format 9/11 (FLAGGED TEST EDIT, fixture only): a whole synced later flight
        // and a frame after it, as in `a_zeroed_hole_followed_by_whole_frames_is_corrupt`.
        let frame = forged_synced_flights(&files.log, &frame);
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&files.log).unwrap();
            f.write_all(&[0u8; 25]).unwrap();
            f.write_all(&frame).unwrap();
        }
        let err = match Journal::recover(&files, SyncClass::Off) {
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
        let mut journal = Journal::create(&files, 512, SyncClass::Off).unwrap();
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
        let records = Journal::recover(&files, SyncClass::Off).unwrap().expect("state").records;
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
        let mut first = Journal::create(&files, 512, SyncClass::Off).unwrap();
        first.buffer(&Record::Fork { child: 1, parent: 0 }).unwrap();
        first.flush(&mut arena).unwrap();

        assert!(Journal::recover(&files, SyncClass::Off).is_err(), "a second journal recovered a live log");
        assert!(Journal::create(&files, 512, SyncClass::Off).is_err(), "a second journal re-created a live log");
        // The refused create must not have truncated the live log on its way to being refused.
        first.buffer(&Record::Release { branch: 1 }).unwrap();
        first.flush(&mut arena).unwrap();
        drop(first);
        let records = Journal::recover(&files, SyncClass::Off).unwrap().expect("state").records;
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
        assert!(Journal::recover(&files, SyncClass::Off).is_err(), "a log of another version was accepted");

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
        assert!(Journal::recover(&files, SyncClass::Off).is_err(), "a snapshot of another version was accepted");
    }

    /// r11-ever amendment 15: the F7 splice arm writes version 4 and the default arm 3, and each
    /// reads only its own, because replay repeats every splice (a log written with splices and
    /// replayed without them would rebuild another tree). The log's check and the snapshot's are
    /// each made to fire alone: in the second half the log is of the READING arm's version.
    #[test]
    fn each_splice_arm_reads_only_its_own_format_version() {
        // FLAGGED TEST EDIT (format 11, fixture only): the header's format-11 layout, nonce
        // under its checksum.
        fn header(format: u32, generation: u64) -> Vec<u8> {
            log_header(format, 512, generation, 0x5EED, SyncClass::Off)
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
                Journal::recover_as(&files, SyncClass::Off, other).is_err(),
                "splice={written}: the other arm read this arm's log"
            );
            let got = Journal::recover_as(&files, SyncClass::Off, own).unwrap();
            assert!(got.is_some(), "splice={written}: its own arm found no state in its log");
            drop(got);

            std::fs::write(&files.snap, snapshot(own, 1)).unwrap();
            std::fs::write(&files.log, header(other, 1)).unwrap();
            assert!(
                Journal::recover_as(&files, SyncClass::Off, other).is_err(),
                "splice={written}: the other arm read this arm's snapshot"
            );
            std::fs::write(&files.log, header(own, 1)).unwrap();
            let got = Journal::recover_as(&files, SyncClass::Off, own).unwrap();
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
                    name: Some("server-one".to_string()),
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
                    name: None,
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
                    name: None,
                },
            ],
        };
        let mut body = Vec::new();
        state.encode(&mut body);
        assert_eq!(SnapshotState::decode(&body), Some(state));
        assert_eq!(SnapshotState::decode(&body[..body.len() - 1]), None);
    }
}

/// fastest-engine M2 (r12-noforce's flight framing 339c17b2a / 02c5ce7f0, ported to the r13 store's
/// group flights): a log written in flights tells a flight torn in the air — written, never synced,
/// so never acknowledged — from an acknowledged flight lost under a later one; and a catalog
/// checkpoint's cut log keeps its suffix readable.
#[cfg(test)]
mod flight_tests {
    use super::*;

    /// Three flights, each written as a group flight writes it and synced in `class` (`Off`: none
    /// is synced, as under D0). Returns the files and where each flight began.
    fn three_flights(dir: &Path, class: SyncClass) -> (BranchFiles, [u64; 3]) {
        let files = BranchFiles::for_db(dir.join("db").to_str().unwrap());
        let mut journal = Journal::create(&files, 512, class).unwrap();
        let mut arena = Arena::new(512);
        let mut starts = [0; 3];
        for (i, start) in starts.iter_mut().enumerate() {
            journal.buffer(&Record::Fork { child: i as u64 + 1, parent: 0 }).unwrap();
            journal.buffer(&Record::Clock { now_ms: 7 }).unwrap();
            *start = journal.len;
            journal.take_flight(&mut arena, class, false).unwrap().write().unwrap();
        }
        (files, starts)
    }

    fn zero(path: &Path, at: u64, n: usize) {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = OpenOptions::new().write(true).open(path).unwrap();
        f.seek(SeekFrom::Start(at)).unwrap();
        f.write_all(&vec![0u8; n]).unwrap();
    }

    fn forks(records: &[Record]) -> Vec<u64> {
        records
            .iter()
            .filter_map(|r| match r {
                Record::Fork { child, .. } => Some(*child),
                _ => None,
            })
            .collect()
    }

    /// A flight spans blocks, so a power cut while it is in the air can keep its end and lose its
    /// start. It was never synced, so never acknowledged: recovery cuts the whole flight instead of
    /// refusing the store as Corrupt (339c17b2a's finding 3).
    #[test]
    fn a_flight_torn_in_the_air_is_cut_not_corrupt() {
        let dir = tempfile::TempDir::new().unwrap();
        let (files, starts) = three_flights(dir.path(), SyncClass::Fsync);
        zero(&files.log, starts[2], FRAME_HEADER_LEN + 4);
        let recovered = Journal::recover(&files, SyncClass::Off)
            .expect("a hole inside the last flight is a torn flight, not corruption")
            .expect("state");
        assert_eq!(forks(&recovered.records), vec![1, 2], "the torn flight was not cut whole");
        drop(recovered);
        assert_eq!(std::fs::metadata(&files.log).unwrap().len(), starts[2]);
    }

    /// ...and the refusal stands where it protects something: a hole with a WHOLE later flight after
    /// it means an earlier, acknowledged flight was lost under a later one.
    #[test]
    fn a_hole_under_a_whole_later_flight_is_still_corrupt() {
        let dir = tempfile::TempDir::new().unwrap();
        let (files, starts) = three_flights(dir.path(), SyncClass::Fsync);
        zero(&files.log, starts[1], FRAME_HEADER_LEN + 4);
        assert!(Journal::recover(&files, SyncClass::Off).is_err(), "a lost acknowledged flight was cut");
    }

    /// A flight whose end did not reach the disk whole is dropped whole: none of it was
    /// acknowledged, and a whole record inside it is not replayed on its own (mutant M-h,
    /// `apply_torn_flight`, must fail this).
    #[test]
    fn a_flight_whose_end_is_torn_is_dropped_whole() {
        let dir = tempfile::TempDir::new().unwrap();
        let (files, starts) = three_flights(dir.path(), SyncClass::Fsync);
        let len = std::fs::metadata(&files.log).unwrap().len();
        OpenOptions::new().write(true).open(&files.log).unwrap().set_len(len - 3).unwrap();
        let recovered = Journal::recover(&files, SyncClass::Off).unwrap().expect("state");
        assert_eq!(forks(&recovered.records), vec![1, 2], "a torn flight's whole record was replayed");
        drop(recovered);
        assert_eq!(std::fs::metadata(&files.log).unwrap().len(), starts[2]);
    }

    /// ANY damage under a whole later flight is Corrupt, not only a zeroed frame (02c5ce7f0's R2-3).
    #[cfg(unix)]
    #[test]
    fn a_garbled_frame_under_a_whole_later_flight_is_corrupt() {
        let dir = tempfile::TempDir::new().unwrap();
        let (files, starts) = three_flights(dir.path(), SyncClass::Fsync);
        let mut byte = std::fs::read(&files.log).unwrap()[starts[1] as usize + FRAME_HEADER_LEN];
        byte ^= 0x5A;
        {
            use std::os::unix::fs::FileExt;
            let f = OpenOptions::new().write(true).open(&files.log).unwrap();
            f.write_all_at(&[byte], starts[1] + FRAME_HEADER_LEN as u64).unwrap();
        }
        assert!(Journal::recover(&files, SyncClass::Off).is_err(), "damage under a later flight was cut");
    }

    /// Review B-F5: under D0 no flight is synced, so a later flight that survived proves nothing
    /// about an earlier one — the OS may write them back in any order. A flight lost under a later
    /// one is cut there, with everything after it, instead of refusing the store.
    #[test]
    fn an_unsynced_flight_lost_under_a_later_unsynced_one_is_cut() {
        let dir = tempfile::TempDir::new().unwrap();
        let (files, starts) = three_flights(dir.path(), SyncClass::Off);
        zero(&files.log, starts[1], FRAME_HEADER_LEN + 4);
        let recovered = Journal::recover(&files, SyncClass::Off)
            .expect("nothing was synced, so nothing acknowledged-durable was lost")
            .expect("state");
        assert_eq!(forks(&recovered.records), vec![1], "not cut at the lost flight");
        drop(recovered);
        assert_eq!(std::fs::metadata(&files.log).unwrap().len(), starts[1]);
    }

    /// The last flight lost its first block AND its end while a frame in its middle survived: never
    /// synced, so cut back to the flight before it (02c5ce7f0's R2-4).
    #[test]
    fn a_last_flight_that_lost_its_start_and_end_is_cut() {
        let dir = tempfile::TempDir::new().unwrap();
        let (files, starts) = three_flights(dir.path(), SyncClass::Fsync);
        zero(&files.log, starts[2], FRAME_HEADER_LEN + 4);
        let len = std::fs::metadata(&files.log).unwrap().len();
        OpenOptions::new().write(true).open(&files.log).unwrap().set_len(len - 3).unwrap();
        let recovered = Journal::recover(&files, SyncClass::Off)
            .expect("a torn last flight is not corruption")
            .expect("state");
        assert_eq!(forks(&recovered.records), vec![1, 2]);
    }

    /// The r13 composition: a catalog checkpoint keeps the log's suffix after its capture, which can
    /// begin INSIDE a flight. The cut log must read back every kept record, including the second
    /// half of that flight, and a flight written after the cut (fastest-engine; r12-noforce's port
    /// had no log rewrite).
    #[test]
    fn a_cut_log_keeps_every_record_after_the_cut_even_mid_flight() {
        let dir = tempfile::TempDir::new().unwrap();
        let (files, starts) = three_flights(dir.path(), SyncClass::Fsync);
        let mut journal = Journal::recover(&files, SyncClass::Off).unwrap().expect("state").journal;
        // The cut falls after flight 2's first frame: its Clock and all of flight 3 are kept.
        let fork_frame = {
            let bytes = std::fs::read(&files.log).unwrap();
            let at = starts[1] as usize;
            FRAME_HEADER_LEN + u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize
        };
        journal.rewrite_from(starts[1] + fork_frame as u64, 1).unwrap();
        let mut arena = Arena::new(512);
        journal.buffer(&Record::Fork { child: 4, parent: 0 }).unwrap();
        journal.take_flight(&mut arena, SyncClass::Off, false).unwrap().write().unwrap();
        drop(journal);
        let recovered = Journal::recover_catalog_as(&files, SyncClass::Off, Some((512, 1)), FORMAT_VERSION)
            .unwrap()
            .expect("state");
        assert_eq!(forks(&recovered.records), vec![3, 4], "the cut log lost a kept record");
        let clocks = recovered.records.iter().filter(|r| matches!(r, Record::Clock { .. })).count();
        assert_eq!(clocks, 2, "flight 2's kept Clock and flight 3's");
    }
}

/// Review 2's format bump (11/12), red first against 675adbfb3: a log incarnation that recovery can
/// tell from an older one, an end frame that says whether its flight was synced, a cut that keeps
/// every flight's boundary, a recovery scan linear in the log, and a 17-byte end frame.
#[cfg(test)]
mod format_tests {
    use super::*;

    fn flights(files: &BranchFiles, generation: u64, shapes: &[usize], class: SyncClass) -> Vec<u64> {
        let mut journal = Journal::create(files, 512, class).unwrap();
        if generation > 0 {
            journal.rewrite_from(journal.len, generation).unwrap();
        }
        let mut arena = Arena::new(512);
        let mut starts = Vec::new();
        for (i, &records) in shapes.iter().enumerate() {
            for j in 0..records {
                journal
                    .buffer(&Record::Fork { child: 1000 * generation + 10 * i as u64 + j as u64 + 1, parent: 0 })
                    .unwrap();
            }
            starts.push(std::fs::metadata(&files.log).unwrap().len());
            journal.take_flight(&mut arena, class, false).unwrap().write().unwrap();
        }
        starts
    }

    fn forks(records: &[Record]) -> Vec<u64> {
        records
            .iter()
            .filter_map(|r| match r {
                Record::Fork { child, .. } => Some(*child),
                _ => None,
            })
            .collect()
    }

    fn overwrite(path: &Path, at: u64, bytes: &[u8]) {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = OpenOptions::new().write(true).open(path).unwrap();
        f.seek(SeekFrom::Start(at)).unwrap();
        f.write_all(bytes).unwrap();
    }

    /// Review 2 #1 (HIGH): a log reset in place whose truncation never reached the disk leaves the
    /// new incarnation's header and flights over the old one's bytes. Recovery returns exactly the
    /// new flights — none of the old ones, however their sizes line up — and a control with the
    /// truncation persisted returns the same.
    #[test]
    fn an_older_incarnations_tail_is_never_replayed() {
        for (k, shapes) in [(0usize, vec![]), (1, vec![1]), (2, vec![1, 1]), (1, vec![3]), (2, vec![2, 1])] {
            for truncated in [false, true] {
                let dir = tempfile::TempDir::new().unwrap();
                let old = BranchFiles::for_db(dir.path().join("old").to_str().unwrap());
                flights(&old, 0, &[1; 40], SyncClass::Fsync);
                let new = BranchFiles::for_db(dir.path().join("new").to_str().unwrap());
                flights(&new, 1, &shapes, SyncClass::Fsync);
                let image = std::fs::read(&new.log).unwrap();
                if truncated {
                    std::fs::write(&old.log, &image).unwrap();
                } else {
                    overwrite(&old.log, 0, &image);
                }
                let recovered =
                    Journal::recover_catalog_as(&old, SyncClass::Off, Some((512, 1)), FORMAT_VERSION)
                        .unwrap_or_else(|e| panic!("k={k} {shapes:?} truncated={truncated}: refused: {e}"))
                        .expect("state");
                let want: Vec<u64> = shapes
                    .iter()
                    .enumerate()
                    .flat_map(|(i, &n)| (0..n).map(move |j| 1000 + 10 * i as u64 + j as u64 + 1))
                    .collect();
                assert_eq!(forks(&recovered.records), want, "k={k} {shapes:?} truncated={truncated}");
            }
        }
    }

    /// Review 2 #2: flight 2 lost a block while its own end frame survived, and flight 3 was torn
    /// at its end: flight 3's surviving frames prove flight 2's sync had returned, so this is not a
    /// torn tail. Refused, and the log left byte-identical.
    #[test]
    fn a_damaged_synced_flight_with_a_later_flights_frames_after_it_is_refused() {
        let dir = tempfile::TempDir::new().unwrap();
        let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
        let starts = flights(&files, 0, &[2, 2, 2], SyncClass::Fsync);
        overwrite(&files.log, starts[1], &[0u8; 12]);
        let len = std::fs::metadata(&files.log).unwrap().len();
        OpenOptions::new().write(true).open(&files.log).unwrap().set_len(len - 3).unwrap();
        let before = std::fs::read(&files.log).unwrap();
        assert!(Journal::recover(&files, SyncClass::Off).is_err(), "an acknowledged flight was cut");
        assert_eq!(std::fs::read(&files.log).unwrap(), before, "the refused recovery changed the log");
    }

    /// Review 2 #3: a cut keeps each kept flight's boundary, so damage to an early kept flight with
    /// later kept flights whole after it — all of them synced by the cut — is refused, not cut.
    #[test]
    fn a_cut_log_keeps_its_flights_apart() {
        let dir = tempfile::TempDir::new().unwrap();
        let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
        let starts = flights(&files, 0, &[2, 2, 2], SyncClass::Fsync);
        let mut journal = Journal::recover(&files, SyncClass::Fsync).unwrap().expect("state").journal;
        // Mid flight 1: after its first frame.
        let first_frame = {
            let bytes = std::fs::read(&files.log).unwrap();
            let at = starts[0] as usize;
            FRAME_HEADER_LEN + u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize
        };
        journal.rewrite_from(starts[0] + first_frame as u64, 1).unwrap();
        drop(journal);
        // The first kept frame, just past the header, is lost.
        overwrite(&files.log, LOG_HEADER_LEN as u64, &[0u8; 12]);
        assert!(
            Journal::recover_catalog_as(&files, SyncClass::Off, Some((512, 1)), FORMAT_VERSION).is_err(),
            "damage to a synced kept flight under later whole flights was cut"
        );
    }

    /// Review 2 #7: recovery's CRC work stays linear in the log: a 10k-page Commit torn at its start
    /// costs at most four times the tail's bytes in CRC, not a CRC at every offset.
    #[test]
    fn a_torn_large_commit_costs_a_linear_scan() {
        let dir = tempfile::TempDir::new().unwrap();
        let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
        let mut journal = Journal::create(&files, 512, SyncClass::Fsync).unwrap();
        let mut arena = Arena::new(512);
        journal.buffer(&Record::Fork { child: 1, parent: 0 }).unwrap();
        journal.take_flight(&mut arena, SyncClass::Fsync, false).unwrap().write().unwrap();
        let at = std::fs::metadata(&files.log).unwrap().len();
        let pages: Vec<(u32, Slot, u32)> = (0..10_000u32).map(|i| (i, i, i.wrapping_mul(2_654_435_761))).collect();
        journal.buffer(&Record::Commit { branch: 1, pages }).unwrap();
        journal.take_flight(&mut arena, SyncClass::Fsync, false).unwrap().write().unwrap();
        drop(journal);
        overwrite(&files.log, at, &[0u8; 8]);
        let tail = std::fs::metadata(&files.log).unwrap().len() - at;
        let before = SCAN_CRC_BYTES.load(Ordering::Relaxed);
        let recovered = Journal::recover(&files, SyncClass::Off).unwrap().expect("state");
        let crc = SCAN_CRC_BYTES.load(Ordering::Relaxed) - before;
        assert_eq!(forks(&recovered.records), vec![1]);
        assert!(crc <= 4 * tail, "recovery computed {crc} CRC bytes over a {tail}-byte tail");
    }

    /// Review 2 #11: a garbled tag byte in the LAST flight is a torn flight: cut, the flights before
    /// it kept. Mutant `skip_frame_crc` must fail it (the garbled record would be decoded).
    #[test]
    fn a_garbled_tag_in_the_last_flight_is_cut() {
        let dir = tempfile::TempDir::new().unwrap();
        let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
        let starts = flights(&files, 0, &[1, 1, 1], SyncClass::Fsync);
        let bytes = std::fs::read(&files.log).unwrap();
        let tag = bytes[starts[2] as usize + FRAME_HEADER_LEN];
        overwrite(&files.log, starts[2] + FRAME_HEADER_LEN as u64, &[tag ^ 0x40]);
        let recovered = Journal::recover(&files, SyncClass::Off).expect("a torn last flight").expect("state");
        assert_eq!(forks(&recovered.records), vec![1, 11]);
        drop(recovered);
        assert_eq!(std::fs::metadata(&files.log).unwrap().len(), starts[2]);
    }

    /// Review 2 #11: a record replaced by another whole, CRC-valid one is caught by its flight's
    /// checksum: in a middle flight (a synced flight after it) the store is refused; in the last
    /// flight it is cut. Mutant `skip_flight_crc` must fail it.
    #[test]
    fn a_replaced_record_fails_its_flights_checksum() {
        for (flight, refused) in [(1usize, true), (2, false)] {
            let dir = tempfile::TempDir::new().unwrap();
            let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
            let starts = flights(&files, 0, &[1, 1, 1], SyncClass::Fsync);
            // Same size, valid frame checksum, different record: Fork{child: 77}.
            let mut payload = Vec::new();
            Record::Fork { child: 77, parent: 0 }.encode(&mut payload);
            let mut frame = Vec::new();
            put_u32(&mut frame, payload.len() as u32);
            put_u32(&mut frame, crc32c::crc32c(&payload));
            frame.extend_from_slice(&payload);
            overwrite(&files.log, starts[flight], &frame);
            let got = Journal::recover(&files, SyncClass::Off);
            if refused {
                assert!(got.is_err(), "flight {flight}: a replaced record under a later synced flight was taken");
            } else {
                let got = got.unwrap().expect("state");
                assert_eq!(forks(&got.records), vec![1, 11], "flight {flight}: the replaced record was replayed");
            }
        }
    }

    /// Review 2 #11: a whole flight moved to where it did not begin is rejected: its end frame names
    /// its own length, which no longer reaches back to the last whole flight's end. Mutant
    /// `skip_flight_start` must fail it.
    #[test]
    fn a_flight_at_the_wrong_offset_is_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
        let starts = flights(&files, 0, &[2, 1], SyncClass::Fsync);
        let bytes = std::fs::read(&files.log).unwrap();
        // Flight 2 (one record and its end) moved back over the second record of flight 1.
        let second = {
            let at = starts[0] as usize;
            at + FRAME_HEADER_LEN + u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize
        };
        let flight2 = bytes[starts[1] as usize..].to_vec();
        let mut image = bytes[..second].to_vec();
        image.extend_from_slice(&flight2);
        std::fs::write(&files.log, &image).unwrap();
        let recovered = Journal::recover(&files, SyncClass::Off).expect("a torn flight").expect("state");
        assert_eq!(forks(&recovered.records), Vec::<u64>::new(), "a flight at the wrong offset was taken");
    }

    /// Review 2 #1(b): a log reset (a compaction's) is a new file renamed over the old one, never a
    /// truncation in place. Mutant `reset_in_place` must fail it.
    #[cfg(unix)]
    #[test]
    fn a_reset_log_is_a_new_file() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::TempDir::new().unwrap();
        let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
        let mut journal = Journal::create(&files, 512, SyncClass::Fsync).unwrap();
        let before = std::fs::metadata(&files.log).unwrap().ino();
        journal.reset_log(1).unwrap();
        assert_ne!(std::fs::metadata(&files.log).unwrap().ino(), before, "the log was reset in place");
    }

    /// Ruling 85a032f01's guard: a log and an arena on two devices are refused (one device's flush
    /// does not make the other's writes durable), and so is a device that cannot be read.
    #[cfg(unix)]
    #[test]
    fn the_log_and_the_arena_must_share_a_device() {
        assert!(one_device(Some(7), Some(7)).is_ok());
        assert!(one_device(Some(7), Some(8)).is_err(), "two devices were accepted");
        assert!(one_device(None, Some(8)).is_err(), "an unreadable device was accepted");
        let dir = tempfile::TempDir::new().unwrap();
        let a = File::create(dir.path().join("a")).unwrap();
        let b = File::create(dir.path().join("b")).unwrap();
        assert!(one_device(file_device(&a), file_device(&b)).is_ok(), "siblings read as two devices");
    }

    /// Review 2 #8: a flight's end frame is 17 bytes — a 9-byte payload: a tag that says whether the
    /// flight was synced, the flight's length, and its crc32c seeded with the log's nonce.
    #[test]
    fn an_end_frame_is_seventeen_bytes() {
        let dir = tempfile::TempDir::new().unwrap();
        let files = BranchFiles::for_db(dir.path().join("db").to_str().unwrap());
        let starts = flights(&files, 0, &[1], SyncClass::Fsync);
        let bytes = std::fs::read(&files.log).unwrap();
        let at = starts[0] as usize;
        let fork_len = FRAME_HEADER_LEN + u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        assert_eq!(bytes.len() - at - fork_len, 17, "the end frame's length");
        let end = &bytes[at + fork_len..];
        assert_eq!(u32::from_le_bytes(end[0..4].try_into().unwrap()), 9, "the end frame's payload length");
        assert_eq!(
            u32::from_le_bytes(end[9..13].try_into().unwrap()) as usize,
            fork_len,
            "the end frame names its flight's length"
        );
    }
}
