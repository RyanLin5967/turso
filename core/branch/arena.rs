//! The branch arena: page-sized slots handed out from a free list.
//!
//! A slot is a branch's own copy of one page — either the page's current version on a branch that
//! wrote it, or a superseded version kept alive because a child forked while it was current. The
//! arena does not know which; [`super::store`] owns that bookkeeping and the arena only answers
//! "give me a page" and "take this page back".
//!
//! Two backings. **Memory** (volatile branches): slots live in RAM and die with the process.
//! **File** (durable branches, UNBUILT when written): slot `i` is bytes `[i*page_size,
//! (i+1)*page_size)` of `<db>-branch-arena`. The file arena never records which slots are free:
//! at open, every slot below the file's high-water mark that the recovered state does not name is
//! free. So a slot written by a commit whose record never became durable is free after a crash by
//! construction — there is no free list on disk to leak from (reachability GC at mount, as an LFS
//! cleaner or `git fsck` does; LMDB, by contrast, persists its freelist).

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use super::journal::{fsync_file, open_rw, read_at, write_at};

/// r11-restart lane instrument: `R11_TRACE_SLOTS` prints every slot transition (observing only).
pub(crate) fn trace_slots() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("R11_TRACE_SLOTS").is_some())
}

/// r11-githost-attr lane instrument: `R11_UBC_PROBE` asks the OS, just before each file-arena slot
/// read or write, whether the slot's page is in the page cache ([`file_pages_resident`]). Off by
/// default: the probe costs three system calls inside the timed operation, so timed runs leave it
/// unset and counter runs set it (observing only).
fn ubc_probe() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("R11_UBC_PROBE").is_some())
}

/// Page-cache residency of each VM page covering bytes `[offset, offset + len)` of `file`, first
/// page first: mincore(2) on a read-only shared mapping of just those pages, made and dropped here.
/// Mapping touches no page, so asking does not change the answer. `None` when the OS refuses the
/// mapping or the query; callers count that apart, never as resident or not.
/// Blind spots: residency is per VM page (16 KiB on Apple silicon), not per slot; a page can be
/// evicted or read in between the answer and the caller's I/O; Apple targets only (elsewhere
/// `None`); and whether mincore on a file mapping reports the page cache on this OS at all is itself
/// a premise, fire-checked by the harness's `ubc-selftest` before any run that reads these counters
/// (r11-githost-attr PREREG section 4). An observing instrument for the r11-githost-attr lane;
/// nothing in the mechanism reads it.
#[doc(hidden)]
pub fn file_residency(file: &File, offset: u64, len: u64) -> Option<Vec<bool>> {
    if len == 0 {
        return Some(Vec::new());
    }
    // Apple only: the lane's box. Linux's mincore takes `unsigned char` and has no MINCORE_INCORE.
    #[cfg(target_vendor = "apple")]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: sysconf reads a constant.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page <= 0 {
            return None;
        }
        let page = page as u64;
        let start = offset / page * page;
        let end = offset.checked_add(len)?.div_ceil(page) * page;
        let span = usize::try_from(end - start).ok()?;
        let at = libc::off_t::try_from(start).ok()?;
        // SAFETY: a fresh read-only mapping the kernel places; the descriptor is owned by `file` and
        // open for the whole call; the mapping is never dereferenced and is unmapped below.
        let addr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                span,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                at,
            )
        };
        if addr == libc::MAP_FAILED {
            return None;
        }
        let pages = span / page as usize;
        let mut vec: Vec<libc::c_char> = vec![0; pages];
        // SAFETY: `addr..addr + span` is the mapping made above and `vec` holds one byte per page.
        let rc = unsafe { libc::mincore(addr as *const libc::c_void, span, vec.as_mut_ptr()) };
        // SAFETY: unmaps exactly the mapping made above.
        unsafe { libc::munmap(addr, span) };
        if rc != 0 {
            return None;
        }
        Some(
            vec.iter()
                .map(|&v| i32::from(v) & libc::MINCORE_INCORE != 0)
                .collect(),
        )
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        let _ = (file, offset);
        None
    }
}

/// [`file_residency`] counted: `(resident pages, pages)`.
#[doc(hidden)]
pub fn file_pages_resident(file: &File, offset: u64, len: u64) -> Option<(u64, u64)> {
    let map = file_residency(file, offset, len)?;
    Some((map.iter().filter(|&&r| r).count() as u64, map.len() as u64))
}

// ---------------------------------------------------------------------------------------------
// F-PW (r11-githost-attr PREREG A3): prewarm. With `R11_PREWARM` set, a file arena remembers the
// slots it read or wrote most recently, a clean close names them in `<db>-branch-hot`, and the next
// open reads them back into the OS page cache before serving anything: InnoDB's buffer-pool dump
// and load (page ids only, the most recently used pages; `innodb_buffer_pool_dump_at_shutdown` /
// `_load_at_startup`), in pg_prewarm's synchronous `read` mode, because this engine keeps no page
// cache of its own for the arena. Unset (the default), nothing here runs.

/// The recency set's cap when `R11_PREWARM_SLOTS` is unset: 64 MiB of 1 KiB pages.
const PREWARM_SLOTS_DEFAULT: usize = 65_536;
/// Named slots at most this many slots apart are read as one run: reading a short gap costs less
/// than a second request.
const PREWARM_BRIDGE_SLOTS: u64 = 32;
/// The longest run one read covers.
const PREWARM_RUN_BYTES: usize = 1 << 20;
const HOT_MAGIC: &[u8; 8] = b"R11HOT01";
/// magic, page size (u32), slot count (u32), crc32c of the slot list (u32); then the slots, u32 LE.
const HOT_HEADER: usize = 20;

/// The prewarm cap, or `None` when `R11_PREWARM` is unset. A cap that is not a positive integer is
/// an error, never a silent default: `BranchStore::open` refuses on it.
pub(crate) fn prewarm_cap() -> Result<Option<usize>> {
    if std::env::var_os("R11_PREWARM").is_none() {
        return Ok(None);
    }
    match std::env::var("R11_PREWARM_SLOTS") {
        Err(std::env::VarError::NotPresent) => Ok(Some(PREWARM_SLOTS_DEFAULT)),
        Ok(v) => match v.parse::<usize>() {
            Ok(n) if n > 0 => Ok(Some(n)),
            _ => Err(LimboError::InvalidArgument(format!(
                "R11_PREWARM_SLOTS={v:?} is not a positive integer"
            ))),
        },
        Err(e) => Err(LimboError::InvalidArgument(format!("R11_PREWARM_SLOTS: {e}"))),
    }
}

/// A file arena's recency set, when `R11_PREWARM` is set. Read at every file arena's creation; an
/// invalid cap yields none here, and the store's open has already refused it.
fn hot_from_env() -> Option<Mutex<HotSet>> {
    prewarm_cap().ok().flatten().map(|cap| Mutex::new(HotSet::new(cap)))
}

/// The slots a file arena read or wrote most recently: the analogue of InnoDB's LRU list. Each
/// touch stamps the slot with the next sequence number; at 2 x cap entries the set keeps the cap
/// most recent, so a touch costs O(1) amortized and the set never holds more than 2 x cap entries.
pub(crate) struct HotSet {
    cap: usize,
    seq: u64,
    last: HashMap<Slot, u64>,
}

impl HotSet {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            seq: 0,
            last: HashMap::new(),
        }
    }

    fn touch(&mut self, slot: Slot) {
        self.seq += 1;
        self.last.insert(slot, self.seq);
        if self.last.len() >= 2 * self.cap {
            self.trim();
        }
    }

    /// Keep the cap most recent. Sequence numbers are distinct, so the cap largest are exactly the
    /// entries at and after the cut once `select_nth_unstable` has partitioned around it.
    fn trim(&mut self) {
        if self.last.len() <= self.cap {
            return;
        }
        let mut by_age: Vec<(u64, Slot)> = self.last.iter().map(|(&s, &q)| (q, s)).collect();
        let cut = by_age.len() - self.cap;
        by_age.select_nth_unstable(cut);
        self.last = by_age[cut..].iter().map(|&(q, s)| (s, q)).collect();
    }

    /// The cap most recent slots, ascending.
    pub(crate) fn slots(&mut self) -> Vec<Slot> {
        self.trim();
        let mut slots: Vec<Slot> = self.last.keys().copied().collect();
        slots.sort_unstable();
        slots
    }
}

/// `<db>-branch-hot`'s bytes for `slots` (ascending).
pub(crate) fn encode_hot(page_size: usize, slots: &[Slot]) -> Vec<u8> {
    let mut body = Vec::with_capacity(slots.len() * 4);
    for s in slots {
        body.extend_from_slice(&s.to_le_bytes());
    }
    let mut out = Vec::with_capacity(HOT_HEADER + body.len());
    out.extend_from_slice(HOT_MAGIC);
    out.extend_from_slice(&(page_size as u32).to_le_bytes());
    out.extend_from_slice(&(slots.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(&body).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// The slots `<db>-branch-hot` names, or which check refused it: the magic, the page size (a list
/// written at another page size names other bytes), the count against the length, the crc (a torn
/// write), and strict ascending order.
pub(crate) fn decode_hot(
    bytes: &[u8],
    page_size: usize,
) -> std::result::Result<Vec<Slot>, &'static str> {
    if bytes.len() < HOT_HEADER || &bytes[..8] != HOT_MAGIC {
        return Err("magic");
    }
    let u32_at =
        |i: usize| u32::from_le_bytes([bytes[i], bytes[i + 1], bytes[i + 2], bytes[i + 3]]);
    if u32_at(8) as usize != page_size {
        return Err("page size");
    }
    let body = &bytes[HOT_HEADER..];
    if body.len() != u32_at(12) as usize * 4 {
        return Err("count");
    }
    if crc32c::crc32c(body) != u32_at(16) {
        return Err("crc");
    }
    let slots: Vec<Slot> = body
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect();
    if slots.windows(2).any(|w| w[0] >= w[1]) {
        return Err("order");
    }
    Ok(slots)
}

/// Write the hot list to `path` through a temporary file and a rename. Not fsynced: the list is a
/// hint and is never read as state. A torn file fails `decode_hot`; a stale one (a crash keeps the
/// previous close's) only warms pages that are no longer hot.
pub(crate) fn write_hot(path: &Path, page_size: usize, slots: &[Slot]) -> std::io::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = std::path::PathBuf::from(tmp);
    std::fs::write(&tmp, encode_hot(page_size, slots))?;
    std::fs::rename(&tmp, path)
}

/// Runs over the ascending `slots` as (first slot, slot count): a slot joins the run before it when
/// at most `bridge` slots lie between them and the run stays within `max_run` slots.
pub(crate) fn hot_runs(slots: &[Slot], bridge: u64, max_run: u64) -> Vec<(u64, u64)> {
    let mut runs: Vec<(u64, u64)> = Vec::new();
    for &s in slots {
        let s = u64::from(s);
        if let Some((first, len)) = runs.last_mut() {
            let end = *first + *len;
            if s >= end && s - end <= bridge && s - *first < max_run {
                *len = s - *first + 1;
                continue;
            }
        }
        runs.push((s, 1));
    }
    runs
}

/// What one prewarm read: the named slots it covered, bytes read (bridged gaps included) and runs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Prewarm {
    pub(crate) read_slots: u64,
    pub(crate) bytes: u64,
    pub(crate) ranges: u64,
}

/// r11-githost-attr lane instrument (observing only): the file arena's I/O, each timer paired with
/// its count, and, while `R11_UBC_PROBE` is set, the page-cache residency of each slot just before
/// it is read or written. A write at or past the file's end reads nothing back, so it is an append,
/// not a hit or a miss; a write into a page that is not resident makes the OS read that page first
/// (read-modify-write, INFERRED from the page cache's page granularity, not measured). Cumulative;
/// atomics because `read_slot` takes `&self`.
#[derive(Default)]
pub(crate) struct ArenaIo {
    pub(crate) reads: AtomicU64,
    pub(crate) read_ns: AtomicU64,
    pub(crate) writes: AtomicU64,
    pub(crate) write_ns: AtomicU64,
    pub(crate) syncs: AtomicU64,
    pub(crate) sync_ns: AtomicU64,
    pub(crate) ubc_read_hits: AtomicU64,
    pub(crate) ubc_read_misses: AtomicU64,
    pub(crate) ubc_read_unknown: AtomicU64,
    pub(crate) ubc_write_hits: AtomicU64,
    pub(crate) ubc_write_misses: AtomicU64,
    pub(crate) ubc_write_appends: AtomicU64,
    pub(crate) ubc_write_unknown: AtomicU64,
}

fn bump(c: &AtomicU64, by: u64) {
    c.fetch_add(by, Ordering::Relaxed);
}

fn ns_since(t: Instant) -> u64 {
    u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

impl ArenaIo {
    /// Count one probe answer into (hit, miss, unknown). A slot never spans two VM pages when the
    /// page size divides the VM page size, so "resident" means every page the slot touches is.
    fn note(answer: Option<(u64, u64)>, hit: &AtomicU64, miss: &AtomicU64, unknown: &AtomicU64) {
        match answer {
            Some((r, n)) if r == n => bump(hit, 1),
            Some(_) => bump(miss, 1),
            None => bump(unknown, 1),
        }
    }

    pub(crate) fn get(c: &AtomicU64) -> u64 {
        c.load(Ordering::Relaxed)
    }
}
use crate::sync::Mutex;
use crate::{turso_assert, LimboError, Result};

/// Index of a page-sized slot in the arena.
pub(crate) type Slot = u32;

/// Slots per allocation. Chunks are zero-filled through the allocator, so the OS backs a chunk with
/// memory only as its slots are touched; the chunk size bounds the granularity, not the footprint.
const SLOTS_PER_CHUNK: usize = 256;

enum Backing {
    Memory { chunks: Vec<Box<[u8]>> },
    File { file: File, dirty: bool },
}

pub(crate) struct Arena {
    page_size: usize,
    backing: Backing,
    /// Slots below this have been handed out at least once.
    high_water: u32,
    free: Vec<Slot>,
    /// One bit per slot below `high_water`, set while the slot is on the free list. The list alone
    /// cannot answer "is this slot free" without a scan, and releasing an already-free slot must be
    /// caught AT the release — found later, it is two owners of one page and nothing says which.
    free_bits: Vec<u64>,
    /// Slots handed out and not released. Equal to `high_water - free.len()` except in a catalog
    /// store, whose free slots are mostly in the catalog's free table, not in `free`.
    in_use: usize,
    /// r11-githost-attr instrument (observing only).
    pub(crate) io: ArenaIo,
    /// F-PW: the slots this file arena read or wrote most recently, while `R11_PREWARM` is set.
    hot: Option<Mutex<HotSet>>,
}

impl Arena {
    pub(crate) fn new(page_size: usize) -> Self {
        Self {
            page_size,
            backing: Backing::Memory { chunks: Vec::new() },
            high_water: 0,
            free: Vec::new(),
            free_bits: Vec::new(),
            in_use: 0,
            io: ArenaIo::default(),
            hot: None,
        }
    }

    /// A file-backed arena over `path`. `referenced` is every slot the recovered state names;
    /// every other slot below the file's high-water mark is free. A slot named twice, or named
    /// past the end of the file, is corruption and refuses the open.
    pub(crate) fn open_file(
        path: &Path,
        page_size: usize,
        truncate: bool,
        referenced: &[Slot],
    ) -> Result<Self> {
        let file = open_rw(path, truncate)?;
        let len = file
            .metadata()
            .map_err(|e| crate::error::io_error(e, "stat branch arena"))?
            .len();
        // A partly written last slot is a slot whose record never became durable.
        let high = len / page_size as u64;
        let high_water = u32::try_from(high)
            .map_err(|_| LimboError::Corrupt("branch arena is larger than 2^32 slots".into()))?;
        let words = (high_water as usize).div_ceil(64);
        let mut free_bits = vec![u64::MAX; words];
        if high_water % 64 != 0 {
            // Bits past the high-water mark are not slots.
            free_bits[words - 1] = (1u64 << (high_water % 64)) - 1;
        }
        for &slot in referenced {
            if slot >= high_water {
                return Err(LimboError::Corrupt(format!(
                    "branch store names arena slot {slot}, past the end of the arena file"
                )));
            }
            let word = &mut free_bits[slot as usize / 64];
            let bit = 1u64 << (slot % 64);
            if *word & bit == 0 {
                return Err(LimboError::Corrupt(format!(
                    "branch store names arena slot {slot} twice"
                )));
            }
            *word &= !bit;
        }
        let free: Vec<Slot> = (0..high_water)
            .rev()
            .filter(|&s| free_bits[s as usize / 64] & (1u64 << (s % 64)) != 0)
            .collect();
        Ok(Self {
            page_size,
            backing: Backing::File { file, dirty: false },
            high_water,
            in_use: high_water as usize - free.len(),
            free,
            free_bits,
            io: ArenaIo::default(),
            hot: hot_from_env(),
        })
    }

    /// A file-backed arena for a catalog store (r11-restart lane): no reachability sweep. The
    /// caller supplies the high-water mark, the count in use and the slots known free now; the
    /// catalog's free table holds the rest, which `add_free` moves in as they are needed. The free
    /// bitmap starts zeroed ("not known free"), and a zeroed allocation costs no page until
    /// touched.
    pub(crate) fn open_file_catalog(
        path: &Path,
        page_size: usize,
        high_water: u32,
        in_use: u64,
        free: Vec<Slot>,
    ) -> Result<Self> {
        let file = open_rw(path, false)?;
        let mut arena = Self {
            page_size,
            backing: Backing::File { file, dirty: false },
            high_water,
            free: Vec::with_capacity(free.len()),
            free_bits: vec![0; (high_water as usize).div_ceil(64)],
            in_use: in_use as usize,
            io: ArenaIo::default(),
            hot: hot_from_env(),
        };
        for slot in free {
            arena.add_free(slot);
        }
        Ok(arena)
    }

    pub(crate) fn high_water(&self) -> u32 {
        self.high_water
    }

    /// Put a slot that is free but not on the in-memory list (a catalog free row) on it. Not a
    /// release: the count in use does not change.
    pub(crate) fn add_free(&mut self, slot: Slot) {
        if trace_slots() {
            eprintln!("R11SLOT add_free {slot}");
        }
        turso_assert!(slot < self.high_water, "a free slot past the high-water mark");
        turso_assert!(!self.is_free(slot), "a slot added to the free list twice");
        self.set_free_bit(slot, true);
        self.free.push(slot);
    }

    /// The in-memory free list.
    pub(crate) fn free_list(&self) -> &[Slot] {
        &self.free
    }

    /// Empty the in-memory free list (a catalog checkpoint has written it to the catalog).
    pub(crate) fn drain_free(&mut self) {
        if trace_slots() {
            eprintln!("R11SLOT drain_free {:?}", self.free);
        }
        for slot in std::mem::take(&mut self.free) {
            self.set_free_bit(slot, false);
        }
    }

    pub(crate) fn page_size(&self) -> usize {
        self.page_size
    }

    pub(crate) fn is_file_backed(&self) -> bool {
        matches!(self.backing, Backing::File { .. })
    }

    pub(crate) fn alloc(&mut self) -> Slot {
        self.in_use += 1;
        if let Some(slot) = self.free.pop() {
            self.set_free_bit(slot, false);
            if trace_slots() {
                eprintln!("R11SLOT alloc {slot} (free list)");
            }
            return slot;
        }
        if trace_slots() {
            eprintln!("R11SLOT alloc {} (high water)", self.high_water);
        }
        let slot = self.high_water;
        if let Backing::Memory { chunks } = &mut self.backing {
            let chunk = slot as usize / SLOTS_PER_CHUNK;
            if chunk == chunks.len() {
                chunks.push(vec![0u8; SLOTS_PER_CHUNK * self.page_size].into_boxed_slice());
            }
        }
        // A file-backed arena grows when the slot is first written.
        self.high_water += 1;
        let words = (self.high_water as usize).div_ceil(64);
        if self.free_bits.len() < words {
            self.free_bits.resize(words, 0);
        }
        slot
    }

    pub(crate) fn release(&mut self, slot: Slot) {
        turso_assert!(slot < self.high_water, "released a slot the arena never handed out");
        turso_assert!(!self.is_free(slot), "released an arena slot that was already free");
        if trace_slots() {
            eprintln!("R11SLOT release {slot}");
        }
        self.set_free_bit(slot, true);
        self.free.push(slot);
        self.in_use -= 1;
    }

    pub(crate) fn is_free(&self, slot: Slot) -> bool {
        if slot >= self.high_water {
            return false;
        }
        self.free_bits[slot as usize / 64] & (1u64 << (slot % 64)) != 0
    }

    pub(crate) fn in_use(&self) -> usize {
        self.in_use
    }

    /// Every slot currently handed out, for membership checks.
    pub(crate) fn slots_in_use(&self) -> Vec<Slot> {
        (0..self.high_water).filter(|&s| !self.is_free(s)).collect()
    }

    pub(crate) fn free_count(&self) -> usize {
        self.free.len()
    }

    pub(crate) fn write_slot(&mut self, slot: Slot, bytes: &[u8]) -> Result<()> {
        turso_assert!(bytes.len() == self.page_size, "arena write of a wrong-sized page");
        let offset = self.check(slot) as u64 * self.page_size as u64;
        self.touch_hot(slot);
        if let Backing::File { file, dirty } = &mut self.backing {
            let io = &self.io;
            if ubc_probe() {
                match file.metadata() {
                    Ok(m) if offset >= m.len() => bump(&io.ubc_write_appends, 1),
                    Ok(_) => ArenaIo::note(
                        file_pages_resident(file, offset, bytes.len() as u64),
                        &io.ubc_write_hits,
                        &io.ubc_write_misses,
                        &io.ubc_write_unknown,
                    ),
                    Err(_) => bump(&io.ubc_write_unknown, 1),
                }
            }
            let t = Instant::now();
            write_at(file, bytes, offset)?;
            bump(&io.write_ns, ns_since(t));
            bump(&io.writes, 1);
            *dirty = true;
            return Ok(());
        }
        self.page_mut(slot).copy_from_slice(bytes);
        Ok(())
    }

    pub(crate) fn read_slot(&self, slot: Slot, out: &mut [u8]) -> Result<()> {
        turso_assert!(out.len() == self.page_size, "arena read into a wrong-sized buffer");
        let offset = self.check(slot) as u64 * self.page_size as u64;
        self.touch_hot(slot);
        if let Backing::File { file, .. } = &self.backing {
            let io = &self.io;
            if ubc_probe() {
                ArenaIo::note(
                    file_pages_resident(file, offset, out.len() as u64),
                    &io.ubc_read_hits,
                    &io.ubc_read_misses,
                    &io.ubc_read_unknown,
                );
            }
            let t = Instant::now();
            let read = read_at(file, out, offset);
            bump(&io.read_ns, ns_since(t));
            bump(&io.reads, 1);
            return read;
        }
        out.copy_from_slice(self.page(slot));
        Ok(())
    }

    /// Make every slot written so far durable. A no-op for the memory backing.
    pub(crate) fn sync(&mut self) -> Result<()> {
        if let Backing::File { file, dirty } = &mut self.backing {
            if *dirty {
                let t = Instant::now();
                fsync_file(file)?;
                bump(&self.io.sync_ns, ns_since(t));
                bump(&self.io.syncs, 1);
                *dirty = false;
            }
        }
        Ok(())
    }

    /// F-PW: start remembering the slots this arena reads and writes, whatever `R11_PREWARM` says
    /// (tests; the store's arenas take theirs from the environment at creation). A memory arena has
    /// no file to warm and ignores it.
    pub(crate) fn remember_hot(&mut self, cap: usize) {
        if self.is_file_backed() {
            self.hot = Some(Mutex::new(HotSet::new(cap)));
        }
    }

    fn touch_hot(&self, slot: Slot) {
        if let Some(hot) = &self.hot {
            hot.lock().touch(slot);
        }
    }

    /// F-PW: the remembered slots still handed out, ascending; `None` when this arena remembers
    /// none.
    pub(crate) fn hot_slots(&self) -> Option<Vec<Slot>> {
        let hot = self.hot.as_ref()?;
        let slots = hot.lock().slots();
        Some(
            slots
                .into_iter()
                .filter(|&s| s < self.high_water && !self.is_free(s))
                .collect(),
        )
    }

    /// F-PW: read `slots` (ascending) into the OS page cache and drop the bytes (pg_prewarm's
    /// `read` mode). The list is a hint from an earlier process, so slots at or past the high-water
    /// mark or the file's end are skipped, and nothing reads a slot's meaning: a slot freed or
    /// reused since costs one wasted read. The file arena's I/O counters are not moved (they count
    /// the operations').
    pub(crate) fn prewarm(&self, slots: &[Slot]) -> Result<Prewarm> {
        let Backing::File { file, .. } = &self.backing else {
            return Ok(Prewarm::default());
        };
        let len = file
            .metadata()
            .map_err(|e| crate::error::io_error(e, "stat branch arena"))?
            .len();
        let page = self.page_size as u64;
        let end = (len / page).min(u64::from(self.high_water));
        let keep: Vec<Slot> = slots.iter().copied().filter(|&s| u64::from(s) < end).collect();
        let max_run = (PREWARM_RUN_BYTES / self.page_size).max(1) as u64;
        let mut buf = vec![0u8; (max_run * page) as usize];
        let mut out = Prewarm {
            read_slots: keep.len() as u64,
            ..Prewarm::default()
        };
        for (first, n) in hot_runs(&keep, PREWARM_BRIDGE_SLOTS, max_run) {
            let bytes = (n * page) as usize;
            read_at(file, &mut buf[..bytes], first * page)?;
            out.bytes += bytes as u64;
            out.ranges += 1;
        }
        Ok(out)
    }

    /// The bytes of a slot in a MEMORY arena.
    pub(crate) fn page(&self, slot: Slot) -> &[u8] {
        let (chunk, offset) = self.locate(slot);
        let Backing::Memory { chunks } = &self.backing else {
            panic!("a file-backed arena has no in-memory page; use read_slot");
        };
        &chunks[chunk][offset..offset + self.page_size]
    }

    /// The bytes of a slot in a MEMORY arena, for writing.
    pub(crate) fn page_mut(&mut self, slot: Slot) -> &mut [u8] {
        let (chunk, offset) = self.locate(slot);
        let page_size = self.page_size;
        let Backing::Memory { chunks } = &mut self.backing else {
            panic!("a file-backed arena has no in-memory page; use write_slot");
        };
        &mut chunks[chunk][offset..offset + page_size]
    }

    fn check(&self, slot: Slot) -> Slot {
        turso_assert!(slot < self.high_water, "arena slot out of range");
        turso_assert!(!self.is_free(slot), "access to a free arena slot");
        slot
    }

    fn locate(&self, slot: Slot) -> (usize, usize) {
        let slot = self.check(slot) as usize;
        (
            slot / SLOTS_PER_CHUNK,
            (slot % SLOTS_PER_CHUNK) * self.page_size,
        )
    }

    fn set_free_bit(&mut self, slot: Slot, free: bool) {
        let word = &mut self.free_bits[slot as usize / 64];
        let bit = 1u64 << (slot % 64);
        if free {
            *word |= bit;
        } else {
            *word &= !bit;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_released_slot_is_reused_and_reads_as_free_only_while_released() {
        let mut arena = Arena::new(512);
        let a = arena.alloc();
        let b = arena.alloc();
        assert_ne!(a, b);
        arena.page_mut(a).fill(0xAA);
        arena.page_mut(b).fill(0xBB);
        assert!(arena.page(a).iter().all(|&x| x == 0xAA));
        assert!(arena.page(b).iter().all(|&x| x == 0xBB));
        assert_eq!(arena.in_use(), 2);

        arena.release(a);
        assert!(arena.is_free(a));
        assert!(!arena.is_free(b));
        assert_eq!(arena.in_use(), 1);

        let c = arena.alloc();
        assert_eq!(c, a, "the free list must be drained before the arena grows");
        assert!(!arena.is_free(c));
        assert_eq!(arena.in_use(), 2);
    }

    #[test]
    #[should_panic(expected = "already free")]
    fn a_double_release_is_caught_at_the_release() {
        let mut arena = Arena::new(512);
        let a = arena.alloc();
        arena.release(a);
        arena.release(a);
    }

    #[test]
    fn slots_span_chunks_without_aliasing() {
        let mut arena = Arena::new(64);
        let slots: Vec<Slot> = (0..(SLOTS_PER_CHUNK * 2 + 3)).map(|_| arena.alloc()).collect();
        for &s in &slots {
            arena.page_mut(s).fill((s % 251) as u8);
        }
        for &s in &slots {
            assert!(arena.page(s).iter().all(|&x| x == (s % 251) as u8), "slot {s} aliased");
        }
    }
    // F-PW (r11-githost-attr PREREG A3).

    #[test]
    fn a_hot_set_keeps_the_cap_most_recent_distinct_slots() {
        let mut hot = HotSet::new(3);
        for s in [1, 2, 3, 4, 2, 5] {
            hot.touch(s);
        }
        // Most recent first: 5, 2 (touched again after 4), 4.
        assert_eq!(hot.slots(), vec![2, 4, 5]);
        let mut hot = HotSet::new(3);
        for s in 0..100 {
            hot.touch(s);
            assert!(hot.last.len() < 2 * 3, "the set grew past 2 x cap");
        }
        assert_eq!(hot.slots(), vec![97, 98, 99]);
    }

    #[test]
    fn the_hot_list_round_trips_and_each_check_refuses() {
        let good = encode_hot(1024, &[3, 7, 9]);
        assert_eq!(decode_hot(&good, 1024), Ok(vec![3, 7, 9]));
        assert_eq!(decode_hot(&encode_hot(1024, &[]), 1024), Ok(vec![]));
        let mut magic = good.clone();
        magic[0] ^= 1;
        assert_eq!(decode_hot(&magic, 1024), Err("magic"));
        assert_eq!(decode_hot(&good[..HOT_HEADER - 1], 1024), Err("magic"));
        assert_eq!(decode_hot(&good, 512), Err("page size"));
        assert_eq!(decode_hot(&good[..good.len() - 1], 1024), Err("count"));
        let mut torn = good.clone();
        let last = torn.len() - 1;
        torn[last] ^= 1;
        assert_eq!(decode_hot(&torn, 1024), Err("crc"));
        assert_eq!(decode_hot(&encode_hot(1024, &[7, 3]), 1024), Err("order"));
        assert_eq!(decode_hot(&encode_hot(1024, &[3, 3]), 1024), Err("order"));
    }

    #[test]
    fn write_hot_replaces_the_list_through_a_rename() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("g.db-branch-hot");
        write_hot(&path, 512, &[1, 2]).unwrap();
        write_hot(&path, 512, &[5]).unwrap();
        assert_eq!(decode_hot(&std::fs::read(&path).unwrap(), 512), Ok(vec![5]));
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(
            names,
            vec![std::ffi::OsString::from("g.db-branch-hot")],
            "a temporary file was left"
        );
    }

    #[test]
    fn hot_runs_bridge_short_gaps_and_cap_run_length() {
        assert_eq!(hot_runs(&[0, 1, 2, 10, 50, 51], 32, 256), vec![(0, 11), (50, 2)]);
        assert_eq!(hot_runs(&[0, 1, 2, 3], 32, 2), vec![(0, 2), (2, 2)]);
        assert_eq!(hot_runs(&[0, 40], 32, 256), vec![(0, 1), (40, 1)]);
        let top = u64::from(u32::MAX);
        assert_eq!(hot_runs(&[u32::MAX - 1, u32::MAX], 32, 256), vec![(top - 1, 2)]);
        assert_eq!(hot_runs(&[], 32, 256), Vec::<(u64, u64)>::new());
    }

    #[test]
    fn prewarm_reads_named_slots_once_and_skips_slots_past_the_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut arena = Arena::open_file(&dir.path().join("a"), 512, true, &[]).unwrap();
        for _ in 0..4 {
            let s = arena.alloc();
            arena.write_slot(s, &[s as u8; 512]).unwrap();
        }
        // Handed out, never written: past the file's end.
        let unwritten = arena.alloc();
        let reads = ArenaIo::get(&arena.io.reads);
        let got = arena.prewarm(&[0, 2, 3, unwritten, 99]).unwrap();
        assert_eq!(got, Prewarm { read_slots: 3, bytes: 4 * 512, ranges: 1 });
        assert_eq!(ArenaIo::get(&arena.io.reads), reads, "prewarm moved the operations' counters");
        assert_eq!(Arena::new(512).prewarm(&[0]).unwrap(), Prewarm::default());
    }

    #[test]
    fn hot_slots_name_only_slots_still_handed_out() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut arena = Arena::open_file(&dir.path().join("a"), 512, true, &[]).unwrap();
        arena.remember_hot(8);
        let slots: Vec<Slot> = (0..3).map(|_| arena.alloc()).collect();
        for &s in &slots {
            arena.write_slot(s, &[1; 512]).unwrap();
        }
        let mut out = [0u8; 512];
        arena.read_slot(slots[0], &mut out).unwrap();
        arena.release(slots[1]);
        assert_eq!(arena.hot_slots(), Some(vec![slots[0], slots[2]]));
        let mut memory = Arena::new(512);
        memory.remember_hot(8);
        assert_eq!(memory.hot_slots(), None);
    }
}
