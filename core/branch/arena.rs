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
}
