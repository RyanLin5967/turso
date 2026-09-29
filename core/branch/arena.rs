//! The branch arena: page-sized slots handed out lowest-address first.
//!
//! A slot is a branch's own copy of one page — either the page's current version on a branch that
//! wrote it, or a superseded version kept alive because a child forked while it was current. The
//! arena does not know which; [`super::store`] owns that bookkeeping and the arena only answers
//! "give me a page" and "take this page back".
//!
//! ⚠ **The arena is volatile.** Slots live in memory and there is no persisted map from branch to
//! slot, so branches do not survive a restart. That is a scope line, not an oversight: the question
//! this fork exists to answer is how branch management behaves at 10^6 live branches on a real
//! engine's pager, and ferrodb's own D79 showed that making the branch map durable is a separate
//! wall with its own curve. Mixing the two would hide which one a slope belongs to.
//!
//! **Memory after a peak (F9, lane r12-f9-shrink).** Before F9 the arena kept every chunk it ever
//! allocated and a LIFO free list as long as the peak, so after a grow to 10^6 branches and a shrink
//! to 10^3 it still held every page it had handed out. F9 returns that memory with three published
//! mechanisms and changes nothing a caller can see:
//! * **Slab reclaim** (Bonwick, USENIX Summer 1994): a chunk is an anonymous mapping, unmapped when
//!   its last slot is released. One empty chunk stays mapped as a spare, the lowest one, so a count
//!   oscillating across a chunk boundary does not map and unmap per operation (jemalloc 3's
//!   `arena->spare`).
//! * **Purge of free pages** (jemalloc's dirty-page purge, with its pre-decay ratio rule
//!   `opt.lg_dirty_mult = 3`): a unit is one OS page (or one slot, where a slot spans whole OS
//!   pages). A unit whose slots are all free joins a FIFO, and while the FIFO holds more units than
//!   `max(units per chunk, active units / 8)` its oldest is handed back to the OS (macOS
//!   `MADV_FREE_REUSABLE` then `MADV_FREE_REUSE` before reuse, as libmalloc does; elsewhere
//!   `MADV_DONTNEED`). Every caller writes a whole page into a slot before reading it, so a purged
//!   unit's contents never matter.
//! * **Address-ordered first fit** from the free bitmap, with one bit per chunk saying whether it
//!   has a free slot (the lowest-address policy of Bonwick's slabs and jemalloc's runs), in place
//!   of the LIFO free list: live slots gather at low addresses, so the high chunks empty out and
//!   are unmapped. When the top chunks are all unmapped, `high_water` drops to the mapped top and
//!   the bookkeeping vectors shrink, with hysteresis (capacity above 4x length shrinks to 2x).
//!
//! The free list's doubling copies under the store mutex (sweep S5) are gone with the list. A
//! release still does bounded work under the mutex: it may grow the FIFO (at most
//! `max(units per chunk, active units / 8) + 1` entries of 4 bytes), `madvise` units past the bound,
//! `munmap` a chunk, and, when a trim shrinks the bookkeeping vectors, copy them (bytes per slot of
//! the peak / 8, once per shrink, with hysteresis).

use crate::turso_assert;
use std::collections::VecDeque;
use std::ptr::NonNull;

/// Index of a page-sized slot in the arena.
pub(crate) type Slot = u32;

/// Slots per chunk. A chunk is mapped when its first slot is needed and the OS backs it with
/// memory only as its slots are written; it is unmapped when its last slot is released.
const SLOTS_PER_CHUNK: usize = 256;
const WORDS_PER_CHUNK: usize = SLOTS_PER_CHUNK / 64;

/// jemalloc 4's `opt.lg_dirty_mult`: free units not yet handed back may reach active units / 2^3.
const LG_DIRTY_MULT: u32 = 3;

/// Bytes the arena holds, by part (see `BranchResident`). Observation only.
#[derive(Default, Clone, Copy)]
pub(crate) struct ArenaBytes {
    pub(crate) chunks_mapped: usize,
    pub(crate) resident: usize,
    pub(crate) meta: usize,
    pub(crate) purges: u64,
    pub(crate) reuses: u64,
    pub(crate) chunk_maps: u64,
    pub(crate) chunk_unmaps: u64,
}

/// One chunk's memory, owned like a `Box<[u8]>` and returned to the OS by `Drop`.
struct ChunkMem {
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: the memory is owned by this value alone; the arena hands out references to it only
// through `&self` and `&mut self`, as a `Box<[u8]>` would.
unsafe impl Send for ChunkMem {}
unsafe impl Sync for ChunkMem {}

impl ChunkMem {
    #[cfg(unix)]
    fn new(len: usize) -> Self {
        // SAFETY: an anonymous private mapping; nothing else refers to it.
        let p = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANON,
                -1,
                0,
            )
        };
        match NonNull::new(p as *mut u8) {
            Some(ptr) if p != libc::MAP_FAILED => Self { ptr, len },
            _ => std::alloc::handle_alloc_error(Self::layout(len)),
        }
    }

    #[cfg(not(unix))]
    fn new(len: usize) -> Self {
        // SAFETY: the layout has a non-zero size.
        let p = unsafe { std::alloc::alloc_zeroed(Self::layout(len)) };
        match NonNull::new(p) {
            Some(ptr) => Self { ptr, len },
            None => std::alloc::handle_alloc_error(Self::layout(len)),
        }
    }

    fn layout(len: usize) -> std::alloc::Layout {
        std::alloc::Layout::from_size_align(len, 4096).expect("chunk layout")
    }

    /// Hand `[offset, offset + len)` back to the OS. Its contents become undefined.
    #[cfg(unix)]
    fn purge(&self, offset: usize, len: usize) {
        #[cfg(target_os = "macos")]
        let advice = libc::MADV_FREE_REUSABLE;
        #[cfg(not(target_os = "macos"))]
        let advice = libc::MADV_DONTNEED;
        // In tests, poison the range first: macOS keeps a purged page's bytes until the kernel
        // takes it, so without this a purge of the wrong range would go unseen by a content check.
        #[cfg(test)]
        // SAFETY: the range lies inside this mapping.
        unsafe {
            std::ptr::write_bytes(self.ptr.as_ptr().add(offset), 0xDB, len)
        };
        // SAFETY: the range lies inside this mapping and no live slot overlaps it (the caller
        // checks every slot of the unit is free).
        let rc = unsafe {
            libc::madvise(self.ptr.as_ptr().add(offset) as *mut libc::c_void, len, advice)
        };
        turso_assert!(rc == 0, "branch arena: madvise purge failed");
    }

    /// Take a purged range back into use. Only macOS needs it, to count the pages in the task's
    /// footprint again; elsewhere a purged page simply faults in zeroed.
    #[cfg(unix)]
    fn reuse(&self, offset: usize, len: usize) {
        #[cfg(target_os = "macos")]
        {
            // SAFETY: the range lies inside this mapping.
            let rc = unsafe {
                libc::madvise(
                    self.ptr.as_ptr().add(offset) as *mut libc::c_void,
                    len,
                    libc::MADV_FREE_REUSE,
                )
            };
            turso_assert!(rc == 0, "branch arena: madvise reuse failed");
        }
        #[cfg(not(target_os = "macos"))]
        let _ = (offset, len);
    }

    #[cfg(not(unix))]
    fn purge(&self, _offset: usize, _len: usize) {}

    #[cfg(not(unix))]
    fn reuse(&self, _offset: usize, _len: usize) {}
}

impl Drop for ChunkMem {
    fn drop(&mut self) {
        #[cfg(unix)]
        // SAFETY: unmaps exactly the mapping `new` made; no reference to it outlives `self`.
        unsafe {
            libc::munmap(self.ptr.as_ptr() as *mut libc::c_void, self.len);
        }
        #[cfg(not(unix))]
        // SAFETY: allocated by `new` with this layout.
        unsafe {
            std::alloc::dealloc(self.ptr.as_ptr(), Self::layout(self.len));
        }
    }
}

struct Chunk {
    /// `None` while unmapped: then every slot of the chunk below `high_water` is free.
    mem: Option<ChunkMem>,
    /// Slots of this chunk handed out and not released.
    live: u16,
}

fn os_page_size() -> usize {
    #[cfg(unix)]
    {
        // SAFETY: sysconf has no preconditions.
        let n = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if n > 0 {
            return n as usize;
        }
    }
    4096
}

fn bit(v: &[u64], i: usize) -> bool {
    v[i / 64] & (1u64 << (i % 64)) != 0
}

fn set_bit(v: &mut [u64], i: usize, on: bool) {
    let b = 1u64 << (i % 64);
    if on {
        v[i / 64] |= b;
    } else {
        v[i / 64] &= !b;
    }
}

/// Shrink `v`'s capacity to twice its length once it exceeds four times it (hysteresis, so a
/// length oscillating around a threshold does not reallocate per operation).
fn shrink_hysteresis<T>(v: &mut Vec<T>) {
    if v.capacity() > 64 && v.capacity() > 4 * v.len() {
        v.shrink_to(2 * v.len());
    }
}

pub(crate) struct Arena {
    page_size: usize,
    chunks: Vec<Chunk>,
    /// Slots below this have been handed out at least once since the chunk holding them was last
    /// trimmed away. Never above `chunks.len() * SLOTS_PER_CHUNK`.
    high_water: u32,
    /// One bit per slot of every chunk, set while the slot is below `high_water` and free. The
    /// free set is what first fit searches, and releasing an already-free slot must be caught AT
    /// the release — found later, it is two owners of one page and nothing says which.
    free_bits: Vec<u64>,
    free_count: usize,
    /// One bit per chunk, set while it has a free slot below `high_water`.
    chunk_has_free: Vec<u64>,
    /// No chunk below this has a free slot: first fit starts here.
    first_free_chunk: usize,
    /// Slots per purge unit, and whether a unit covers whole OS pages (else nothing is purged and
    /// a unit is a whole chunk).
    unit_slots: usize,
    purgeable: bool,
    /// Per unit: written since it was last mapped or purged; handed back to the OS since it was
    /// last used; on the `dirty` FIFO.
    resident_bits: Vec<u64>,
    purged_bits: Vec<u64>,
    listed_bits: Vec<u64>,
    /// Units that became all free, oldest first, each at most once (its listed bit).
    dirty: VecDeque<u32>,
    resident_units: usize,
    /// Units holding at least one slot in use.
    active_units: usize,
    /// The one empty chunk kept mapped, if any.
    spare: Option<usize>,
    purges: u64,
    reuses: u64,
    chunk_maps: u64,
    chunk_unmaps: u64,
    #[cfg(test)]
    trims: u64,
}

impl Arena {
    pub(crate) fn new(page_size: usize) -> Self {
        let os_page = os_page_size();
        let unit_slots = (os_page / page_size).max(1);
        // A unit must cover whole OS pages and fit in a chunk; the purge relies on both.
        let purgeable = unit_slots <= SLOTS_PER_CHUNK
            && (unit_slots * page_size) % os_page == 0
            && cfg!(unix);
        Self {
            page_size,
            chunks: Vec::new(),
            high_water: 0,
            free_bits: Vec::new(),
            free_count: 0,
            chunk_has_free: Vec::new(),
            first_free_chunk: 0,
            unit_slots: if purgeable { unit_slots } else { SLOTS_PER_CHUNK },
            purgeable,
            resident_bits: Vec::new(),
            purged_bits: Vec::new(),
            listed_bits: Vec::new(),
            dirty: VecDeque::new(),
            resident_units: 0,
            active_units: 0,
            spare: None,
            purges: 0,
            reuses: 0,
            chunk_maps: 0,
            chunk_unmaps: 0,
            #[cfg(test)]
            trims: 0,
        }
    }

    pub(crate) fn page_size(&self) -> usize {
        self.page_size
    }

    fn units_per_chunk(&self) -> usize {
        SLOTS_PER_CHUNK / self.unit_slots
    }

    fn unit_bytes(&self) -> usize {
        self.unit_slots * self.page_size
    }

    fn chunk_bytes(&self) -> usize {
        SLOTS_PER_CHUNK * self.page_size
    }

    pub(crate) fn alloc(&mut self) -> Slot {
        let slot = if self.free_count > 0 {
            let c = self.lowest_chunk_with_free();
            let base = c * WORDS_PER_CHUNK;
            let words = &self.free_bits[base..base + WORDS_PER_CHUNK];
            let slot = if super::mutant::on(1) {
                let i = words.iter().rposition(|&w| w != 0).expect("the chunk has a free slot");
                (base + i) * 64 + 63 - words[i].leading_zeros() as usize
            } else {
                let i = words.iter().position(|&w| w != 0).expect("the chunk has a free slot");
                (base + i) * 64 + words[i].trailing_zeros() as usize
            };
            set_bit(&mut self.free_bits, slot, false);
            self.free_count -= 1;
            if self.free_bits[base..base + WORDS_PER_CHUNK].iter().all(|&w| w == 0) {
                set_bit(&mut self.chunk_has_free, c, false);
            }
            self.first_free_chunk = c;
            slot as Slot
        } else {
            let slot = self.high_water;
            if slot as usize / SLOTS_PER_CHUNK == self.chunks.len() {
                self.push_chunk();
            }
            self.high_water += 1;
            slot
        };
        let c = slot as usize / SLOTS_PER_CHUNK;
        if self.chunks[c].mem.is_none() {
            self.chunks[c].mem = Some(ChunkMem::new(self.chunk_bytes()));
            self.chunk_maps += 1;
        }
        if self.spare == Some(c) {
            self.spare = None;
        }
        self.chunks[c].live += 1;
        let u = slot as usize / self.unit_slots;
        if self.unit_live(u) == 1 {
            self.active_units += 1;
        }
        if !bit(&self.resident_bits, u) {
            set_bit(&mut self.resident_bits, u, true);
            self.resident_units += 1;
            if bit(&self.purged_bits, u) {
                set_bit(&mut self.purged_bits, u, false);
                let off = (u % self.units_per_chunk()) * self.unit_bytes();
                self.chunks[c].mem.as_ref().expect("mapped above").reuse(off, self.unit_bytes());
                self.reuses += 1;
            }
        }
        slot
    }

    pub(crate) fn release(&mut self, slot: Slot) {
        turso_assert!(slot < self.high_water, "released a slot the arena never handed out");
        turso_assert!(!self.is_free(slot), "released an arena slot that was already free");
        let s = slot as usize;
        let c = s / SLOTS_PER_CHUNK;
        set_bit(&mut self.free_bits, s, true);
        if !super::mutant::on(10) {
            set_bit(&mut self.chunk_has_free, c, true);
        }
        self.free_count += 1;
        self.first_free_chunk = self.first_free_chunk.min(c);
        self.chunks[c].live -= 1;
        let u = s / self.unit_slots;
        if self.unit_live(u) == 0 {
            self.active_units -= 1;
            if self.purgeable && bit(&self.resident_bits, u) && !bit(&self.listed_bits, u) {
                set_bit(&mut self.listed_bits, u, true);
                self.dirty.push_back(u as u32);
            }
        }
        let empty = if super::mutant::on(3) {
            self.chunks[c].live <= 1
        } else {
            self.chunks[c].live == 0
        };
        if empty {
            self.chunk_emptied(c);
        }
        self.purge_past_bound();
    }

    /// Slots of unit `u` below `high_water` that are in use.
    fn unit_live(&self, u: usize) -> usize {
        let (lo, hi) = (u * self.unit_slots, (u + 1) * self.unit_slots);
        let hi = hi.min(self.high_water as usize);
        (lo..hi).filter(|&s| !bit(&self.free_bits, s)).count()
    }

    fn lowest_chunk_with_free(&self) -> usize {
        if super::mutant::on(1) {
            let w = self
                .chunk_has_free
                .iter()
                .rposition(|&w| w != 0)
                .expect("free_count > 0, so some chunk has a free slot");
            return w * 64 + 63 - self.chunk_has_free[w].leading_zeros() as usize;
        }
        let mut w = self.first_free_chunk / 64;
        let mut mask = !0u64 << (self.first_free_chunk % 64);
        loop {
            let x = self
                .chunk_has_free
                .get(w)
                .expect("free_count > 0, so some chunk has a free slot")
                & mask;
            if x != 0 {
                return w * 64 + x.trailing_zeros() as usize;
            }
            w += 1;
            mask = !0;
        }
    }

    fn push_chunk(&mut self) {
        self.chunks.push(Chunk { mem: None, live: 0 });
        let n = self.chunks.len();
        self.free_bits.resize(n * WORDS_PER_CHUNK, 0);
        self.chunk_has_free.resize(n.div_ceil(64), 0);
        let units = (n * self.units_per_chunk()).div_ceil(64);
        self.resident_bits.resize(units, 0);
        self.purged_bits.resize(units, 0);
        self.listed_bits.resize(units, 0);
    }

    /// Chunk `c` has no slot in use: keep the lower of it and the spare mapped, unmap the other,
    /// then trim unmapped chunks off the top.
    fn chunk_emptied(&mut self, c: usize) {
        match self.spare {
            None => self.spare = Some(c),
            Some(s) if s == c => {}
            Some(s) => {
                let (keep, drop) = if c < s { (c, s) } else { (s, c) };
                self.unmap(drop);
                self.spare = Some(keep);
            }
        }
        self.trim();
    }

    fn unmap(&mut self, c: usize) {
        self.chunks[c].mem = None;
        self.chunk_unmaps += 1;
        if super::mutant::on(11) {
            return;
        }
        let upc = self.units_per_chunk();
        for u in c * upc..(c + 1) * upc {
            if bit(&self.resident_bits, u) {
                set_bit(&mut self.resident_bits, u, false);
                self.resident_units -= 1;
            }
            // A fresh mapping reads as zeros and needs no reuse call. The listed bit stays with
            // its FIFO entry, which the purge skips once it finds the unit not resident.
            set_bit(&mut self.purged_bits, u, false);
        }
    }

    /// Drop the unmapped chunks at the top: their slots are all free, so `high_water` can fall to
    /// the top of the highest chunk still mapped, and every per-slot and per-unit vector with it.
    fn trim(&mut self) {
        let mut n = self.chunks.len();
        while n > 0 && self.chunks[n - 1].mem.is_none() {
            n -= 1;
        }
        if super::mutant::on(7) && n > 0 && n < self.chunks.len() {
            n -= 1;
        }
        if n == self.chunks.len() {
            return;
        }
        #[cfg(test)]
        {
            self.trims += 1;
        }
        let hw = (self.high_water as usize).min(n * SLOTS_PER_CHUNK);
        // The chunks dropped are unmapped, so every slot of them below `high_water` is free: the
        // count is arithmetic, and a trim costs O(chunks dropped), not O(slots).
        #[cfg(test)]
        assert_eq!(
            (hw..self.high_water as usize)
                .filter(|&s| bit(&self.free_bits, s))
                .count(),
            self.high_water as usize - hw,
            "a trimmed chunk held a slot in use"
        );
        self.free_count -= self.high_water as usize - hw;
        self.high_water = hw as u32;
        self.chunks.truncate(n);
        self.free_bits.truncate(n * WORDS_PER_CHUNK);
        self.chunk_has_free.truncate(n.div_ceil(64));
        if n % 64 != 0 {
            if let Some(last) = self.chunk_has_free.last_mut() {
                *last &= (1u64 << (n % 64)) - 1;
            }
        }
        let units = n * self.units_per_chunk();
        let words = units.div_ceil(64);
        for v in [
            &mut self.resident_bits,
            &mut self.purged_bits,
            &mut self.listed_bits,
        ] {
            v.truncate(words);
            if units % 64 != 0 {
                if let Some(last) = v.last_mut() {
                    *last &= (1u64 << (units % 64)) - 1;
                }
            }
        }
        self.dirty.retain(|&u| (u as usize) < units);
        if self.spare.is_some_and(|s| s >= n) {
            self.spare = None;
        }
        self.first_free_chunk = self.first_free_chunk.min(n);
        shrink_hysteresis(&mut self.chunks);
        shrink_hysteresis(&mut self.free_bits);
        shrink_hysteresis(&mut self.chunk_has_free);
        shrink_hysteresis(&mut self.resident_bits);
        shrink_hysteresis(&mut self.purged_bits);
        shrink_hysteresis(&mut self.listed_bits);
        if self.dirty.capacity() > 64 && self.dirty.capacity() > 4 * self.dirty.len() {
            self.dirty.shrink_to(2 * self.dirty.len());
        }
    }

    /// jemalloc's ratio rule: while more units wait on the FIFO than max(units per chunk,
    /// active units / 8), hand the oldest back to the OS if it is still resident and all free.
    fn purge_past_bound(&mut self) {
        if super::mutant::on(6) {
            return;
        }
        let bound = self.units_per_chunk().max(self.active_units >> LG_DIRTY_MULT);
        while self.dirty.len() > bound {
            let u = self.dirty.pop_front().expect("longer than the bound") as usize;
            set_bit(&mut self.listed_bits, u, false);
            let c = (u * self.unit_slots) / SLOTS_PER_CHUNK;
            let valid = super::mutant::on(8)
                || (bit(&self.resident_bits, u) && (super::mutant::on(2) || self.unit_live(u) == 0));
            if !valid {
                continue;
            }
            let Some(mem) = self.chunks[c].mem.as_ref() else {
                continue;
            };
            let off = (u % self.units_per_chunk()) * self.unit_bytes();
            mem.purge(off, self.unit_bytes());
            set_bit(&mut self.resident_bits, u, false);
            set_bit(&mut self.purged_bits, u, true);
            self.resident_units -= 1;
            self.purges += 1;
        }
        if self.dirty.capacity() > 64 && self.dirty.capacity() > 4 * self.dirty.len() {
            self.dirty.shrink_to(2 * self.dirty.len());
        }
    }

    pub(crate) fn is_free(&self, slot: Slot) -> bool {
        if slot >= self.high_water {
            return false;
        }
        bit(&self.free_bits, slot as usize)
    }

    pub(crate) fn in_use(&self) -> usize {
        self.high_water as usize - self.free_count
    }

    /// Every slot currently handed out, for membership checks.
    pub(crate) fn slots_in_use(&self) -> Vec<Slot> {
        (0..self.high_water).filter(|&s| !self.is_free(s)).collect()
    }

    pub(crate) fn free_count(&self) -> usize {
        self.free_count
    }

    /// `(high_water, free list capacity, free-bit words, chunks)`, for resident-size curves. There
    /// is no free list since F9, so its capacity is 0. Observation only.
    pub(crate) fn shape(&self) -> (usize, usize, usize, usize) {
        (
            self.high_water as usize,
            0,
            self.free_bits.len(),
            self.chunks.len(),
        )
    }

    /// Bytes held, by part. Observation only.
    pub(crate) fn bytes(&self) -> ArenaBytes {
        ArenaBytes {
            chunks_mapped: self.chunks.iter().filter(|c| c.mem.is_some()).count(),
            resident: self.resident_units * self.unit_bytes(),
            meta: self.chunks.capacity() * std::mem::size_of::<Chunk>()
                + (self.free_bits.capacity()
                    + self.chunk_has_free.capacity()
                    + self.resident_bits.capacity()
                    + self.purged_bits.capacity()
                    + self.listed_bits.capacity())
                    * std::mem::size_of::<u64>()
                + self.dirty.capacity() * std::mem::size_of::<u32>(),
            purges: self.purges,
            reuses: self.reuses,
            chunk_maps: self.chunk_maps,
            chunk_unmaps: self.chunk_unmaps,
        }
    }

    pub(crate) fn page(&self, slot: Slot) -> &[u8] {
        let (chunk, offset) = self.locate(slot);
        let mem = self.chunks[chunk].mem.as_ref().expect("a slot in use is in a mapped chunk");
        // SAFETY: the mapping holds SLOTS_PER_CHUNK pages and `offset` starts one of them; it is
        // unmapped only through `&mut self`, so it outlives the returned borrow.
        unsafe { std::slice::from_raw_parts(mem.ptr.as_ptr().add(offset), self.page_size) }
    }

    pub(crate) fn page_mut(&mut self, slot: Slot) -> &mut [u8] {
        let (chunk, offset) = self.locate(slot);
        let mem = self.chunks[chunk].mem.as_ref().expect("a slot in use is in a mapped chunk");
        // SAFETY: as in `page`, and `&mut self` makes this the only reference into the arena.
        unsafe { std::slice::from_raw_parts_mut(mem.ptr.as_ptr().add(offset), self.page_size) }
    }

    fn locate(&self, slot: Slot) -> (usize, usize) {
        turso_assert!(slot < self.high_water, "arena slot out of range");
        turso_assert!(!self.is_free(slot), "access to a free arena slot");
        let slot = slot as usize;
        (
            slot / SLOTS_PER_CHUNK,
            (slot % SLOTS_PER_CHUNK) * self.page_size,
        )
    }

    /// Every structural invariant, by brute force. Test only.
    #[cfg(test)]
    pub(crate) fn check(&self) {
        let hw = self.high_water as usize;
        let n = self.chunks.len();
        assert!(hw <= n * SLOTS_PER_CHUNK, "high_water above the chunks");
        if let Some(last) = self.chunks.last() {
            assert!(last.mem.is_some(), "an unmapped chunk left on top (trim)");
        }
        let free = (0..hw).filter(|&s| bit(&self.free_bits, s)).count();
        assert_eq!(free, self.free_count, "free_count");
        assert!(
            (hw..n * SLOTS_PER_CHUNK).all(|s| !bit(&self.free_bits, s)),
            "a free bit at or above high_water"
        );
        let mut lowest_free = None;
        for (c, ch) in self.chunks.iter().enumerate() {
            let slots = c * SLOTS_PER_CHUNK..((c + 1) * SLOTS_PER_CHUNK).min(hw);
            let live = slots.clone().filter(|&s| !bit(&self.free_bits, s)).count();
            let has_free = slots.clone().any(|s| bit(&self.free_bits, s));
            assert_eq!(live, ch.live as usize, "chunk {c}: live count");
            assert_eq!(bit(&self.chunk_has_free, c), has_free, "chunk {c}: has-free bit");
            if has_free && lowest_free.is_none() {
                lowest_free = Some(c);
            }
            assert_eq!(
                ch.mem.is_some(),
                live > 0 || self.spare == Some(c),
                "chunk {c}: mapped iff in use or the spare"
            );
        }
        if let Some(l) = lowest_free {
            assert!(self.first_free_chunk <= l, "first-fit start above the lowest free chunk");
        }
        let upc = self.units_per_chunk();
        let (mut resident, mut active) = (0, 0);
        for u in 0..n * upc {
            let c = u / upc;
            let live = self.unit_live(u);
            let res = bit(&self.resident_bits, u);
            if live > 0 {
                active += 1;
                assert!(res, "unit {u} holds a slot in use but is not resident (purged?)");
            }
            if res {
                resident += 1;
                assert!(self.chunks[c].mem.is_some(), "unit {u} resident in an unmapped chunk");
                assert!(!bit(&self.purged_bits, u), "unit {u} both resident and purged");
            }
        }
        assert_eq!(resident, self.resident_units, "resident_units");
        assert_eq!(active, self.active_units, "active_units");
        for u in 0..n * upc {
            if self.purgeable && bit(&self.resident_bits, u) && self.unit_live(u) == 0 {
                assert!(bit(&self.listed_bits, u), "unit {u} resident and all free but not listed");
            }
        }
        if self.purgeable {
            assert!(
                self.resident_units
                    <= self.active_units + upc.max(self.active_units >> LG_DIRTY_MULT),
                "resident units past active + the dirty bound"
            );
        }
        let mut listed: Vec<u32> = self.dirty.iter().copied().collect();
        let len = listed.len();
        listed.sort_unstable();
        listed.dedup();
        assert_eq!(listed.len(), len, "a unit twice on the FIFO");
        assert!(
            listed
                .iter()
                .all(|&u| (u as usize) < n * upc && bit(&self.listed_bits, u as usize)),
            "a FIFO entry without its listed bit"
        );
        let bits = (0..n * upc).filter(|&u| bit(&self.listed_bits, u)).count();
        assert_eq!(bits, len, "listed bits without a FIFO entry");
        if self.purgeable {
            assert!(
                len <= upc.max(self.active_units >> LG_DIRTY_MULT),
                "FIFO past its bound"
            );
        }
    }

    #[cfg(test)]
    fn mapped_chunks(&self) -> usize {
        self.chunks.iter().filter(|c| c.mem.is_some()).count()
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

    struct Rng(u64);
    impl Rng {
        fn below(&mut self, n: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            (self.0 % n as u64) as usize
        }
    }

    /// F9 T1: first fit, against a model that knows nothing of the arena's internals: `alloc` returns
    /// the lowest slot not in use (below `high_water` that is the lowest free slot; with none free it
    /// is `high_water`, which a trim may have lowered). Phases of growth and shrink, so the walk
    /// reaches unmaps and trims, with `check` every 64 steps.
    #[test]
    fn alloc_returns_the_lowest_slot_not_in_use() {
        let mut arena = Arena::new(4096);
        let mut live = std::collections::BTreeSet::new();
        let mut order: Vec<Slot> = Vec::new();
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for step in 0..40_000usize {
            // Grow 2:1 for 5,000 steps, then shrink 1:3, alternately.
            let grow = (step / 5_000) % 2 == 0;
            let alloc = order.is_empty()
                || if grow {
                    rng.below(3) != 0
                } else {
                    rng.below(4) == 0
                };
            if alloc {
                let want = (0..).find(|s| !live.contains(s)).expect("some slot is not in use");
                let got = arena.alloc();
                assert!(!live.contains(&got), "step {step}: slot {got} handed out twice");
                assert_eq!(got, want, "step {step}: alloc must take the lowest slot not in use");
                live.insert(got);
                order.push(got);
            } else {
                let s = order.swap_remove(rng.below(order.len()));
                arena.release(s);
                live.remove(&s);
            }
            if step % 64 == 0 {
                arena.check();
            }
        }
        arena.check();
        assert!(arena.chunk_unmaps > 0 && arena.trims > 0, "the walk never unmapped or trimmed");
    }

    /// F9 T2: a random walk with peaks and shrinks against a model. Every page in use keeps the
    /// bytes last written to it through purges, unmaps and trims; after every step every invariant
    /// of `check` holds (a slot in use sits in a mapped chunk and a resident unit, the FIFO stays
    /// within max(units per chunk, active / 8), mapped chunks are exactly those in use plus the
    /// spare). The walk must purge, unmap and trim at least once, or it proves nothing.
    #[test]
    fn pages_survive_purges_unmaps_and_trims_under_peaks_and_shrinks() {
        for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03] {
            let mut arena = Arena::new(4096);
            let mut rng = Rng(seed);
            let mut live: Vec<(Slot, u8)> = Vec::new();
            let mut stamp = 0u8;
            for round in 0..6 {
                // Grow to a peak of up to 20 chunks, then shrink to a handful of survivors
                // scattered over it, then churn.
                let peak = SLOTS_PER_CHUNK * (4 + rng.below(16));
                let keep = 1 + rng.below(40);
                let mut steps = 0;
                while live.len() < peak {
                    stamp = stamp.wrapping_add(1);
                    let s = arena.alloc();
                    arena.page_mut(s).fill(stamp);
                    live.push((s, stamp));
                    steps += 1;
                    if steps % 97 == 0 {
                        arena.check();
                    }
                }
                while live.len() > keep {
                    let (s, _) = live.swap_remove(rng.below(live.len()));
                    arena.release(s);
                    assert!(!arena.slots_in_use().contains(&s), "a released slot still in use");
                    arena.check();
                    assert_eq!(arena.in_use(), live.len(), "in_use against the model");
                }
                let mut want: Vec<Slot> = live.iter().map(|&(s, _)| s).collect();
                want.sort_unstable();
                assert_eq!(arena.slots_in_use(), want, "the slots in use against the model");
                for _ in 0..3 * SLOTS_PER_CHUNK {
                    let (s, _) = live.swap_remove(rng.below(live.len()));
                    arena.release(s);
                    assert!(!arena.slots_in_use().contains(&s), "a released slot still in use");
                    stamp = stamp.wrapping_add(1);
                    let n = arena.alloc();
                    arena.page_mut(n).fill(stamp);
                    live.push((n, stamp));
                    arena.check();
                    assert_eq!(arena.in_use(), live.len(), "in_use against the model");
                }
                for &(s, v) in &live {
                    assert!(
                        arena.page(s).iter().all(|&x| x == v),
                        "seed {seed:#x} round {round}: slot {s} lost its bytes"
                    );
                }
                assert_eq!(arena.in_use(), live.len());
                // The model's chunks in use, plus at most the spare.
                let mut chunks: Vec<usize> =
                    live.iter().map(|&(s, _)| s as usize / SLOTS_PER_CHUNK).collect();
                chunks.sort_unstable();
                chunks.dedup();
                assert!(arena.mapped_chunks() <= chunks.len() + 1, "more than one spare mapped");
            }
            assert!(arena.chunk_unmaps > 0, "the walk never unmapped a chunk");
            assert!(arena.trims > 0, "the walk never trimmed");
            if arena.purgeable {
                assert!(arena.purges > 0, "the walk never purged a unit");
                assert!(arena.reuses > 0, "the walk never reused a purged unit");
            }
        }
    }

    /// F9 T3: releasing everything leaves at most the spare mapped, and trims the rest away.
    #[test]
    fn a_full_release_returns_every_chunk_but_the_spare() {
        let mut arena = Arena::new(4096);
        let slots: Vec<Slot> = (0..SLOTS_PER_CHUNK * 40).map(|_| arena.alloc()).collect();
        assert!(arena.free_bits.capacity() > 64, "the peak must be big enough to test the shrink");
        for &s in &slots {
            arena.page_mut(s).fill(1);
        }
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        let mut order = slots.clone();
        for i in (1..order.len()).rev() {
            order.swap(i, rng.below(i + 1));
        }
        for s in order {
            arena.release(s);
        }
        arena.check();
        assert_eq!(arena.in_use(), 0);
        assert!(arena.mapped_chunks() <= 1, "{} chunks still mapped", arena.mapped_chunks());
        assert!(arena.chunks.len() <= 1, "{} chunks left after the trim", arena.chunks.len());
        assert!(arena.high_water as usize <= SLOTS_PER_CHUNK);
        assert!(arena.free_bits.capacity() <= 64, "free bits kept at the peak's size");
        let again: Vec<Slot> = (0..10).map(|_| arena.alloc()).collect();
        assert_eq!(again, (0..10).collect::<Vec<Slot>>(), "first fit from the bottom again");
        arena.check();
    }
}
