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
//!
//! Ported from r11-bigtxn (turso `7fcc8db5b`) onto the durable store: the free list, the free bits
//! and a memory arena's chunk table are [`Blocks`] arrays, so no alloc or release inside a store
//! hold moves them; and a write transaction fills the slots it reserved without the store mutex,
//! through a [`SlotPtr`] (the address of the slot's bytes, or the arena file and the slot's offset).

use std::alloc::{alloc_zeroed, dealloc, handle_alloc_error, Layout};
use std::fs::File;
use std::path::Path;
use std::ptr::NonNull;

use super::journal::{fsync_file, open_rw, read_at, write_at};

/// r11-restart lane instrument: `R11_TRACE_SLOTS` prints every slot transition (observing only).
pub(crate) fn trace_slots() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("R11_TRACE_SLOTS").is_some())
}
use crate::{turso_assert, LimboError, Result};

/// Index of a page-sized slot in the arena.
pub(crate) type Slot = u32;

/// Where one slot's bytes are, for filling the slot without the store mutex: their address in a
/// memory arena, or the arena file and the slot's offset in a file arena.
///
/// Sound only for the write transaction that reserved the slot and has not published it: until
/// the store maps it, no other party can name the slot, so nothing else reads or writes those
/// bytes. A memory arena never moves or frees a chunk while it lives (chunks are only appended),
/// which the holder guarantees by keeping the store alive; a file arena's slot is written with a
/// positional write on a shared descriptor, which touches no other slot's bytes.
#[derive(Clone)]
pub(crate) enum SlotPtr {
    Memory {
        ptr: NonNull<u8>,
        len: usize,
    },
    File {
        file: std::sync::Arc<File>,
        offset: u64,
        len: usize,
    },
}

// SAFETY: see the type's contract; the pointer is plain memory with no thread affinity, and a
// `File` is `Send + Sync`.
unsafe impl Send for SlotPtr {}
unsafe impl Sync for SlotPtr {}

impl SlotPtr {
    /// Copy `src` into the slot.
    ///
    /// # Safety
    /// The caller owns the slot under the type's contract.
    pub(crate) unsafe fn write(&self, src: &[u8]) -> Result<()> {
        match self {
            SlotPtr::Memory { ptr, len } => {
                turso_assert!(src.len() == *len, "slot write of the wrong length");
                std::ptr::copy_nonoverlapping(src.as_ptr(), ptr.as_ptr(), *len);
                Ok(())
            }
            SlotPtr::File { file, offset, len } => {
                turso_assert!(src.len() == *len, "slot write of the wrong length");
                write_at(file, src, *offset)
            }
        }
    }

    /// Copy the slot into `dst`.
    ///
    /// # Safety
    /// The caller owns the slot under the type's contract.
    pub(crate) unsafe fn read(&self, dst: &mut [u8]) -> Result<()> {
        match self {
            SlotPtr::Memory { ptr, len } => {
                turso_assert!(dst.len() == *len, "slot read of the wrong length");
                std::ptr::copy_nonoverlapping(ptr.as_ptr(), dst.as_mut_ptr(), *len);
                Ok(())
            }
            SlotPtr::File { file, offset, len } => {
                turso_assert!(dst.len() == *len, "slot read of the wrong length");
                read_at(file, dst, *offset)
            }
        }
    }
}

/// Slots per allocation. Chunks are zero-filled through the allocator, so the OS backs a chunk with
/// memory only as its slots are touched; the chunk size bounds the granularity, not the footprint.
const SLOTS_PER_CHUNK: usize = 256;

/// Entries per block of a [`Blocks`] array.
const BLOCK: usize = 1024;

/// A growable array kept as fixed blocks, so that growing it never moves the entries already in it.
/// The arena's free list, free bits and chunk table grow inside store-mutex holds; as flat `Vec`s
/// they doubled there and copied everything (the free list moved 2 MB at a million free slots,
/// stalling every branch). Only the outer vector of block pointers is ever reallocated.
struct Blocks<T: Copy + Default> {
    blocks: Vec<Box<[T; BLOCK]>>,
    len: usize,
}

impl<T: Copy + Default> Blocks<T> {
    fn new() -> Self {
        Self {
            blocks: Vec::new(),
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    /// Blocks are kept once allocated (the arena never shrinks), so a later push reuses them.
    fn push(&mut self, v: T) {
        if self.len == self.blocks.len() * BLOCK {
            self.blocks.push(Box::new([T::default(); BLOCK]));
        }
        self.blocks[self.len / BLOCK][self.len % BLOCK] = v;
        self.len += 1;
    }

    fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        let last = self.get(self.len - 1);
        self.len -= 1;
        Some(last)
    }

    fn get(&self, i: usize) -> T {
        turso_assert!(i < self.len, "block array index out of range");
        self.blocks[i / BLOCK][i % BLOCK]
    }

    fn get_mut(&mut self, i: usize) -> &mut T {
        turso_assert!(i < self.len, "block array index out of range");
        &mut self.blocks[i / BLOCK][i % BLOCK]
    }

    /// Forget every entry; the blocks stay for reuse.
    fn clear(&mut self) {
        self.len = 0;
    }

    fn iter(&self) -> impl Iterator<Item = T> + '_ {
        (0..self.len).map(|i| self.get(i))
    }

    /// Capacity of the outer vector of block pointers: the only allocation a push can move.
    fn outer_capacity(&self) -> usize {
        self.blocks.capacity()
    }
}

enum Backing {
    /// Zero-filled allocations of `SLOTS_PER_CHUNK * page_size` bytes, never moved or freed before
    /// the arena drops. Raw, so that a slice is only ever formed over one slot's bytes: a
    /// transaction may then fill a slot it owns (see [`SlotPtr`]) while the store mutex's holder
    /// works on other slots of the same chunk.
    Memory {
        chunks: Blocks<Option<NonNull<u8>>>,
    },
    /// Shared so that a [`SlotPtr`] can write a slot with no lock held. `unsynced` counts the
    /// bytes written since the last sync (observation only; `dirty` is what decides a sync).
    File {
        file: std::sync::Arc<File>,
        dirty: bool,
        unsynced: u64,
    },
}

pub(crate) struct Arena {
    page_size: usize,
    backing: Backing,
    /// Slots below this have been handed out at least once.
    high_water: u32,
    free: Blocks<Slot>,
    /// One bit per slot below `high_water`, set while the slot is on the free list. The list alone
    /// cannot answer "is this slot free" without a scan, and releasing an already-free slot must be
    /// caught AT the release — found later, it is two owners of one page and nothing says which.
    free_bits: Blocks<u64>,
    /// Slots handed out and not released. Equal to `high_water - free.len()` except in a catalog
    /// store, whose free slots are mostly in the catalog's free table, not in `free`.
    in_use: usize,
}

// SAFETY: the memory chunks are plain memory owned by the arena; access to them is governed by the
// store mutex, and by `SlotPtr`'s contract for a slot a transaction owns.
unsafe impl Send for Arena {}

impl Drop for Arena {
    fn drop(&mut self) {
        let layout = self.chunk_layout();
        if let Backing::Memory { chunks } = &self.backing {
            for chunk in chunks.iter() {
                let chunk = chunk.expect("every chunk index up to len is allocated");
                // SAFETY: allocated in `alloc` with this layout, and freed only here.
                unsafe { dealloc(chunk.as_ptr(), layout) };
            }
        }
    }
}

impl Arena {
    pub(crate) fn new(page_size: usize) -> Self {
        Self {
            page_size,
            backing: Backing::Memory {
                chunks: Blocks::new(),
            },
            high_water: 0,
            free: Blocks::new(),
            free_bits: Blocks::new(),
            in_use: 0,
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
        let mut bits = vec![u64::MAX; words];
        if high_water % 64 != 0 {
            // Bits past the high-water mark are not slots.
            bits[words - 1] = (1u64 << (high_water % 64)) - 1;
        }
        for &slot in referenced {
            if slot >= high_water {
                return Err(LimboError::Corrupt(format!(
                    "branch store names arena slot {slot}, past the end of the arena file"
                )));
            }
            let word = &mut bits[slot as usize / 64];
            let bit = 1u64 << (slot % 64);
            if *word & bit == 0 {
                return Err(LimboError::Corrupt(format!(
                    "branch store names arena slot {slot} twice"
                )));
            }
            *word &= !bit;
        }
        let mut free = Blocks::new();
        for s in (0..high_water).rev() {
            if bits[s as usize / 64] & (1u64 << (s % 64)) != 0 {
                free.push(s);
            }
        }
        let mut free_bits = Blocks::new();
        for w in bits {
            free_bits.push(w);
        }
        Ok(Self {
            page_size,
            backing: Backing::File {
                file: std::sync::Arc::new(file),
                dirty: false,
                unsynced: 0,
            },
            high_water,
            in_use: high_water as usize - free.len(),
            free,
            free_bits,
        })
    }

    /// A file-backed arena for a catalog store (r11-restart lane): no reachability sweep. The
    /// caller supplies the high-water mark, the count in use and the slots known free now; the
    /// catalog's free table holds the rest, which `add_free` moves in as they are needed. The free
    /// bitmap starts zeroed ("not known free").
    pub(crate) fn open_file_catalog(
        path: &Path,
        page_size: usize,
        high_water: u32,
        in_use: u64,
        free: Vec<Slot>,
    ) -> Result<Self> {
        let file = open_rw(path, false)?;
        let mut free_bits = Blocks::new();
        for _ in 0..(high_water as usize).div_ceil(64) {
            free_bits.push(0);
        }
        let mut arena = Self {
            page_size,
            backing: Backing::File {
                file: std::sync::Arc::new(file),
                dirty: false,
                unsynced: 0,
            },
            high_water,
            free: Blocks::new(),
            free_bits,
            in_use: in_use as usize,
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

    /// The in-memory free list, in stack order (the next `alloc` takes the last).
    pub(crate) fn free_list(&self) -> Vec<Slot> {
        self.free.iter().collect()
    }

    /// Empty the in-memory free list (a catalog checkpoint has written it to the catalog).
    pub(crate) fn drain_free(&mut self) {
        if trace_slots() {
            eprintln!("R11SLOT drain_free {:?}", self.free_list());
        }
        for i in 0..self.free.len() {
            let slot = self.free.get(i);
            self.set_free_bit(slot, false);
        }
        self.free.clear();
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
        let layout = self.chunk_layout();
        if let Backing::Memory { chunks } = &mut self.backing {
            let chunk = slot as usize / SLOTS_PER_CHUNK;
            if chunk == chunks.len() {
                // SAFETY: the layout has a nonzero size.
                let ptr = unsafe { alloc_zeroed(layout) };
                chunks.push(Some(
                    NonNull::new(ptr).unwrap_or_else(|| handle_alloc_error(layout)),
                ));
            }
        }
        // A file-backed arena grows when the slot is first written.
        self.high_water += 1;
        let words = (self.high_water as usize).div_ceil(64);
        while self.free_bits.len() < words {
            self.free_bits.push(0);
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
        self.free_bits.get(slot as usize / 64) & (1u64 << (slot % 64)) != 0
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
        if let Backing::File {
            file,
            dirty,
            unsynced,
        } = &mut self.backing
        {
            write_at(file, bytes, offset)?;
            *dirty = true;
            *unsynced += bytes.len() as u64;
            return Ok(());
        }
        self.page_mut(slot).copy_from_slice(bytes);
        Ok(())
    }

    pub(crate) fn read_slot(&self, slot: Slot, out: &mut [u8]) -> Result<()> {
        turso_assert!(out.len() == self.page_size, "arena read into a wrong-sized buffer");
        let offset = self.check(slot) as u64 * self.page_size as u64;
        if let Backing::File { file, .. } = &self.backing {
            return read_at(file, out, offset);
        }
        out.copy_from_slice(self.page(slot));
        Ok(())
    }

    /// The slot's writer, for its owning transaction to fill without the store mutex (see
    /// [`SlotPtr`]). A file arena's writes through it are not yet known to [`Arena::sync`]: the
    /// store calls [`Arena::note_unsynced_writes`] under the mutex before it buffers the record
    /// that names such a slot (rule 1: a record is durable only after the slots it names).
    pub(crate) fn slot_ptr(&self, slot: Slot) -> SlotPtr {
        let offset = self.check(slot) as u64 * self.page_size as u64;
        match &self.backing {
            Backing::File { file, .. } => SlotPtr::File {
                file: file.clone(),
                offset,
                len: self.page_size,
            },
            Backing::Memory { .. } => SlotPtr::Memory {
                ptr: self.slot_addr(slot),
                len: self.page_size,
            },
        }
    }

    /// `bytes` were written through [`SlotPtr`]s since the last sync: the next sync or flight
    /// must sync the arena file. A no-op for the memory backing.
    pub(crate) fn note_unsynced_writes(&mut self, bytes: u64) {
        if let Backing::File {
            dirty, unsynced, ..
        } = &mut self.backing
        {
            *dirty = true;
            *unsynced += bytes;
        }
    }

    /// For a group flush (r11-churn amendment 4): a duplicate of the arena file's descriptor if
    /// slots were written since the last sync, clearing the mark, with the bytes written since;
    /// the flight syncs it. `None` for the memory backing or a clean arena.
    pub(crate) fn take_dirty_file(&mut self) -> Result<Option<(File, u64)>> {
        if let Backing::File {
            file,
            dirty,
            unsynced,
        } = &mut self.backing
        {
            if *dirty {
                let dup = file
                    .try_clone()
                    .map_err(|e| crate::error::io_error(e, "dup branch arena"))?;
                *dirty = false;
                return Ok(Some((dup, std::mem::take(unsynced))));
            }
        }
        Ok(None)
    }

    /// Make every slot written so far durable; returns the bytes written since the last sync
    /// (observation only). A no-op for the memory backing.
    pub(crate) fn sync(&mut self) -> Result<u64> {
        if let Backing::File {
            file,
            dirty,
            unsynced,
        } = &mut self.backing
        {
            if *dirty {
                fsync_file(file)?;
                *dirty = false;
                return Ok(std::mem::take(unsynced));
            }
        }
        Ok(0)
    }

    /// Bytes of page memory a memory arena has allocated (whole chunks); 0 for a file arena.
    /// Observation only.
    pub(crate) fn chunk_bytes(&self) -> usize {
        match &self.backing {
            Backing::Memory { chunks } => chunks.len() * SLOTS_PER_CHUNK * self.page_size,
            Backing::File { .. } => 0,
        }
    }

    /// The capacities of the arena's growable vectors (free list, free bits, chunk table), for the
    /// store's realloc counter. Observation only.
    pub(crate) fn capacities(&self) -> [usize; 3] {
        let chunks = match &self.backing {
            Backing::Memory { chunks } => chunks.outer_capacity(),
            Backing::File { .. } => 0,
        };
        [
            self.free.outer_capacity(),
            self.free_bits.outer_capacity(),
            chunks,
        ]
    }

    /// The bytes of a slot in a MEMORY arena.
    pub(crate) fn page(&self, slot: Slot) -> &[u8] {
        let ptr = self.slot_addr(slot);
        // SAFETY: one slot's bytes inside a live chunk; `&self` rules out a `page_mut` alias, and a
        // slot a transaction owns is not one anybody resolves to.
        unsafe { std::slice::from_raw_parts(ptr.as_ptr(), self.page_size) }
    }

    /// The bytes of a slot in a MEMORY arena, for writing.
    pub(crate) fn page_mut(&mut self, slot: Slot) -> &mut [u8] {
        let ptr = self.slot_addr(slot);
        // SAFETY: as `page`, exclusively.
        unsafe { std::slice::from_raw_parts_mut(ptr.as_ptr(), self.page_size) }
    }

    fn slot_addr(&self, slot: Slot) -> NonNull<u8> {
        let (chunk, offset) = self.locate(slot);
        let Backing::Memory { chunks } = &self.backing else {
            panic!("a file-backed arena has no in-memory page; use read_slot or write_slot");
        };
        // SAFETY: `locate` bounds the offset by the chunk's size.
        unsafe {
            chunks
                .get(chunk)
                .expect("a located chunk is allocated")
                .add(offset)
        }
    }

    fn chunk_layout(&self) -> Layout {
        Layout::from_size_align(SLOTS_PER_CHUNK * self.page_size, 64).expect("a chunk's layout")
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
        let word = self.free_bits.get_mut(slot as usize / 64);
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

    #[test]
    fn a_mass_release_reuses_slots_last_in_first_out_across_segments() {
        let mut arena = Arena::new(8);
        let n = BLOCK * 2 + 7;
        let slots: Vec<Slot> = (0..n).map(|_| arena.alloc()).collect();
        for &s in &slots {
            arena.release(s);
        }
        assert_eq!(arena.free_count(), n);
        assert_eq!(arena.free.blocks.len(), 3, "blocks");
        for &s in slots.iter().rev() {
            assert_eq!(arena.alloc(), s, "the free list is a stack");
        }
        assert_eq!(arena.free_count(), 0);
        assert_eq!(arena.in_use(), n);
        assert_eq!(arena.alloc() as usize, n, "an empty free list grows the arena");
    }

    /// A slot filled through its `SlotPtr` reads back through the arena, in both backings; the
    /// file writer marks nothing dirty by itself (the store notes it before the record).
    #[test]
    fn a_slot_ptr_fills_the_slot_in_both_backings() {
        let mut mem = Arena::new(64);
        let s = mem.alloc();
        // SAFETY: the test owns the slot.
        unsafe { mem.slot_ptr(s).write(&[7u8; 64]).unwrap() };
        assert!(mem.page(s).iter().all(|&x| x == 7));

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("arena");
        let mut file = Arena::open_file(&path, 64, true, &[]).unwrap();
        let s = file.alloc();
        // SAFETY: the test owns the slot.
        unsafe { file.slot_ptr(s).write(&[9u8; 64]).unwrap() };
        let mut out = [0u8; 64];
        file.read_slot(s, &mut out).unwrap();
        assert!(out.iter().all(|&x| x == 9));
        assert!(file.take_dirty_file().unwrap().is_none(), "a slot-pointer write is not noted");
        file.note_unsynced_writes(64);
        let (_, bytes) = file.take_dirty_file().unwrap().expect("noted, the next flight syncs it");
        assert_eq!(bytes, 64, "the noted bytes travel with the flight");
    }
}
