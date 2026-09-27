//! The branch arena: page-sized slots handed out from a free list.
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

use std::alloc::{alloc_zeroed, dealloc, handle_alloc_error, Layout};
use std::ptr::NonNull;

use crate::turso_assert;

/// Index of a page-sized slot in the arena.
pub(crate) type Slot = u32;

/// The address of one slot's bytes, for filling a slot without the store mutex.
///
/// Sound only for the write transaction that allocated the slot and has not published it: until
/// the store maps it, no other party can name the slot, so nothing else reads or writes those
/// bytes; and the arena never moves or frees a chunk while it lives (chunks are only appended),
/// which the holder guarantees by keeping the store alive.
#[derive(Clone, Copy)]
pub(crate) struct SlotPtr {
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: see the type's contract; the pointer is plain memory with no thread affinity.
unsafe impl Send for SlotPtr {}
unsafe impl Sync for SlotPtr {}

impl SlotPtr {
    /// Copy `src` into the slot.
    ///
    /// # Safety
    /// The caller owns the slot under the type's contract.
    pub(crate) unsafe fn write(&self, src: &[u8]) {
        turso_assert!(src.len() == self.len, "slot write of the wrong length");
        std::ptr::copy_nonoverlapping(src.as_ptr(), self.ptr.as_ptr(), self.len);
    }

    /// Copy the slot into `dst`.
    ///
    /// # Safety
    /// The caller owns the slot under the type's contract.
    pub(crate) unsafe fn read(&self, dst: &mut [u8]) {
        turso_assert!(dst.len() == self.len, "slot read of the wrong length");
        std::ptr::copy_nonoverlapping(self.ptr.as_ptr(), dst.as_mut_ptr(), self.len);
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

    /// Capacity of the outer vector of block pointers: the only allocation a push can move.
    fn outer_capacity(&self) -> usize {
        self.blocks.capacity()
    }

    /// Length of the outer vector of block pointers (blocks allocated so far).
    fn outer_len(&self) -> usize {
        self.blocks.len()
    }
}

pub(crate) struct Arena {
    page_size: usize,
    /// Zero-filled allocations of `SLOTS_PER_CHUNK * page_size` bytes, never moved or freed before
    /// the arena drops. Raw, so that a slice is only ever formed over one slot's bytes: a
    /// transaction may then fill a slot it owns (see [`SlotPtr`]) while the store mutex's holder
    /// works on other slots of the same chunk.
    chunks: Blocks<Option<NonNull<u8>>>,
    /// Slots below this have been handed out at least once.
    high_water: u32,
    /// The free slots, as a stack.
    free: Blocks<Slot>,
    /// One bit per slot below `high_water`, set while the slot is on the free list. The list alone
    /// cannot answer "is this slot free" without a scan, and releasing an already-free slot must be
    /// caught AT the release — found later, it is two owners of one page and nothing says which.
    free_bits: Blocks<u64>,
}

// SAFETY: the chunks are plain memory owned by the arena; access to them is governed by the
// store mutex, and by `SlotPtr`'s contract for a slot a transaction owns.
unsafe impl Send for Arena {}

impl Drop for Arena {
    fn drop(&mut self) {
        let layout = self.chunk_layout();
        for i in 0..self.chunks.len() {
            let chunk = self.chunks.get(i).expect("every chunk index up to len is allocated");
            // SAFETY: allocated in `alloc` with this layout, and freed only here.
            unsafe { dealloc(chunk.as_ptr(), layout) };
        }
    }
}

impl Arena {
    pub(crate) fn new(page_size: usize) -> Self {
        Self {
            page_size,
            chunks: Blocks::new(),
            high_water: 0,
            free: Blocks::new(),
            free_bits: Blocks::new(),
        }
    }

    pub(crate) fn page_size(&self) -> usize {
        self.page_size
    }

    pub(crate) fn alloc(&mut self) -> Slot {
        if let Some(slot) = self.free.pop() {
            self.set_free_bit(slot, false);
            return slot;
        }
        let slot = self.high_water;
        let chunk = slot as usize / SLOTS_PER_CHUNK;
        if chunk == self.chunks.len() {
            let layout = self.chunk_layout();
            // SAFETY: the layout has a nonzero size.
            let ptr = unsafe { alloc_zeroed(layout) };
            self.chunks
                .push(Some(NonNull::new(ptr).unwrap_or_else(|| handle_alloc_error(layout))));
        }
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
        self.set_free_bit(slot, true);
        self.free.push(slot);
    }

    pub(crate) fn is_free(&self, slot: Slot) -> bool {
        if slot >= self.high_water {
            return false;
        }
        self.free_bits.get(slot as usize / 64) & (1u64 << (slot % 64)) != 0
    }

    pub(crate) fn in_use(&self) -> usize {
        self.high_water as usize - self.free.len()
    }

    /// Every slot currently handed out, for membership checks.
    pub(crate) fn slots_in_use(&self) -> Vec<Slot> {
        (0..self.high_water).filter(|&s| !self.is_free(s)).collect()
    }

    pub(crate) fn free_count(&self) -> usize {
        self.free.len()
    }

    /// Bytes of page memory the arena has allocated (whole chunks). Observation only.
    pub(crate) fn chunk_bytes(&self) -> usize {
        self.chunks.len() * SLOTS_PER_CHUNK * self.page_size
    }

    /// The capacities of the arena's growable vectors (free list, free bits, chunk table), for the
    /// store's realloc counter. Observation only.
    pub(crate) fn capacities(&self) -> [usize; 3] {
        [
            self.free.outer_capacity(),
            self.free_bits.outer_capacity(),
            self.chunks.outer_capacity(),
        ]
    }

    /// Free-list length and, for the free list, free bits and chunk table, the length and capacity
    /// of the outer vector of block pointers: the arena's only containers that can double.
    /// Observation only (r11-bigtxn amendment 7).
    pub(crate) fn shape(&self) -> [usize; 7] {
        [
            self.free.len(),
            self.free.outer_len(),
            self.free.outer_capacity(),
            self.free_bits.outer_len(),
            self.free_bits.outer_capacity(),
            self.chunks.outer_len(),
            self.chunks.outer_capacity(),
        ]
    }

    pub(crate) fn page(&self, slot: Slot) -> &[u8] {
        let ptr = self.slot_addr(slot);
        // SAFETY: one slot's bytes inside a live chunk; `&self` rules out a `page_mut` alias, and a
        // slot a transaction owns is not one anybody resolves to.
        unsafe { std::slice::from_raw_parts(ptr.as_ptr(), self.page_size) }
    }

    pub(crate) fn page_mut(&mut self, slot: Slot) -> &mut [u8] {
        let ptr = self.slot_addr(slot);
        // SAFETY: as `page`, exclusively.
        unsafe { std::slice::from_raw_parts_mut(ptr.as_ptr(), self.page_size) }
    }

    /// The slot's address, for its owning transaction to fill without the store mutex.
    pub(crate) fn slot_ptr(&self, slot: Slot) -> SlotPtr {
        SlotPtr {
            ptr: self.slot_addr(slot),
            len: self.page_size,
        }
    }

    fn slot_addr(&self, slot: Slot) -> NonNull<u8> {
        let (chunk, offset) = self.locate(slot);
        // SAFETY: `locate` bounds the offset by the chunk's size.
        unsafe {
            self.chunks
                .get(chunk)
                .expect("a located chunk is allocated")
                .add(offset)
        }
    }

    fn chunk_layout(&self) -> Layout {
        Layout::from_size_align(SLOTS_PER_CHUNK * self.page_size, 64).expect("a chunk's layout")
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
}
