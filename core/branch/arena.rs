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
//!
//! # Pages readable without the arena's lock (F-K3, lane r11-k3-trunklock)
//!
//! A slot's bytes live in chunks that are installed once and never move or go away while any
//! holder of the arena's [`Chunks`] lives, so a thread holding a clone can copy a slot's page
//! without the lock that guards the arena's free list (FRS's chunk pattern, turso dd4215f3f). Who
//! may touch the bytes is the store's rule, not a lock: a slot is written only while no other
//! thread can reach it (see [`Chunks::read`]).

use std::cell::UnsafeCell;
use std::sync::{Arc, OnceLock};

use crate::turso_assert;

/// Index of a page-sized slot in the arena.
pub(crate) type Slot = u32;

/// Slots per allocation. Chunks are zero-filled through the allocator, so the OS backs a chunk with
/// memory only as its slots are touched; the chunk size bounds the granularity, not the footprint.
const SLOTS_PER_CHUNK: usize = 256;
/// Chunks per group, and groups: 2^9 * 2^8 chunks of 2^8 slots covers the store's 2^25 slots per
/// arena domain. Only the group table (2^8 empty cells) is allocated up front.
const CHUNKS_PER_GROUP: usize = 1 << 9;
const GROUPS: usize = 1 << 8;
/// Slots an arena can hand out.
pub(crate) const MAX_SLOTS: usize = SLOTS_PER_CHUNK * CHUNKS_PER_GROUP * GROUPS;

/// One chunk's pages. `UnsafeCell` because the bytes are written through a shared reference; the
/// store's rule (module doc) is what makes that sound.
struct Chunk(Box<[UnsafeCell<u8>]>);

// SAFETY: every access to a chunk's bytes goes through `Arena::page`/`Arena::page_mut` under the
// owner's lock, or through `Chunks::read`, whose callers read only a slot no thread writes.
unsafe impl Sync for Chunk {}
unsafe impl Send for Chunk {}

/// An arena's pages, shareable with readers that do not hold the arena's lock.
pub(crate) struct Chunks {
    page_size: usize,
    groups: Box<[OnceLock<Box<[OnceLock<Chunk>]>>]>,
}

impl Chunks {
    fn new(page_size: usize) -> Self {
        Self {
            page_size,
            groups: (0..GROUPS).map(|_| OnceLock::new()).collect(),
        }
    }

    /// The group and the chunk within it that hold `slot`.
    fn split(slot: Slot) -> (usize, usize) {
        let chunk = slot as usize / SLOTS_PER_CHUNK;
        (chunk / CHUNKS_PER_GROUP, chunk % CHUNKS_PER_GROUP)
    }

    /// Install the chunk holding `slot`, zero-filled, if it is not there yet.
    fn install(&self, slot: Slot) {
        let (g, c) = Self::split(slot);
        let group = self.groups[g].get_or_init(|| (0..CHUNKS_PER_GROUP).map(|_| OnceLock::new()).collect());
        group[c].get_or_init(|| {
            let bytes: Box<[u8]> = vec![0u8; SLOTS_PER_CHUNK * self.page_size].into_boxed_slice();
            // SAFETY: `UnsafeCell<u8>` has the same layout as `u8` (`repr(transparent)`).
            Chunk(unsafe { Box::from_raw(Box::into_raw(bytes) as *mut [UnsafeCell<u8>]) })
        });
    }

    /// The first byte of `slot`'s page; the chunk must be installed.
    fn base(&self, slot: Slot) -> *mut u8 {
        let (g, c) = Self::split(slot);
        let chunk = self.groups[g]
            .get()
            .and_then(|group| group[c].get())
            .expect("a slot the arena handed out lies in an installed chunk");
        let offset = (slot as usize % SLOTS_PER_CHUNK) * self.page_size;
        chunk.0[offset..offset + self.page_size].as_ptr() as *mut u8
    }

    /// Copy `slot`'s page into `out` without the arena's lock.
    ///
    /// # Safety
    /// `slot` must have been handed out by the arena these chunks belong to, its bytes must have
    /// been written before the caller learned of the slot (through a Release/Acquire pair), and no
    /// thread may write or release it until the copy returns.
    pub(crate) unsafe fn read(&self, slot: Slot, out: &mut [u8]) {
        turso_assert!(out.len() == self.page_size, "a page read into a buffer of the wrong size");
        std::ptr::copy_nonoverlapping(self.base(slot), out.as_mut_ptr(), self.page_size);
    }
}

pub(crate) struct Arena {
    page_size: usize,
    chunks: Arc<Chunks>,
    /// Slots below this have been handed out at least once.
    high_water: u32,
    free: Vec<Slot>,
    /// One bit per slot below `high_water`, set while the slot is on the free list. The list alone
    /// cannot answer "is this slot free" without a scan, and releasing an already-free slot must be
    /// caught AT the release — found later, it is two owners of one page and nothing says which.
    free_bits: Vec<u64>,
}

impl Arena {
    pub(crate) fn new(page_size: usize) -> Self {
        Self {
            page_size,
            chunks: Arc::new(Chunks::new(page_size)),
            high_water: 0,
            free: Vec::new(),
            free_bits: Vec::new(),
        }
    }

    pub(crate) fn page_size(&self) -> usize {
        self.page_size
    }

    /// The arena's pages, for reads without its lock (see [`Chunks::read`]).
    pub(crate) fn chunks(&self) -> Arc<Chunks> {
        self.chunks.clone()
    }

    pub(crate) fn alloc(&mut self) -> Slot {
        if let Some(slot) = self.free.pop() {
            self.set_free_bit(slot, false);
            return slot;
        }
        let slot = self.high_water;
        turso_assert!((slot as usize) < MAX_SLOTS, "the arena is out of slots");
        self.chunks.install(slot);
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
        self.set_free_bit(slot, true);
        self.free.push(slot);
    }

    pub(crate) fn is_free(&self, slot: Slot) -> bool {
        if slot >= self.high_water {
            return false;
        }
        self.free_bits[slot as usize / 64] & (1u64 << (slot % 64)) != 0
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

    pub(crate) fn page(&self, slot: Slot) -> &[u8] {
        self.locate(slot);
        // SAFETY: the slot is handed out and its chunk installed; the caller holds the owner's lock,
        // and a lock-free reader (`Chunks::read`) never reads a slot that is being written.
        unsafe { std::slice::from_raw_parts(self.chunks.base(slot), self.page_size) }
    }

    pub(crate) fn page_mut(&mut self, slot: Slot) -> &mut [u8] {
        self.locate(slot);
        // SAFETY: as `page`; `&mut self` means the owner's lock is held, and no lock-free reader
        // can reach a slot while it is written (the store's rule, module doc).
        unsafe { std::slice::from_raw_parts_mut(self.chunks.base(slot), self.page_size) }
    }

    fn locate(&self, slot: Slot) {
        turso_assert!(slot < self.high_water, "arena slot out of range");
        turso_assert!(!self.is_free(slot), "access to a free arena slot");
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
