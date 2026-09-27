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

use crate::turso_assert;

/// Index of a page-sized slot in the arena.
pub(crate) type Slot = u32;

/// Slots the slot table grows by at a time.
const SLOTS_PER_CHUNK: usize = 256;

/// One slot's page: its own shared allocation, so a reader can hold a version's bytes by reference
/// (FS11) instead of copying them.
type SlotBytes = crate::sync::Arc<crate::alloc::DynBoxedSlice<u8>>;

pub(crate) struct Arena {
    page_size: usize,
    /// Each slot's page (r11-sessions FS11; before it, 256-slot chunks). `None` only for a free slot
    /// whose bytes a reader still held when it was released: it gets a fresh page when reused.
    /// A slot's bytes are written only while the arena holds the sole reference: a write to bytes
    /// a reader holds first gives the slot a fresh copy (`page_mut`), so a reader's version never
    /// changes under it.
    slots: Vec<Option<SlotBytes>>,
    /// FS11: writes that found a reader holding the slot's bytes and gave the slot a fresh copy.
    unshared_writes: u64,
    /// Slots below this have been handed out at least once.
    high_water: u32,
    free: Vec<Slot>,
    /// One bit per slot below `high_water`, set while the slot is on the free list. The list alone
    /// cannot answer "is this slot free" without a scan, and releasing an already-free slot must be
    /// caught AT the release — found later, it is two owners of one page and nothing says which.
    free_bits: Vec<u64>,
    /// FS9B (r11-sessions): shared copies of slots that descendants read through their inherited
    /// page map, keyed by the slot. A slot names one (owner, page, version) until it is written or
    /// released: while a descendant that inherited it lives, its owner retains it and writes a new
    /// slot instead; once none does, the owner may rewrite it in place, and a later child inherits
    /// the same slot. So `page_mut` (every write) and `release` both drop the clone.
    clones: std::collections::HashMap<Slot, crate::sync::Arc<crate::alloc::DynBoxedSlice<u8>>>,
}

impl Arena {
    pub(crate) fn new(page_size: usize) -> Self {
        Self {
            page_size,
            slots: Vec::new(),
            unshared_writes: 0,
            high_water: 0,
            free: Vec::new(),
            free_bits: Vec::new(),
            clones: std::collections::HashMap::new(),
        }
    }

    /// FS9B: the shared copy of `slot`, made on first use; `true` when this call made it.
    pub(crate) fn shared_clone(
        &mut self,
        slot: Slot,
    ) -> (crate::sync::Arc<crate::alloc::DynBoxedSlice<u8>>, bool) {
        if let Some(bytes) = self.clones.get(&slot) {
            return (bytes.clone(), false);
        }
        let bytes = crate::sync::Arc::new(self.page(slot).to_vec().into_boxed_slice());
        self.clones.insert(slot, bytes.clone());
        (bytes, true)
    }

    /// FS9B: slots currently cloned for sharing.
    pub(crate) fn clone_count(&self) -> usize {
        self.clones.len()
    }

    /// FS11: `slot`'s bytes by reference. The holder keeps this version: a later write to the slot
    /// goes to a fresh copy (`page_mut`), and a release leaves the bytes to the holder.
    pub(crate) fn shared_ref(&self, slot: Slot) -> SlotBytes {
        turso_assert!(slot < self.high_water, "arena slot out of range");
        turso_assert!(!self.is_free(slot), "access to a free arena slot");
        self.slots[slot as usize]
            .as_ref()
            .expect("a slot in use has its page")
            .clone()
    }

    /// FS11: writes that gave a slot a fresh copy because a reader held its bytes.
    pub(crate) fn unshared_writes(&self) -> u64 {
        self.unshared_writes
    }

    fn fresh_page(&self) -> SlotBytes {
        crate::sync::Arc::new(vec![0u8; self.page_size].into_boxed_slice())
    }

    pub(crate) fn page_size(&self) -> usize {
        self.page_size
    }

    pub(crate) fn alloc(&mut self) -> Slot {
        if let Some(slot) = self.free.pop() {
            self.set_free_bit(slot, false);
            if self.slots[slot as usize].is_none() {
                self.slots[slot as usize] = Some(self.fresh_page());
            }
            return slot;
        }
        let slot = self.high_water;
        if self.slots.len() == self.slots.capacity() {
            self.slots.reserve(SLOTS_PER_CHUNK);
        }
        self.slots.push(Some(self.fresh_page()));
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
        #[cfg(test)]
        let evict = !super::store::mutants::on("FS9B_NO_EVICT");
        #[cfg(not(test))]
        let evict = true;
        if evict {
            self.clones.remove(&slot);
        }
        // A reader holding this version keeps it; the slot's next owner gets a fresh page.
        let bytes = &mut self.slots[slot as usize];
        if bytes
            .as_mut()
            .is_some_and(|b| crate::sync::Arc::get_mut(b).is_none())
        {
            *bytes = None;
        }
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
        self.slots[slot as usize]
            .as_ref()
            .expect("a slot in use has its page")
    }

    pub(crate) fn page_mut(&mut self, slot: Slot) -> &mut [u8] {
        // FS9B: a slot's clone is exact only until the slot is written. A branch rewrites its own
        // current page in place once no child can see it, and a later child inherits that same slot,
        // so any write drops the clone.
        #[cfg(test)]
        let evict = !super::store::mutants::on("FS9B_NO_WRITE_EVICT");
        #[cfg(not(test))]
        let evict = true;
        if evict {
            self.clones.remove(&slot);
        }
        self.locate(slot);
        let bytes = self.slots[slot as usize]
            .as_mut()
            .expect("a slot in use has its page");
        if crate::sync::Arc::get_mut(bytes).is_none() {
            // FS11: a reader holds this version's bytes. It keeps them; the slot takes a copy.
            #[cfg(test)]
            if super::store::mutants::on("FS11_WRITE_SHARED") {
                let shared = crate::sync::Arc::as_ptr(bytes) as *mut crate::alloc::DynBoxedSlice<u8>;
                // SAFETY: none. The mutant writes through bytes a reader holds, which is the defect
                // the arena test exists to catch.
                return unsafe { &mut (*shared)[..] };
            }
            *bytes = crate::sync::Arc::new(bytes.to_vec().into_boxed_slice());
            self.unshared_writes += 1;
        }
        &mut crate::sync::Arc::get_mut(bytes).expect("the slot's bytes were just made unique")[..]
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

    /// FS11: a reader holding a slot's bytes keeps its version through a write to the slot, the
    /// slot's release, and the slot's reuse (`FS11_WRITE_SHARED` writes through them).
    #[test]
    fn a_slot_written_while_a_reader_holds_it_leaves_the_reader_its_version() {
        let mut arena = Arena::new(64);
        let s = arena.alloc();
        arena.page_mut(s).fill(1);
        let held = arena.shared_ref(s);
        arena.page_mut(s).fill(2);
        assert!(held.iter().all(|&x| x == 1), "a write reached bytes a reader holds");
        assert!(arena.page(s).iter().all(|&x| x == 2), "the slot lost its own write");
        assert_eq!(arena.unshared_writes(), 1);
        let held2 = arena.shared_ref(s);
        arena.release(s);
        let t = arena.alloc();
        assert_eq!(t, s, "the free list must be drained before the arena grows");
        arena.page_mut(t).fill(3);
        assert!(held2.iter().all(|&x| x == 2), "a reused slot wrote into bytes a reader holds");
        assert!(held.iter().all(|&x| x == 1));
        assert!(arena.page(t).iter().all(|&x| x == 3));
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
