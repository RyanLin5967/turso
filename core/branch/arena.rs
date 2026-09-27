//! The branch arena: page-sized slots handed out from a free list.
//!
//! A slot is a branch's own copy of one page — either the page's current version on a branch that
//! wrote it, or a superseded version kept alive because a child forked while it was current. The
//! arena does not know which; [`super::store`] owns that bookkeeping and the arena only answers
//! "give me a page" and "take this page back".
//!
//! A slot's bytes are an immutable `Arc<[u8]>`: writing a slot REPLACES its Arc rather than writing
//! into it. So anything that took a clone of a slot's bytes — a shipping snapshot — keeps exactly the
//! bytes it took, whatever the store writes afterwards, and a send can read them with no lock.
//!
//! ⚠ **The arena is volatile.** Slots live in memory and there is no persisted map from branch to
//! slot, so branches do not survive a restart. That is a scope line, not an oversight: the question
//! this fork exists to answer is how branch management behaves at 10^6 live branches on a real
//! engine's pager, and ferrodb's own D79 showed that making the branch map durable is a separate
//! wall with its own curve. Mixing the two would hide which one a slope belongs to.

use std::sync::Arc;

use crate::turso_assert;

/// Index of a page-sized slot in the arena.
pub(crate) type Slot = u32;

pub(crate) struct Arena {
    page_size: usize,
    /// Each handed-out slot's bytes; `None` while the slot is free.
    pages: Vec<Option<Arc<[u8]>>>,
    /// Slots below this have been handed out at least once.
    high_water: u32,
    free: Vec<Slot>,
    /// One bit per slot below `high_water`, set while the slot is on the free list. The list alone
    /// cannot answer "is this slot free" without a scan, and releasing an already-free slot must be
    /// caught AT the release — found later, it is two owners of one page and nothing says which.
    free_bits: Vec<u64>,
    /// Free-list entries whose slot [`Arena::claim`] has since taken (a replica mirroring its
    /// sender's slot numbers). `alloc` skips them as it pops them, so a claim costs O(1).
    stale: usize,
    /// Shipping stamps, kept once [`Arena::with_stamps`] made the arena.
    stamps: Option<Stamps>,
}

/// Per-slot shipping stamps: when the slot was handed out, when its CONTENT was born, and which
/// page it is a version of. They differ for a retained trunk pre-image, whose slot is handed out
/// when the trunk overwrites the page but whose content was born at the page's previous write.
#[derive(Default)]
pub(crate) struct Stamps {
    alloc_seq: Vec<u64>,
    content_seq: Vec<u64>,
    page: Vec<u32>,
}

impl Arena {
    pub(crate) fn new(page_size: usize) -> Self {
        Self {
            page_size,
            pages: Vec::new(),
            high_water: 0,
            free: Vec::new(),
            free_bits: Vec::new(),
            stale: 0,
            stamps: None,
        }
    }

    /// An arena that keeps shipping stamps for every slot.
    pub(crate) fn with_stamps(page_size: usize) -> Self {
        Self {
            stamps: Some(Stamps::default()),
            ..Self::new(page_size)
        }
    }

    pub(crate) fn page_size(&self) -> usize {
        self.page_size
    }

    pub(crate) fn alloc(&mut self) -> Slot {
        while let Some(slot) = self.free.pop() {
            if !self.is_free(slot) {
                // Claimed since it was freed; its entry was left here for this pop to drop.
                self.stale -= 1;
                continue;
            }
            self.set_free_bit(slot, false);
            return slot;
        }
        let slot = self.high_water;
        self.grow_to(slot + 1);
        slot
    }

    /// Extend the handed-out range to `high_water`, adding page entries and bitmap words.
    fn grow_to(&mut self, high_water: u32) {
        self.pages.resize(high_water as usize, None);
        self.high_water = high_water;
        let words = (self.high_water as usize).div_ceil(64);
        if self.free_bits.len() < words {
            self.free_bits.resize(words, 0);
        }
        if let Some(st) = self.stamps.as_mut() {
            let n = high_water as usize;
            st.alloc_seq.resize(n, 0);
            st.content_seq.resize(n, 0);
            st.page.resize(n, 0);
        }
    }

    /// Take exactly `slot`, which must be free or beyond the high-water mark: a replica mirrors its
    /// sender's slot numbers, so the stream can name a slot without a translation table.
    pub(crate) fn claim(&mut self, slot: Slot) {
        turso_assert!(slot != Slot::MAX, "slot u32::MAX is the page map's empty marker");
        if slot >= self.high_water {
            let from = self.high_water;
            self.grow_to(slot + 1);
            for s in from..slot {
                self.set_free_bit(s, true);
                self.free.push(s);
            }
            return;
        }
        turso_assert!(self.is_free(slot), "claimed an arena slot that is in use");
        self.set_free_bit(slot, false);
        self.stale += 1;
    }

    pub(crate) fn release(&mut self, slot: Slot) {
        turso_assert!(slot < self.high_water, "released a slot the arena never handed out");
        turso_assert!(!self.is_free(slot), "released an arena slot that was already free");
        self.set_free_bit(slot, true);
        self.free.push(slot);
        self.pages[slot as usize] = None;
    }

    /// Stamp a slot just handed out: its allocation, its content's birth and its page number.
    pub(crate) fn stamp(&mut self, slot: Slot, alloc_seq: u64, content_seq: u64, page: u32) {
        if let Some(st) = self.stamps.as_mut() {
            let i = slot as usize;
            st.alloc_seq[i] = alloc_seq;
            st.content_seq[i] = content_seq;
            st.page[i] = page;
        }
    }

    /// The slot's content was rewritten at `seq`.
    pub(crate) fn stamp_content(&mut self, slot: Slot, seq: u64) {
        if let Some(st) = self.stamps.as_mut() {
            st.content_seq[slot as usize] = seq;
        }
    }

    /// `(alloc_seq, content_seq, page)` of a slot, if stamps are kept.
    pub(crate) fn stamps_of(&self, slot: Slot) -> Option<(u64, u64, u32)> {
        let st = self.stamps.as_ref()?;
        let i = slot as usize;
        Some((st.alloc_seq[i], st.content_seq[i], st.page[i]))
    }

    /// The slot is handed out (below the high-water mark and not free).
    pub(crate) fn is_live(&self, slot: Slot) -> bool {
        slot < self.high_water && !self.is_free(slot)
    }

    pub(crate) fn is_free(&self, slot: Slot) -> bool {
        if slot >= self.high_water {
            return false;
        }
        self.free_bits[slot as usize / 64] & (1u64 << (slot % 64)) != 0
    }

    pub(crate) fn in_use(&self) -> usize {
        self.high_water as usize - self.free_count()
    }

    /// Every slot currently handed out, for membership checks.
    pub(crate) fn slots_in_use(&self) -> Vec<Slot> {
        (0..self.high_water).filter(|&s| !self.is_free(s)).collect()
    }

    pub(crate) fn free_count(&self) -> usize {
        self.free.len() - self.stale
    }

    pub(crate) fn page(&self, slot: Slot) -> &[u8] {
        self.page_arc(slot)
    }

    /// The slot's bytes, shared: a clone stays exactly these bytes whatever the slot holds later.
    pub(crate) fn page_arc(&self, slot: Slot) -> &Arc<[u8]> {
        turso_assert!(slot < self.high_water, "arena slot out of range");
        turso_assert!(!self.is_free(slot), "access to a free arena slot");
        self.pages[slot as usize]
            .as_ref()
            .expect("a handed-out slot has bytes once its owner wrote them")
    }

    /// Give the slot new bytes. The old bytes are not written: whoever holds them keeps them.
    pub(crate) fn set_page(&mut self, slot: Slot, bytes: &[u8]) {
        turso_assert!(slot < self.high_water, "arena slot out of range");
        turso_assert!(!self.is_free(slot), "write to a free arena slot");
        turso_assert!(bytes.len() == self.page_size, "a slot holds exactly one page");
        self.pages[slot as usize] = Some(Arc::from(bytes));
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
        arena.set_page(a, &[0xAA; 512]);
        arena.set_page(b, &[0xBB; 512]);
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

    /// A clone of a slot's bytes is a snapshot of them: writing the slot afterwards replaces its
    /// bytes and leaves the clone as it was.
    #[test]
    fn a_clone_of_a_slots_bytes_survives_every_later_write_and_the_release() {
        let mut arena = Arena::new(64);
        let a = arena.alloc();
        arena.set_page(a, &[1; 64]);
        let kept = arena.page_arc(a).clone();
        arena.set_page(a, &[2; 64]);
        assert!(kept.iter().all(|&x| x == 1) && arena.page(a).iter().all(|&x| x == 2));
        arena.release(a);
        let b = arena.alloc();
        assert_eq!(a, b);
        arena.set_page(b, &[3; 64]);
        assert!(kept.iter().all(|&x| x == 1), "a reused slot wrote into a kept snapshot");
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
        let slots: Vec<Slot> = (0..(256 * 2 + 3)).map(|_| arena.alloc()).collect();
        for &s in &slots {
            arena.set_page(s, &[(s % 251) as u8; 64]);
        }
        for &s in &slots {
            assert!(arena.page(s).iter().all(|&x| x == (s % 251) as u8), "slot {s} aliased");
        }
    }
}
