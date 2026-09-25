//! The branch arena: page-sized slots handed out and taken back.
//!
//! A slot is one copy of one page: a branch's version, alive while some branch's page map names it,
//! or a trunk pre-image kept for the trunk's children. The arena does not know which. For branch
//! slots it keeps the count of page-map leaves that name each one (see [`super::page_map`]) and frees
//! the slot when that count reaches zero; trunk slots keep a count of zero and are released directly
//! by [`super::store`]'s reclamation.
//!
//! ⚠ **The arena is volatile.** Slots live in memory and there is no persisted map from branch to
//! slot, so branches do not survive a restart. That is a scope line, not an oversight: the question
//! this fork exists to answer is how branch management behaves at 10^6 live branches on a real
//! engine's pager, and ferrodb's own D79 showed that making the branch map durable is a separate
//! wall with its own curve. Mixing the two would hide which one a slope belongs to.
//!
//! # Memory held tracks memory live
//!
//! A slot is a stable HANDLE; the page bytes live in a FRAME, and frames come in chunks of 256.
//! The handle table maps one to the other, so a frame can move without touching anything that
//! names the slot — the store's maps and every branch's persistent page map, which may name one
//! slot from many shared trie nodes. That is the object table of Smalltalk-80 compaction.
//!
//! * Frames are allocated from the fullest chunk that has a free one (occupancy lists, as in
//!   Bonwick's slab allocator, USENIX 1994), and a chunk is freed the moment its last frame is.
//! * That alone cannot bound the memory held: a burst to V slots followed by keeping one slot in
//!   every chunk holds V frames for V/256 live ones. So after a release that leaves more free
//!   frames than live ones (plus one chunk), the SPARSEST chunk is evacuated into the fullest
//!   others and freed. It holds at most half a chunk (the sparsest is at most the average, which
//!   is under half), so one release copies at most 128 pages and frees at least 128 frames, which
//!   keeps free frames at most live frames plus one chunk: held <= 2·live + 256.
//! * A handle cannot move — page maps name it, from however many leaves — so the handle table
//!   cannot be compacted the way frames are. It is a slab too: blocks of 512 handles (4 KiB), each
//!   with its own free list, allocated lowest-block-first (address-ordered first fit) and freed when
//!   their last handle is. So the table holds at most one 4 KiB block per live slot — a constant
//!   factor, not the peak — and after a burst it shrinks to the blocks that still hold a survivor.
//!   (Only relocating handles would do better: back-references from each slot to the leaves that
//!   name it, as btrfs keeps from extents to tree blocks.)
//! * Nothing here relocates a growing vector: free frames are threaded through each chunk and free
//!   handles through each handle block, and both directories are two-level with a top level
//!   reserved at creation.

use std::collections::BTreeSet;

use crate::turso_assert;

/// Index of a page-sized slot in the arena: a handle, stable for the slot's life.
pub(crate) type Slot = u32;

/// Frames per chunk.
const CHUNK: usize = 256;
/// Top-level size of the chunk directory, reserved at creation; and chunks per second-level box.
const BLOCK: usize = 1 << 16;
const CHUNK_BLOCK: usize = 256;
/// Handles per handle block (4 KiB of entries), and handle blocks per second-level directory box.
const HBLOCK: usize = 512;
const HDIR: usize = 4096;
/// A handle entry with this bit set is live and names a frame; clear, it is free and its low
/// 32 bits are the next free handle plus one (0 = none).
const LIVE: u64 = 1 << 63;
/// Bits 32..=62 of a live entry count the page-map leaf nodes that name the slot (see
/// [`super::page_map`]). Only branch slots are counted; a trunk-retained slot keeps 0 and is
/// released directly by the trunk's reclamation.
const REF: u64 = 1 << 32;
const REF_MASK: u64 = ((1 << 31) - 1) << 32;
/// No chunk.
const NIL: u32 = u32::MAX;

struct Chunk {
    data: Box<[u8]>,
    /// The handle each frame holds, or the next free frame plus one (0 = none) while it is free.
    owner: [u32; CHUNK],
    free_head: u32,
    live: u32,
    /// Neighbours on the occupancy list for `live`.
    prev: u32,
    next: u32,
}

enum ChunkSlot {
    Held(Box<Chunk>),
    /// Freed; the next spare id plus one (0 = none).
    Spare(u32),
}

/// 512 handle entries (see [`LIVE`]), with the free ones threaded as index plus one (0 = none).
struct HandleBlock {
    entries: [u64; HBLOCK],
    free_head: u32,
    live: u32,
}

pub(crate) struct Arena {
    page_size: usize,
    /// Handle block `b` (handles `[512·b, 512·(b+1))`) at `hdir[b / 4096][b % 4096]`, `None`
    /// while no handle in it is live.
    hdir: Vec<Box<[Option<Box<HandleBlock>>]>>,
    /// Handle blocks ever created; every handle below `512 ·` this has been handed out or could be.
    hblock_high: u32,
    /// Held handle blocks with a free handle: allocation takes the lowest.
    hfree: BTreeSet<u32>,
    /// Block ids below `hblock_high` whose block is not held.
    hspare: BTreeSet<u32>,
    held_hblocks: usize,
    /// `chunks[c >> 8][c & 0xFF]`.
    chunks: Vec<Box<[ChunkSlot]>>,
    chunk_high_water: u32,
    /// A chunk id whose chunk was freed, plus one (0 = none); the rest are threaded through
    /// [`ChunkSlot::Spare`].
    spare_id: u32,
    /// `lists[c]`: chunks with exactly `c` live frames and at least one free one (c < CHUNK).
    lists: [u32; CHUNK],
    /// Bit `c` set iff `lists[c]` is non-empty.
    nonempty: [u64; CHUNK / 64],
    live: usize,
    held_chunks: usize,
    /// Observation only: page copies made by evacuation, and chunks freed.
    copied: u64,
    chunks_freed: u64,
}

impl Arena {
    pub(crate) fn new(page_size: usize) -> Self {
        Self {
            page_size,
            hdir: Vec::with_capacity((1usize << 32) / (HBLOCK * HDIR)),
            hblock_high: 0,
            hfree: BTreeSet::new(),
            hspare: BTreeSet::new(),
            held_hblocks: 0,
            chunks: Vec::with_capacity(BLOCK),
            chunk_high_water: 0,
            spare_id: 0,
            lists: [NIL; CHUNK],
            nonempty: [0; CHUNK / 64],
            live: 0,
            held_chunks: 0,
            copied: 0,
            chunks_freed: 0,
        }
    }

    pub(crate) fn page_size(&self) -> usize {
        self.page_size
    }

    /// Elements relocated by growth of the free list, the free-bit vector and the chunk vector:
    /// always zero here, where no vector grows by relocation.
    pub(crate) fn moved(&self) -> [u64; 3] {
        [0; 3]
    }

    /// Page copies made by evacuating chunks, and chunks freed.
    pub(crate) fn compaction(&self) -> (u64, u64) {
        (self.copied, self.chunks_freed)
    }

    fn chunk_slot(&mut self, c: u32) -> &mut ChunkSlot {
        &mut self.chunks[c as usize / CHUNK_BLOCK][c as usize % CHUNK_BLOCK]
    }

    fn chunk(&self, c: u32) -> &Chunk {
        match &self.chunks[c as usize / CHUNK_BLOCK][c as usize % CHUNK_BLOCK] {
            ChunkSlot::Held(ch) => ch,
            ChunkSlot::Spare(_) => panic!("chunk {c} is not held"),
        }
    }

    fn chunk_mut(&mut self, c: u32) -> &mut Chunk {
        match self.chunk_slot(c) {
            ChunkSlot::Held(ch) => ch,
            ChunkSlot::Spare(_) => panic!("chunk {c} is not held"),
        }
    }

    fn hblock(&self, b: u32) -> Option<&HandleBlock> {
        self.hdir
            .get(b as usize / HDIR)?
            .get(b as usize % HDIR)?
            .as_deref()
    }

    fn hblock_mut(&mut self, b: u32) -> &mut HandleBlock {
        self.hdir[b as usize / HDIR][b as usize % HDIR]
            .as_deref_mut()
            .expect("a held handle block")
    }

    fn entry(&self, h: Slot) -> u64 {
        self.hblock(h / HBLOCK as u32).expect("a held handle block").entries[h as usize % HBLOCK]
    }

    fn entry_mut(&mut self, h: Slot) -> &mut u64 {
        &mut self.hblock_mut(h / HBLOCK as u32).entries[h as usize % HBLOCK]
    }

    fn handle_high_water(&self) -> u32 {
        self.hblock_high.saturating_mul(HBLOCK as u32)
    }

    /// A free handle from the lowest held block that has one, else from a new block.
    fn alloc_handle(&mut self) -> Slot {
        let b = match self.hfree.first().copied() {
            Some(b) => b,
            None => {
                let b = self.hspare.pop_first().unwrap_or_else(|| {
                    let b = self.hblock_high;
                    turso_assert!(
                        (b as usize + 1) * HBLOCK <= u32::MAX as usize,
                        "the arena ran out of handles"
                    );
                    self.hblock_high += 1;
                    if b as usize / HDIR == self.hdir.len() {
                        self.hdir
                            .push(std::iter::repeat_with(|| None).take(HDIR).collect());
                    }
                    b
                });
                let mut entries = [0u64; HBLOCK];
                for (i, e) in entries.iter_mut().enumerate().take(HBLOCK - 1) {
                    *e = i as u64 + 2;
                }
                self.hdir[b as usize / HDIR][b as usize % HDIR] = Some(Box::new(HandleBlock {
                    entries,
                    free_head: 1,
                    live: 0,
                }));
                self.held_hblocks += 1;
                self.hfree.insert(b);
                b
            }
        };
        let blk = self.hblock_mut(b);
        let i = blk.free_head - 1;
        blk.free_head = blk.entries[i as usize] as u32;
        blk.live += 1;
        if blk.free_head == 0 {
            self.hfree.remove(&b);
        }
        b * HBLOCK as u32 + i
    }

    /// Return handle `h` to its block, and the block to the allocator if that was its last.
    fn free_handle(&mut self, h: Slot) {
        let b = h / HBLOCK as u32;
        let blk = self.hblock_mut(b);
        let was_full = blk.free_head == 0;
        let i = h % HBLOCK as u32;
        blk.entries[i as usize] = u64::from(blk.free_head);
        blk.free_head = i + 1;
        blk.live -= 1;
        if blk.live == 0 {
            self.hdir[b as usize / HDIR][b as usize % HDIR] = None;
            self.held_hblocks -= 1;
            self.hfree.remove(&b);
            self.hspare.insert(b);
        } else if was_full {
            self.hfree.insert(b);
        }
    }

    fn unlink(&mut self, c: u32) {
        let (live, prev, next) = {
            let ch = self.chunk(c);
            (ch.live as usize, ch.prev, ch.next)
        };
        if prev == NIL {
            self.lists[live] = next;
            if next == NIL {
                self.nonempty[live / 64] &= !(1 << (live % 64));
            }
        } else {
            self.chunk_mut(prev).next = next;
        }
        if next != NIL {
            self.chunk_mut(next).prev = prev;
        }
    }

    /// Put `c` on the list for its occupancy, if it has a free frame.
    fn link(&mut self, c: u32) {
        let live = self.chunk(c).live as usize;
        if live == CHUNK {
            return;
        }
        let head = self.lists[live];
        {
            let ch = self.chunk_mut(c);
            ch.prev = NIL;
            ch.next = head;
        }
        if head != NIL {
            self.chunk_mut(head).prev = c;
        }
        self.lists[live] = c;
        self.nonempty[live / 64] |= 1 << (live % 64);
    }

    /// The fullest chunk with a free frame, other than `except`, if any.
    fn fullest(&self, except: u32) -> Option<u32> {
        for w in (0..CHUNK / 64).rev() {
            let mut bits = self.nonempty[w];
            while bits != 0 {
                let c = w * 64 + 63 - bits.leading_zeros() as usize;
                let head = self.lists[c];
                if head != except {
                    return Some(head);
                }
                let next = self.chunk(head).next;
                if next != NIL {
                    return Some(next);
                }
                bits &= !(1 << (c % 64));
            }
        }
        None
    }

    /// The sparsest held chunk with a free frame and at least one live one.
    fn sparsest(&self) -> Option<u32> {
        for w in 0..CHUNK / 64 {
            let mut bits = self.nonempty[w];
            if w == 0 {
                bits &= !1;
            }
            if bits != 0 {
                return Some(self.lists[w * 64 + bits.trailing_zeros() as usize]);
            }
        }
        None
    }

    fn new_chunk(&mut self) -> u32 {
        let c = if self.spare_id != 0 {
            let c = self.spare_id - 1;
            let ChunkSlot::Spare(next) = *self.chunk_slot(c) else {
                panic!("the spare list names a held chunk");
            };
            self.spare_id = next;
            c
        } else {
            let c = self.chunk_high_water;
            self.chunk_high_water += 1;
            let block = c as usize / CHUNK_BLOCK;
            if block == self.chunks.len() {
                turso_assert!(self.chunks.len() < BLOCK, "the arena's chunk directory is full");
                self.chunks.push(
                    std::iter::repeat_with(|| ChunkSlot::Spare(0))
                        .take(CHUNK_BLOCK)
                        .collect(),
                );
            }
            c
        };
        let block = c as usize / CHUNK_BLOCK;
        let mut owner = [0u32; CHUNK];
        for (i, o) in owner.iter_mut().enumerate().take(CHUNK - 1) {
            *o = i as u32 + 2;
        }
        self.chunks[block][c as usize % CHUNK_BLOCK] = ChunkSlot::Held(Box::new(Chunk {
            data: vec![0u8; CHUNK * self.page_size].into_boxed_slice(),
            owner,
            free_head: 1,
            live: 0,
            prev: NIL,
            next: NIL,
        }));
        self.held_chunks += 1;
        self.link(c);
        c
    }

    /// Take a free frame of chunk `c` for handle `h`; returns the frame.
    fn take_frame(&mut self, c: u32, h: Slot) -> u32 {
        self.unlink(c);
        let ch = self.chunk_mut(c);
        let i = ch.free_head - 1;
        ch.free_head = ch.owner[i as usize];
        ch.owner[i as usize] = h;
        ch.live += 1;
        self.link(c);
        c * CHUNK as u32 + i
    }

    pub(crate) fn alloc(&mut self) -> Slot {
        let h = self.alloc_handle();
        let c = self.fullest(NIL).unwrap_or_else(|| self.new_chunk());
        let frame = self.take_frame(c, h);
        *self.entry_mut(h) = LIVE | u64::from(frame);
        self.live += 1;
        h
    }

    /// Count one more page-map leaf that names `slot`.
    pub(crate) fn incref(&mut self, slot: Slot) {
        turso_assert!(!self.is_free(slot), "counted a reference to a free arena slot");
        let entry = self.entry_mut(slot);
        turso_assert!(*entry & REF_MASK != REF_MASK, "arena slot reference count overflow");
        *entry += REF;
    }

    /// Count one leaf fewer; the slot is released when none is left. Returns whether it was.
    pub(crate) fn decref(&mut self, slot: Slot) -> bool {
        turso_assert!(!self.is_free(slot), "dropped a reference to a free arena slot");
        let entry = self.entry(slot);
        turso_assert!(entry & REF_MASK != 0, "dropped a reference the slot does not have");
        *self.entry_mut(slot) = entry - REF;
        if (entry - REF) & REF_MASK == 0 {
            self.release(slot);
            true
        } else {
            false
        }
    }

    /// Page-map leaves that name `slot`.
    pub(crate) fn refs(&self, slot: Slot) -> u32 {
        ((self.entry(slot) & REF_MASK) >> 32) as u32
    }

    pub(crate) fn release(&mut self, slot: Slot) {
        turso_assert!(slot < self.handle_high_water(), "released a slot the arena never handed out");
        turso_assert!(!self.is_free(slot), "released an arena slot that was already free");
        turso_assert!(
            self.entry(slot) & REF_MASK == 0,
            "released an arena slot a page map still names"
        );
        let frame = self.entry(slot) as u32;
        self.free_handle(slot);
        self.live -= 1;
        self.free_frame(frame);
        let free = self.held_chunks * CHUNK - self.live;
        if free > self.live + CHUNK {
            if let Some(c) = self.sparsest() {
                self.evacuate(c);
            }
        }
    }

    fn free_frame(&mut self, frame: u32) {
        let (c, i) = (frame / CHUNK as u32, frame % CHUNK as u32);
        self.unlink_if_listed(c);
        let ch = self.chunk_mut(c);
        ch.owner[i as usize] = ch.free_head;
        ch.free_head = i + 1;
        ch.live -= 1;
        if ch.live == 0 {
            *self.chunk_slot(c) = ChunkSlot::Spare(self.spare_id);
            self.spare_id = c + 1;
            self.held_chunks -= 1;
            self.chunks_freed += 1;
        } else {
            self.link(c);
        }
    }

    /// A full chunk is on no list.
    fn unlink_if_listed(&mut self, c: u32) {
        if (self.chunk(c).live as usize) < CHUNK {
            self.unlink(c);
        }
    }

    /// Move every live frame of chunk `c` into the fullest other chunks, then free `c`.
    fn evacuate(&mut self, c: u32) {
        let owners: Vec<(u32, Slot)> = {
            let ch = self.chunk(c);
            let mut free = vec![false; CHUNK];
            let mut i = ch.free_head;
            while i != 0 {
                free[i as usize - 1] = true;
                i = ch.owner[i as usize - 1];
            }
            (0..CHUNK as u32)
                .filter(|&i| !free[i as usize])
                .map(|i| (i, ch.owner[i as usize]))
                .collect()
        };
        for (i, h) in owners {
            let Some(to) = self.fullest(c) else {
                return;
            };
            let frame = self.take_frame(to, h);
            let (from_off, to_off) = (i as usize * self.page_size, (frame as usize % CHUNK) * self.page_size);
            let page_size = self.page_size;
            let src = self.chunk(c).data[from_off..from_off + page_size].to_vec();
            self.chunk_mut(frame / CHUNK as u32).data[to_off..to_off + page_size]
                .copy_from_slice(&src);
            let refs = self.entry(h) & REF_MASK;
            *self.entry_mut(h) = LIVE | refs | u64::from(frame);
            self.copied += 1;
            self.free_frame(c * CHUNK as u32 + i);
        }
    }

    pub(crate) fn is_free(&self, slot: Slot) -> bool {
        slot < self.handle_high_water()
            && self
                .hblock(slot / HBLOCK as u32)
                .is_none_or(|blk| blk.entries[slot as usize % HBLOCK] & LIVE == 0)
    }

    pub(crate) fn in_use(&self) -> usize {
        self.live
    }

    /// Every slot currently handed out, for membership checks.
    pub(crate) fn slots_in_use(&self) -> Vec<Slot> {
        (0..self.handle_high_water())
            .filter(|&s| !self.is_free(s))
            .collect()
    }

    /// Free frames in the chunks the arena holds.
    pub(crate) fn free_count(&self) -> usize {
        self.held_chunks * CHUNK - self.live
    }

    /// Handle entries the table holds: 512 per block that still has a live handle.
    pub(crate) fn handles_held(&self) -> usize {
        self.held_hblocks * HBLOCK
    }

    pub(crate) fn page(&self, slot: Slot) -> &[u8] {
        let (c, off) = self.locate(slot);
        &self.chunk(c).data[off..off + self.page_size]
    }

    pub(crate) fn page_mut(&mut self, slot: Slot) -> &mut [u8] {
        let (c, off) = self.locate(slot);
        let page_size = self.page_size;
        &mut self.chunk_mut(c).data[off..off + page_size]
    }

    fn locate(&self, slot: Slot) -> (u32, usize) {
        turso_assert!(slot < self.handle_high_water(), "arena slot out of range");
        turso_assert!(!self.is_free(slot), "access to a free arena slot");
        let frame = self.entry(slot) as u32;
        (
            frame / CHUNK as u32,
            (frame as usize % CHUNK) * self.page_size,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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
        assert_eq!(c, a, "a released handle must be reused before the handle table grows");
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
        let slots: Vec<Slot> = (0..(CHUNK * 2 + 3)).map(|_| arena.alloc()).collect();
        for &s in &slots {
            arena.page_mut(s).fill((s % 251) as u8);
        }
        for &s in &slots {
            assert!(arena.page(s).iter().all(|&x| x == (s % 251) as u8), "slot {s} aliased");
        }
    }

    /// The adversarial scatter: a burst to 100 chunks, then every slot but one per chunk
    /// released. Without compaction no chunk ever empties and 100 chunks stay held for 100 live
    /// slots; with it the memory held is at most twice the live memory plus one chunk, every
    /// surviving slot keeps its bytes under its own handle, and no release copies more than half a
    /// chunk.
    #[test]
    fn a_scattered_survivor_set_is_compacted_and_keeps_its_bytes() {
        let mut arena = Arena::new(64);
        let slots: Vec<Slot> = (0..CHUNK * 100).map(|_| arena.alloc()).collect();
        for &s in &slots {
            arena.page_mut(s).fill((s % 251) as u8);
        }
        let mut max_copied = 0;
        for (i, &s) in slots.iter().enumerate() {
            if i % CHUNK != 0 {
                let before = arena.compaction().0;
                arena.release(s);
                max_copied = max_copied.max(arena.compaction().0 - before);
                let held = arena.in_use() + arena.free_count();
                assert!(held <= 2 * arena.in_use() + 2 * CHUNK, "held {held} for {}", arena.in_use());
            }
        }
        assert_eq!(arena.in_use(), 100);
        assert!(arena.in_use() + arena.free_count() <= 2 * 100 + CHUNK);
        assert!(max_copied <= CHUNK as u64 / 2, "one release copied {max_copied} pages");
        assert!(arena.compaction().0 > 0, "the scatter must have forced an evacuation");
        for &s in slots.iter().step_by(CHUNK) {
            assert!(arena.page(s).iter().all(|&x| x == (s % 251) as u8), "slot {s} lost its bytes");
        }
    }

    /// The handle table shrinks to the blocks that still hold a live handle: after a burst of 40
    /// handle blocks, keeping every 1,024th slot keeps every other block (20), and keeping only
    /// the first block's worth keeps one — where a table kept at its peak would hold all 40.
    #[test]
    fn handle_blocks_are_freed_when_their_last_handle_is() {
        for (stride, blocks) in [(1024usize, 20usize), (HBLOCK * 40, 1)] {
            let mut arena = Arena::new(64);
            let slots: Vec<Slot> = (0..HBLOCK * 40).map(|_| arena.alloc()).collect();
            assert_eq!(arena.handles_held(), HBLOCK * 40);
            for (i, &s) in slots.iter().enumerate() {
                arena.page_mut(s).fill((s % 251) as u8);
                if i % stride != 0 {
                    arena.release(s);
                }
            }
            assert_eq!(arena.handles_held(), HBLOCK * blocks, "stride {stride}");
            for &s in slots.iter().step_by(stride) {
                assert!(arena.page(s).iter().all(|&x| x == (s % 251) as u8), "slot {s}");
                assert!(!arena.is_free(s));
            }
            for (i, &s) in slots.iter().enumerate() {
                assert_eq!(arena.is_free(s), i % stride != 0, "slot {s}");
            }
        }
    }

    /// Random allocations, writes and releases against a model of each live slot's bytes, through
    /// growth and shrinkage, with the held/live bound checked after every release.
    #[test]
    fn random_allocations_and_releases_match_a_model() {
        let mut arena = Arena::new(64);
        let mut model: HashMap<Slot, u8> = HashMap::new();
        let mut live: Vec<Slot> = Vec::new();
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n
        };
        for step in 0..200_000u64 {
            let grow = (step / 20_000) % 2 == 0;
            if live.is_empty() || next(10) < if grow { 7 } else { 3 } {
                let s = arena.alloc();
                assert!(!model.contains_key(&s), "handle {s} handed out twice");
                let v = next(256) as u8;
                arena.page_mut(s).fill(v);
                model.insert(s, v);
                live.push(s);
            } else {
                let s = live.swap_remove(next(live.len() as u64) as usize);
                arena.release(s);
                model.remove(&s);
                let held = arena.in_use() + arena.free_count();
                assert!(held <= 2 * arena.in_use() + 2 * CHUNK, "step {step}: held {held}");
            }
            if step % 10_000 == 0 {
                for (&s, &v) in &model {
                    assert!(arena.page(s).iter().all(|&x| x == v), "step {step}: slot {s}");
                }
                assert_eq!(arena.in_use(), model.len());
            }
        }
    }
}
