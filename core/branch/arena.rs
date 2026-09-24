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

use super::journal::{fsync_file, open_rw, read_at, write_at};
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
}

impl Arena {
    pub(crate) fn new(page_size: usize) -> Self {
        Self {
            page_size,
            backing: Backing::Memory { chunks: Vec::new() },
            high_water: 0,
            free: Vec::new(),
            free_bits: Vec::new(),
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
        let free = (0..high_water)
            .rev()
            .filter(|&s| free_bits[s as usize / 64] & (1u64 << (s % 64)) != 0)
            .collect();
        Ok(Self {
            page_size,
            backing: Backing::File { file, dirty: false },
            high_water,
            free,
            free_bits,
        })
    }

    pub(crate) fn page_size(&self) -> usize {
        self.page_size
    }

    pub(crate) fn is_file_backed(&self) -> bool {
        matches!(self.backing, Backing::File { .. })
    }

    pub(crate) fn alloc(&mut self) -> Slot {
        if let Some(slot) = self.free.pop() {
            self.set_free_bit(slot, false);
            return slot;
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

    pub(crate) fn write_slot(&mut self, slot: Slot, bytes: &[u8]) -> Result<()> {
        turso_assert!(bytes.len() == self.page_size, "arena write of a wrong-sized page");
        let offset = self.check(slot) as u64 * self.page_size as u64;
        if let Backing::File { file, dirty } = &mut self.backing {
            write_at(file, bytes, offset)?;
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
            return read_at(file, out, offset);
        }
        out.copy_from_slice(self.page(slot));
        Ok(())
    }

    /// Make every slot written so far durable. A no-op for the memory backing.
    pub(crate) fn sync(&mut self) -> Result<()> {
        if let Backing::File { file, dirty } = &mut self.backing {
            if *dirty {
                fsync_file(file)?;
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
