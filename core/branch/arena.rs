//! The branch arena: page-sized slots handed out from per-owner magazines.
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
//! # Concurrency (FRS, r11-bushy-conc)
//!
//! The store is striped (see `store`), so the arena has no lock of its own on the common path:
//!
//! * **Pages without a lock.** A slot's bytes live in chunks that are installed once and never move
//!   or go away while the arena lives, so any thread can reach a slot's page with two acquire loads.
//!   Who may touch the bytes is settled by the store's ownership rule, not by a lock: a slot is
//!   written only by the branch that allocated it, before any other map names it, or by a branch
//!   that holds the only map naming it (its count is 1 and its path is its own, under its shard's
//!   lock and write transaction); every other access is a read of a slot the reader's own map names,
//!   which keeps the slot allocated and its bytes unchanged for as long as it does (see
//!   [`Arena::read`] and [`Arena::write`]).
//! * **Magazines.** Each owner — a shard of the store, or the trunk — allocates from and frees into
//!   its own [`Magazine`], under the lock it already holds; a magazine that runs dry takes [`BATCH`]
//!   slots from the shared depot, and one that holds twice that gives [`BATCH`] back (Bonwick and
//!   Adams, "Magazines and Vmem", USENIX ATC 2001). A slot freed by one owner and allocated by
//!   another simply moves between magazines: slots are global numbers, so there is no remote free.
//! * The free bitmap is atomic words, so a double release is still caught at the release.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::OnceLock;

use crate::sync::Mutex;
use crate::turso_assert;

use super::radix::Radix;

/// Index of a page-sized slot in the arena.
pub(crate) type Slot = u32;

/// Slots per chunk. Chunks are zero-filled through the allocator, so the OS backs a chunk with
/// memory only as its slots are touched; the chunk size bounds the granularity, not the footprint.
const SLOTS_PER_CHUNK: usize = 256;
/// Chunks per group and groups: 2^12 * 2^12 * 2^8 slots covers every `u32`.
const CHUNKS_PER_GROUP: usize = 1 << 12;
const GROUPS: usize = 1 << 12;
/// Slots a magazine takes from the depot at once, and gives back once it holds twice as many.
pub(crate) const BATCH: usize = 64;

/// One chunk's pages. `UnsafeCell` because the bytes are written through a shared reference; the
/// store's ownership rule (module doc) is what makes that sound.
struct Chunk(Box<[UnsafeCell<u8>]>);

// SAFETY: every access to a chunk's bytes goes through `Arena::read`/`Arena::write`, whose callers
// uphold the ownership rule in the module doc: no slot is written while another thread can read it.
unsafe impl Sync for Chunk {}
unsafe impl Send for Chunk {}

impl Chunk {
    fn zeroed(len: usize) -> Self {
        let bytes: Box<[u8]> = vec![0u8; len].into_boxed_slice();
        // SAFETY: `UnsafeCell<u8>` has the same layout as `u8` (`repr(transparent)`).
        let cells = unsafe { Box::from_raw(Box::into_raw(bytes) as *mut [UnsafeCell<u8>]) };
        Self(cells)
    }
}

/// An owner's slots: the free ones it can hand out without the depot, and how many it has handed
/// out net of what it has taken back (which may be negative: a slot one owner allocated can be
/// freed by another).
#[derive(Default)]
pub(crate) struct Magazine {
    free: Vec<Slot>,
    pub(crate) in_use: i64,
}

struct Depot {
    free: Vec<Slot>,
    /// Slots below this have been handed out at least once. Written only under the depot's lock.
    high_water: u32,
    /// Acquisitions of the depot's lock, and those that found it held. Observation only.
    acquisitions: u64,
    contended: u64,
}

pub(crate) struct Arena {
    page_size: usize,
    groups: Box<[OnceLock<Box<[OnceLock<Chunk>]>>]>,
    depot: Mutex<Depot>,
    /// A copy of the depot's `high_water`, readable without its lock.
    high_water: AtomicU32,
    /// One bit per slot, set while the slot is free (in a magazine or the depot). Releasing an
    /// already-free slot must be caught AT the release — found later, it is two owners of one page
    /// and nothing says which.
    free_bits: Radix<AtomicU64>,
}

impl Arena {
    pub(crate) fn new(page_size: usize) -> Self {
        Self {
            page_size,
            groups: (0..GROUPS).map(|_| OnceLock::new()).collect(),
            depot: Mutex::new(Depot {
                free: Vec::new(),
                high_water: 0,
                acquisitions: 0,
                contended: 0,
            }),
            high_water: AtomicU32::new(0),
            free_bits: Radix::new(),
        }
    }

    pub(crate) fn page_size(&self) -> usize {
        self.page_size
    }

    fn chunk(&self, slot: Slot) -> &Chunk {
        let c = slot as usize / SLOTS_PER_CHUNK;
        self.groups[c / CHUNKS_PER_GROUP]
            .get()
            .and_then(|g| g[c % CHUNKS_PER_GROUP].get())
            .expect("a slot below the high-water mark has its chunk installed")
    }

    fn install(&self, slot: Slot) {
        let c = slot as usize / SLOTS_PER_CHUNK;
        let group = self.groups[c / CHUNKS_PER_GROUP]
            .get_or_init(|| (0..CHUNKS_PER_GROUP).map(|_| OnceLock::new()).collect());
        group[c % CHUNKS_PER_GROUP].get_or_init(|| Chunk::zeroed(SLOTS_PER_CHUNK * self.page_size));
    }

    fn free_word(&self, slot: Slot) -> &AtomicU64 {
        self.free_bits.get_or_insert(slot / 64)
    }

    /// Hand out a slot from `mag`, refilling it from the depot when it is empty.
    pub(crate) fn alloc(&self, mag: &mut Magazine) -> Slot {
        if mag.free.is_empty() {
            self.refill(mag);
        }
        let slot = mag.free.pop().expect("a refill yields slots");
        let bit = 1u64 << (slot % 64);
        let was = self.free_word(slot).fetch_and(!bit, Ordering::AcqRel);
        turso_assert!(was & bit != 0, "handed out an arena slot that was not free");
        mag.in_use += 1;
        slot
    }

    /// Take `slot` back into `mag`, returning a batch to the depot when the magazine is full.
    pub(crate) fn release(&self, mag: &mut Magazine, slot: Slot) {
        turso_assert!(
            slot < self.high_water.load(Ordering::Acquire),
            "released a slot the arena never handed out"
        );
        let bit = 1u64 << (slot % 64);
        let was = self.free_word(slot).fetch_or(bit, Ordering::AcqRel);
        turso_assert!(was & bit == 0, "released an arena slot that was already free");
        mag.free.push(slot);
        mag.in_use -= 1;
        if mag.free.len() >= 2 * BATCH {
            let back = mag.free.split_off(mag.free.len() - BATCH);
            let mut depot = self.take_depot();
            depot.free.extend(back);
        }
    }

    fn take_depot(&self) -> crate::sync::MutexGuard<'_, Depot> {
        let (mut depot, contended) = match self.depot.try_lock() {
            Some(d) => (d, false),
            None => (self.depot.lock(), true),
        };
        depot.acquisitions += 1;
        if contended {
            depot.contended += 1;
        }
        depot
    }

    fn refill(&self, mag: &mut Magazine) {
        let mut depot = self.take_depot();
        if !depot.free.is_empty() {
            let from = depot.free.len().saturating_sub(BATCH);
            mag.free.extend(depot.free.drain(from..));
            return;
        }
        let first = depot.high_water;
        let last = first
            .checked_add(BATCH as u32)
            .filter(|&l| l < Slot::MAX)
            .expect("the branch arena is out of slots");
        for slot in first..last {
            self.install(slot);
            let bit = 1u64 << (slot % 64);
            self.free_word(slot).fetch_or(bit, Ordering::AcqRel);
        }
        depot.high_water = last;
        self.high_water.store(last, Ordering::Release);
        // Highest first out, so a fresh batch is handed out in ascending order.
        mag.free.extend((first..last).rev());
    }

    /// The depot lock's acquisitions and contended acquisitions (the store's "depot" site).
    pub(crate) fn depot_counts(&self) -> (u64, u64) {
        let depot = self.depot.lock();
        (depot.acquisitions, depot.contended)
    }

    pub(crate) fn high_water(&self) -> u32 {
        self.high_water.load(Ordering::Acquire)
    }

    pub(crate) fn is_free(&self, slot: Slot) -> bool {
        if slot >= self.high_water() {
            return false;
        }
        self.free_word(slot).load(Ordering::Acquire) & (1u64 << (slot % 64)) != 0
    }

    /// Every slot currently handed out, for membership checks.
    pub(crate) fn slots_in_use(&self) -> Vec<Slot> {
        (0..self.high_water()).filter(|&s| !self.is_free(s)).collect()
    }

    fn bytes(&self, slot: Slot) -> *mut u8 {
        turso_assert!(slot < self.high_water(), "arena slot out of range");
        turso_assert!(!self.is_free(slot), "access to a free arena slot");
        let offset = (slot as usize % SLOTS_PER_CHUNK) * self.page_size;
        self.chunk(slot).0[offset].get()
    }

    /// Copy `slot`'s page into `out`.
    ///
    /// # Safety
    /// No thread writes `slot` during the call: the caller's map names `slot` (so it stays allocated)
    /// and either no other map names it, or the caller is the only thread that may write it (see the
    /// module doc).
    pub(crate) unsafe fn read(&self, slot: Slot, out: &mut [u8]) {
        let src = self.bytes(slot);
        // SAFETY: the chunk holds `page_size` bytes at this offset, and the caller guarantees no
        // concurrent write to them.
        out.copy_from_slice(unsafe { std::slice::from_raw_parts(src, self.page_size) });
    }

    /// Overwrite `slot`'s page with `src`.
    ///
    /// # Safety
    /// No other thread reads or writes `slot` during the call: the caller allocated it and no other
    /// map names it yet, or the caller holds the only map naming it (count 1, path its own) under its
    /// shard's lock and write transaction, so no fork can share it meanwhile.
    pub(crate) unsafe fn write(&self, slot: Slot, src: &[u8]) {
        let dst = self.bytes(slot);
        // SAFETY: as above; exclusive access to `page_size` bytes at this offset.
        unsafe { std::slice::from_raw_parts_mut(dst, self.page_size) }.copy_from_slice(src);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_released_slot_is_reused_and_reads_as_free_only_while_released() {
        let arena = Arena::new(512);
        let mut mag = Magazine::default();
        let a = arena.alloc(&mut mag);
        let b = arena.alloc(&mut mag);
        assert_ne!(a, b);
        unsafe {
            arena.write(a, &[0xAA; 512]);
            arena.write(b, &[0xBB; 512]);
        }
        let mut buf = [0u8; 512];
        unsafe { arena.read(a, &mut buf) };
        assert!(buf.iter().all(|&x| x == 0xAA));
        unsafe { arena.read(b, &mut buf) };
        assert!(buf.iter().all(|&x| x == 0xBB));
        assert_eq!(mag.in_use, 2);

        arena.release(&mut mag, a);
        assert!(arena.is_free(a));
        assert!(!arena.is_free(b));
        assert_eq!(mag.in_use, 1);

        let c = arena.alloc(&mut mag);
        assert_eq!(c, a, "the magazine must be drained before the arena grows");
        assert!(!arena.is_free(c));
        assert_eq!(mag.in_use, 2);
    }

    #[test]
    #[should_panic(expected = "already free")]
    fn a_double_release_is_caught_at_the_release() {
        let arena = Arena::new(512);
        let mut mag = Magazine::default();
        let a = arena.alloc(&mut mag);
        arena.release(&mut mag, a);
        arena.release(&mut mag, a);
    }

    #[test]
    fn slots_span_chunks_without_aliasing() {
        let arena = Arena::new(64);
        let mut mag = Magazine::default();
        let slots: Vec<Slot> = (0..(SLOTS_PER_CHUNK * 2 + 3)).map(|_| arena.alloc(&mut mag)).collect();
        for &s in &slots {
            unsafe { arena.write(s, &[(s % 251) as u8; 64]) };
        }
        let mut buf = [0u8; 64];
        for &s in &slots {
            unsafe { arena.read(s, &mut buf) };
            assert!(buf.iter().all(|&x| x == (s % 251) as u8), "slot {s} aliased");
        }
    }

    /// Slots move between magazines through the depot, and the free bitmap stays exact: two owners
    /// allocate and free across each other, and at the end every slot is free exactly once.
    #[test]
    fn slots_freed_by_one_owner_are_reused_by_another() {
        let arena = Arena::new(64);
        let (mut a, mut b) = (Magazine::default(), Magazine::default());
        let taken: Vec<Slot> = (0..5 * BATCH).map(|_| arena.alloc(&mut a)).collect();
        for &s in &taken {
            arena.release(&mut b, s);
        }
        assert_eq!(a.in_use + b.in_use, 0);
        let again: Vec<Slot> = (0..5 * BATCH).map(|_| arena.alloc(&mut a)).collect();
        assert_eq!(
            arena.high_water() as usize,
            5 * BATCH + BATCH,
            "b's frees reached a through the depot instead of growing the arena"
        );
        for s in again {
            arena.release(&mut a, s);
        }
        assert_eq!(arena.slots_in_use(), Vec::<Slot>::new());
    }
}
