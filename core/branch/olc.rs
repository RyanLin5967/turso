//! F-K3v (lane r11-k3-trunklock, PREREG amendments 3.2, 3b, 3e): the trunk's retained versions of each
//! page as a skip list with ONE writer and optimistic readers over type-stable nodes.
//!
//! F-K3 keeps these lists in crossbeam's lock-free skip list, whose removed nodes are freed by epoch
//! reclamation: a reader that stalls while pinned (descheduled, stopped) keeps every node removed
//! after it pinned, so the garbage grows with the stall. Here every write already happens under the
//! trunk's lock, so a list needs no lock-free WRITE protocol, only readers that take no lock and
//! hold nothing:
//!
//! * **Type-stable nodes, one pool per height.** Nodes come from pools the lists own, one per tower
//!   height, and are never returned to the allocator while the lists live; a removed node goes back
//!   to its height's pool at once and may be reused by the next insert of that height, in any page's
//!   list (type-stable memory, Greenwald and Cheriton, OSDI 1996; immediate reuse checked by
//!   versions, as in VBR, Sheffi, Herlihy and Petrank, 2021). A node never changes height, so a
//!   pointer a reader holds always points at a node with at least as many links as the level it was
//!   reached through, never at freed memory. A node is 32 + 8h bytes.
//! * **Versions.** Every node carries a version that the writer makes odd before it changes the node
//!   and even again after, and that only ever grows; a free node's version is odd. A reader reads a
//!   node between two loads of its version and trusts what it read only if they are equal and even —
//!   a seqlock per node, taken hand over hand down the list (optimistic lock coupling, Leis et al.,
//!   DaMoN 2016). Unlinking a node changes its predecessor, so a reader that followed the pointer
//!   finds the predecessor's version moved and starts again from the head.
//! * **Nothing held; not lock-free.** A stalled reader holds no epoch and no reference: its next
//!   validation fails and it restarts. Garbage is 0 whatever readers do; the free pools (high-water
//!   minus live) are held for the lists' life. But a reader CAN wait on the writer: a writer stopped
//!   between making a node odd and making it even blocks every reader whose path crosses that node.
//!   So after [`MAX_FAILED_LAPS`] failed attempts a reader gives up and the store answers under the
//!   trunk's lock instead (a pessimistic fallback behind optimistic latches, as in hybrid latches,
//!   Böttcher et al., DaMoN 2019); failed attempts and fallbacks are counted.
//!
//! A reader's answer — the version of a page a child forked at `f` sees — is validated like every
//! other hop, and it cannot go stale after that: the store keeps that version while the reading
//! branch lives (see the store's "Reads of rewritten trunk pages without the trunk's lock").

use std::alloc::{alloc_zeroed, dealloc, handle_alloc_error, Layout};
use std::ptr::{self, NonNull};
use std::sync::atomic::{fence, AtomicPtr, AtomicU32, AtomicU64, Ordering};

use crossbeam_utils::CachePadded;

use crate::sync::Mutex;

use super::radix::Radix;

/// Tower height cap. With p = 1/4 a list of n versions needs about log4 n levels: 7 at the 12.7k
/// versions per page of a steady 10^6-branch trunk (r11-space P2), so 12 is ample.
const MAX_H: usize = 12;
/// Nodes allocated per refill of one height's pool.
const CHUNK: usize = 1024;
/// Failed attempts (restarts and laps that found the head being written) after which a reader
/// stops and the store answers under the trunk's lock.
pub(crate) const MAX_FAILED_LAPS: u32 = 64;
/// The bound for a search under the trunk's lock, where no writer can run and the first attempt
/// succeeds unless an invariant is broken (a node left odd): past it the store panics rather than
/// spin for ever while holding the lock.
const LOCKED_MAX_FAILED: u32 = 1 << 20;
/// Bytes of a node's fixed part; its links follow it.
const HEADER: usize = std::mem::size_of::<Node>();

/// A list node's fixed part. Its `h` links follow it in the same allocation, so a node is
/// `HEADER + 8h` bytes; they are reached only through a [`NodePtr`], whose pointer carries the
/// provenance of the whole chunk. Every field is an atomic so that a reader racing the writer reads
/// stale values, not undefined behaviour; the version tells it whether they belong together.
#[repr(C)]
struct Node {
    version: AtomicU64,
    born: AtomicU64,
    died: AtomicU64,
    slot: AtomicU32,
    /// Fixed when the node's chunk is made; never changes.
    height: AtomicU32,
}

const _: () = assert!(HEADER == 32, "a node's fixed part is 4 words");

impl Node {
    /// The writer starts changing this node: its version goes odd before any field changes.
    fn begin_write(&self) {
        let v = self.version.load(Ordering::Relaxed);
        debug_assert!(v % 2 == 0, "a node written twice at once");
        self.version.store(v + 1, Ordering::Relaxed);
        fence(Ordering::Release);
    }

    /// The writer is done: the version goes even after every field change.
    fn end_write(&self) {
        let v = self.version.load(Ordering::Relaxed);
        debug_assert!(v % 2 == 1, "ending a write that did not begin");
        self.version.store(v + 1, Ordering::Release);
    }

    /// Whether this node still has version `v`: the closing half of a seqlock read, after the
    /// fields were loaded.
    fn unchanged(&self, v: u64) -> bool {
        fence(Ordering::Acquire);
        self.version.load(Ordering::Relaxed) == v
    }
}

/// A pointer to a node, with the provenance of the allocation the node lives in.
#[derive(Clone, Copy, PartialEq, Eq)]
struct NodePtr(NonNull<Node>);

// SAFETY: a `NodePtr` points into memory the lists own until they drop; every access through it is
// an atomic load or store, and the writer's stores happen only under the trunk's lock.
unsafe impl Send for NodePtr {}
unsafe impl Sync for NodePtr {}

impl NodePtr {
    fn new(p: *mut Node) -> Option<Self> {
        NonNull::new(p).map(NodePtr)
    }

    fn raw(self) -> *mut Node {
        self.0.as_ptr()
    }

    /// The fixed part.
    fn hdr<'a>(self) -> &'a Node {
        // SAFETY: the node lives as long as the lists that handed out this pointer, and the
        // reference covers only the fixed part.
        unsafe { self.0.as_ref() }
    }

    /// Link `l`, which the node must have (`l < height`).
    fn link<'a>(self, l: usize) -> &'a AtomicPtr<Node> {
        // SAFETY: links follow the fixed part in the same allocation, and the pointer carries that
        // allocation's provenance; callers only ask for a level the node was reached at or linked
        // at, which is below its height (a node never changes height).
        unsafe {
            &*self
                .0
                .as_ptr()
                .cast::<u8>()
                .add(HEADER + l * std::mem::size_of::<AtomicPtr<Node>>())
                .cast::<AtomicPtr<Node>>()
        }
    }
}

/// Bytes of one node of height `h`.
fn node_size(h: usize) -> usize {
    HEADER + h * std::mem::size_of::<AtomicPtr<Node>>()
}

/// One zeroed allocation of `n` nodes of height `h`, each made free (odd version) with its height.
struct RawChunk {
    ptr: NonNull<u8>,
    layout: Layout,
}

// SAFETY: the chunk is owned memory, freed once in `Writer::drop`.
unsafe impl Send for RawChunk {}

impl RawChunk {
    fn new(h: usize, n: usize) -> Self {
        let layout = Layout::from_size_align(node_size(h) * n, std::mem::align_of::<Node>())
            .expect("a chunk's size fits a layout");
        // SAFETY: the layout is non-zero-sized; zeroed bytes are valid atomics and null links.
        let ptr = NonNull::new(unsafe { alloc_zeroed(layout) })
            .unwrap_or_else(|| handle_alloc_error(layout));
        let chunk = Self { ptr, layout };
        for i in 0..n {
            let node = chunk.node(h, i).hdr();
            node.version.store(1, Ordering::Relaxed);
            node.height.store(h as u32, Ordering::Relaxed);
        }
        chunk
    }

    fn node(&self, h: usize, i: usize) -> NodePtr {
        // SAFETY: `i` is below the chunk's node count, so the offset stays inside the allocation.
        NodePtr(unsafe {
            NonNull::new_unchecked(self.ptr.as_ptr().add(i * node_size(h)).cast::<Node>())
        })
    }
}

/// One height's pool.
#[derive(Default)]
struct Class {
    chunks: Vec<RawChunk>,
    free: Vec<NodePtr>,
    in_use: u64,
    /// The most nodes of this height ever in lists at once.
    peak: u64,
}

/// The writer's state: one pool per height (index `h - 1`) and the height generator.
struct Writer {
    classes: [Class; MAX_H],
    rng: u64,
}

impl Writer {
    fn alloc(&mut self, h: usize) -> NodePtr {
        let class = &mut self.classes[h - 1];
        if class.free.is_empty() {
            let chunk = RawChunk::new(h, CHUNK);
            class.free.extend((0..CHUNK).rev().map(|i| chunk.node(h, i)));
            class.chunks.push(chunk);
        }
        class.in_use += 1;
        class.peak = class.peak.max(class.in_use);
        class.free.pop().expect("just refilled")
    }

    fn free(&mut self, n: NodePtr, h: usize) {
        let class = &mut self.classes[h - 1];
        class.in_use -= 1;
        if !super::k3_mutant(6) {
            // M6 drops the node from the free list: no reuse.
            class.free.push(n);
        }
    }

    fn height(&mut self) -> usize {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        // p = 1/4: one more level per two trailing zero bits; E[h] = 4/3.
        (1 + (self.rng.trailing_zeros() as usize) / 2).min(MAX_H)
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        for class in &mut self.classes {
            for chunk in class.chunks.drain(..) {
                // SAFETY: allocated in `RawChunk::new` with this layout, freed once, here; no reader
                // can exist while the lists drop.
                unsafe { dealloc(chunk.ptr.as_ptr(), chunk.layout) };
            }
        }
    }
}

/// The head of one page's list: a node of the full height that is never freed and holds no
/// version of its own.
struct List {
    head: NodePtr,
    chunk: RawChunk,
}

impl Default for List {
    fn default() -> Self {
        let chunk = RawChunk::new(MAX_H, 1);
        let head = chunk.node(MAX_H, 0);
        // The head is never free: even from the start, with every level empty.
        head.hdr().version.store(0, Ordering::Relaxed);
        Self { head, chunk }
    }
}

impl Drop for List {
    fn drop(&mut self) {
        // SAFETY: allocated in `RawChunk::new` with this layout, freed once, here.
        unsafe { dealloc(self.chunk.ptr.as_ptr(), self.chunk.layout) };
    }
}

/// What a bounded optimistic search found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OlcLookup {
    Found(OlcVersion),
    /// No version of the page covers `f`.
    Absent,
    /// [`MAX_FAILED_LAPS`] attempts failed: the writer kept changing the path, or is stopped in the
    /// middle of a change. The caller answers under the trunk's lock.
    GaveUp,
}

/// One retained version, as the store keeps it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OlcVersion {
    pub(crate) born: u64,
    pub(crate) died: u64,
    pub(crate) slot: u32,
}

/// A consistent snapshot of the lists' accounting, taken under the writer mutex.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct OlcCounts {
    /// Readers' failed attempts and give-ups (see [`OlcLists`]).
    pub(crate) restarts: u64,
    pub(crate) head_spins: u64,
    pub(crate) fallbacks: u64,
    /// Nodes in lists (the list-side count) and the most at once.
    pub(crate) in_use: u64,
    pub(crate) peak_in_use: u64,
    /// The pools' own count of nodes handed out and not given back: equal to `in_use` unless the
    /// pools lost track of a removed node (never freed, or held back).
    pub(crate) pool_in_use: u64,
    /// Bytes of every node the pools hold, and the bytes whole chunks for each height's peak allow.
    pub(crate) pool_bytes: u64,
    pub(crate) pool_bound_bytes: u64,
    /// The most chunks any one height's pool holds.
    pub(crate) max_class_chunks: u64,
}

/// F-K3v's per-page version lists (see the module doc).
pub(crate) struct OlcLists {
    lists: Radix<std::sync::OnceLock<List>>,
    /// The writer's state. Only a caller holding the trunk's lock takes it, so it is never contended;
    /// it is here, not in the trunk's lock, so that the lists are one self-contained structure.
    writer: Mutex<Writer>,
    /// Nodes in lists, and the most there have been at once; written only by the writer.
    in_use: AtomicU64,
    peak: AtomicU64,
    /// Readers' failed attempts, each on its own line so readers stuck on a stopped writer do not
    /// share a line with each other's counters or with the writer's: validations that failed
    /// (`restarts`), laps that found the head odd (`head_spins`), and searches that gave up
    /// (`fallbacks`). Written only when an attempt fails.
    restarts: CachePadded<AtomicU64>,
    head_spins: CachePadded<AtomicU64>,
    fallbacks: CachePadded<AtomicU64>,
    /// Failed attempts after which an unlocked search gives up: [`MAX_FAILED_LAPS`], or a test's
    /// choice (0 makes every search that has a list fall back to the trunk's lock).
    max_failed: u32,
}

// SAFETY: the node pointers inside are only dereferenced through `&self` methods whose writes happen
// under the trunk's lock (and the writer mutex) and whose reads are validated by versions; the memory
// they point at is owned by these lists until they drop.
unsafe impl Send for List {}
unsafe impl Sync for List {}

impl OlcLists {
    pub(crate) fn new() -> Self {
        Self::with_max_failed(MAX_FAILED_LAPS)
    }

    /// Lists whose unlocked searches give up after `max_failed` failed attempts.
    pub(crate) fn with_max_failed(max_failed: u32) -> Self {
        Self {
            max_failed,
            lists: Radix::new(),
            writer: Mutex::new(Writer {
                classes: std::array::from_fn(|_| Class::default()),
                rng: 0x9E37_79B9_7F4A_7C15,
            }),
            in_use: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            restarts: CachePadded::new(AtomicU64::new(0)),
            head_spins: CachePadded::new(AtomicU64::new(0)),
            fallbacks: CachePadded::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn nodes_in_use(&self) -> u64 {
        self.in_use.load(Ordering::Relaxed)
    }

    /// The most nodes the lists have held at once.
    pub(crate) fn peak_in_use(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    /// Every count at once, the pool side read under the writer mutex. The caller holds the trunk's
    /// lock for the list side to be exact with it.
    pub(crate) fn counts(&self) -> OlcCounts {
        let w = self.writer.lock();
        OlcCounts {
            restarts: self.restarts(),
            head_spins: self.head_spins(),
            fallbacks: self.fallbacks(),
            in_use: self.nodes_in_use(),
            peak_in_use: self.peak_in_use(),
            pool_in_use: w.classes.iter().map(|c| c.in_use).sum(),
            pool_bytes: Self::bytes(&w, |c| c.chunks.len() as u64),
            pool_bound_bytes: Self::bytes(&w, |c| c.peak.div_ceil(CHUNK as u64)),
            max_class_chunks: w.classes.iter().map(|c| c.chunks.len() as u64).max().unwrap_or(0),
        }
    }

    /// Bytes of `chunks(class)` chunks in every height's pool.
    fn bytes(w: &Writer, chunks: impl Fn(&Class) -> u64) -> u64 {
        (1..=MAX_H)
            .map(|h| chunks(&w.classes[h - 1]) * (CHUNK * node_size(h)) as u64)
            .sum()
    }

    /// The pools' count of nodes handed out and not given back (see [`OlcCounts::pool_in_use`]).
    #[cfg(test)]
    fn pool_in_use(&self) -> u64 {
        self.writer.lock().classes.iter().map(|c| c.in_use).sum()
    }

    /// Chunks per height's pool, index `h - 1`.
    #[cfg(test)]
    fn class_chunks(&self) -> [usize; MAX_H] {
        let w = self.writer.lock();
        std::array::from_fn(|i| w.classes[i].chunks.len())
    }

    /// Bytes of every node the pools hold, in use or free.
    pub(crate) fn pool_bytes(&self) -> u64 {
        let w = self.writer.lock();
        (1..=MAX_H)
            .map(|h| (w.classes[h - 1].chunks.len() * CHUNK * node_size(h)) as u64)
            .sum()
    }

    /// The most the pools may hold if every removed node is reused: for each height, whole chunks
    /// enough for that height's peak. A pool above this grew while it had free nodes of its height.
    #[cfg(test)]
    fn pool_bound_bytes(&self) -> u64 {
        let w = self.writer.lock();
        (1..=MAX_H)
            .map(|h| (w.classes[h - 1].peak.div_ceil(CHUNK as u64) * (CHUNK * node_size(h)) as u64))
            .sum()
    }

    pub(crate) fn restarts(&self) -> u64 {
        self.restarts.load(Ordering::Relaxed)
    }

    pub(crate) fn head_spins(&self) -> u64 {
        self.head_spins.load(Ordering::Relaxed)
    }

    pub(crate) fn fallbacks(&self) -> u64 {
        self.fallbacks.load(Ordering::Relaxed)
    }

    fn list(&self, page: u32) -> Option<&List> {
        self.lists.get(page)?.get()
    }

    /// The writer's descent: for every level, the last node whose `born` is below `born` (the head
    /// if none). The writer is the only one changing links, so it reads them without validation.
    fn preds(list: &List, born: u64) -> [NodePtr; MAX_H] {
        let mut preds = [list.head; MAX_H];
        let mut x = list.head;
        for l in (0..MAX_H).rev() {
            while let Some(n) = NodePtr::new(x.link(l).load(Ordering::Acquire)) {
                if n.hdr().born.load(Ordering::Relaxed) >= born {
                    break;
                }
                x = n;
            }
            preds[l] = x;
        }
        preds
    }

    /// Link version `v` into `page`'s list. The caller holds the trunk's lock.
    pub(crate) fn insert(&self, page: u32, v: OlcVersion) {
        self.insert_at_height(page, v, None);
    }

    /// `insert` with the tower height drawn (`None`) or chosen (a test's way to make the next insert
    /// reuse a given node: a freed node goes back to its own height's pool, and that pool hands out
    /// last in, first out).
    fn insert_at_height(&self, page: u32, v: OlcVersion, height: Option<usize>) {
        let list = self
            .lists
            .get_or_insert(page)
            .get_or_init(List::default);
        let mut w = self.writer.lock();
        let h = height.unwrap_or_else(|| w.height());
        debug_assert!((1..=MAX_H).contains(&h));
        let n = w.alloc(h);
        drop(w);
        let preds = Self::preds(list, v.born);
        let node = n.hdr();
        // A fresh incarnation: fields first, under the free node's odd version, then even. The node
        // may have been freed by another thread (ordered with this one only by the trunk's lock),
        // so this thread's own release fence must precede its field stores for a reader that sees
        // a new field to see the odd (or a newer) version too (Boehm's seqlock argument).
        debug_assert!(node.version.load(Ordering::Relaxed) % 2 == 1, "reusing a node that is not free");
        debug_assert_eq!(node.height.load(Ordering::Relaxed) as usize, h);
        fence(Ordering::Release);
        node.born.store(v.born, Ordering::Relaxed);
        node.died.store(v.died, Ordering::Relaxed);
        node.slot.store(v.slot, Ordering::Relaxed);
        for (l, pred) in preds.iter().enumerate().take(h) {
            n.link(l)
                .store(pred.link(l).load(Ordering::Relaxed), Ordering::Relaxed);
        }
        node.end_write();
        for (l, pred) in preds.iter().enumerate().take(h) {
            pred.hdr().begin_write();
            pred.link(l).store(n.raw(), Ordering::Relaxed);
            pred.hdr().end_write();
        }
        let now = self.in_use.fetch_add(1, Ordering::Relaxed) + 1;
        if now > self.peak.load(Ordering::Relaxed) {
            self.peak.store(now, Ordering::Relaxed);
        }
    }

    /// Unlink the version of `page` born at `born` and return its node to its height's pool. The
    /// caller holds the trunk's lock.
    pub(crate) fn remove(&self, page: u32, born: u64) -> Option<OlcVersion> {
        let list = self.list(page)?;
        let preds = Self::preds(list, born);
        let n = NodePtr::new(preds[0].link(0).load(Ordering::Acquire))?;
        let node = n.hdr();
        if node.born.load(Ordering::Relaxed) != born {
            return None;
        }
        let v = OlcVersion {
            born,
            died: node.died.load(Ordering::Relaxed),
            slot: node.slot.load(Ordering::Relaxed),
        };
        let h = node.height.load(Ordering::Relaxed) as usize;
        // Top down, so a reader never reaches the node through a level it is already gone from
        // below; each predecessor's version moves, so a reader that followed it there restarts.
        for l in (0..h).rev() {
            let pred = preds[l];
            debug_assert!(ptr::eq(pred.link(l).load(Ordering::Relaxed), n.raw()));
            pred.hdr().begin_write();
            pred.link(l)
                .store(n.link(l).load(Ordering::Relaxed), Ordering::Relaxed);
            pred.hdr().end_write();
        }
        // Free: odd, and so refused by any reader that still holds it.
        node.begin_write();
        if !super::k3_mutant(5) {
            // M5 deletes this free.
            self.writer.lock().free(n, h);
        }
        self.in_use.fetch_sub(1, Ordering::Relaxed);
        Some(v)
    }

    /// The newest version of `page`. The caller holds the trunk's lock.
    pub(crate) fn last(&self, page: u32) -> Option<OlcVersion> {
        let list = self.list(page)?;
        let x = Self::preds(list, u64::MAX)[0];
        if x == list.head {
            return None;
        }
        let node = x.hdr();
        Some(OlcVersion {
            born: node.born.load(Ordering::Relaxed),
            died: node.died.load(Ordering::Relaxed),
            slot: node.slot.load(Ordering::Relaxed),
        })
    }

    /// The version of `page` a child forked at `f` sees — the born-predecessor of `f`, if it was
    /// still current at `f` — without a lock and without writing anything shared (except a failed
    /// attempt's count), or [`OlcLookup::GaveUp`] after [`MAX_FAILED_LAPS`] failed attempts.
    pub(crate) fn covering(&self, page: u32, f: u64) -> OlcLookup {
        self.covering_with(page, f, Some(self.max_failed), |_| {}, |_| {})
    }

    /// `covering` for a caller that holds the trunk's lock: no writer can run, so the first attempt
    /// succeeds. Bounded all the same: a node left odd under the lock (an invariant broken) panics
    /// here instead of spinning for ever with the trunk's lock held.
    pub(crate) fn covering_locked(&self, page: u32, f: u64) -> Option<OlcVersion> {
        let found = self.covering_with(page, f, Some(LOCKED_MAX_FAILED), |_| {}, |_| {});
        crate::turso_assert!(
            found != OlcLookup::GaveUp,
            "a search under the trunk's lock failed 2^20 attempts: a list node was left mid-change"
        );
        match found {
            OlcLookup::Found(v) => Some(v),
            OlcLookup::Absent => None,
            OlcLookup::GaveUp => unreachable!("asserted above"),
        }
    }

    /// `covering`, calling `before_check` with each pointer it reads before the node it was read from
    /// is re-validated, and `before_deref` with each non-null pointer after that check and before the
    /// node it points to is read: the tests' way to change the list at exactly the moment a reader
    /// must notice. `max_failed` bounds the failed attempts (`None`: unbounded).
    fn covering_with(
        &self,
        page: u32,
        f: u64,
        max_failed: Option<u32>,
        mut before_check: impl FnMut(*const Node),
        mut before_deref: impl FnMut(*const Node),
    ) -> OlcLookup {
        let Some(list) = self.list(page) else {
            return OlcLookup::Absent;
        };
        let mut failed = 0u32;
        'restart: loop {
            if max_failed.is_some_and(|k| failed >= k) {
                self.fallbacks.fetch_add(1, Ordering::Relaxed);
                return OlcLookup::GaveUp;
            }
            let mut x = list.head;
            let mut xv = x.hdr().version.load(Ordering::Acquire);
            if xv % 2 == 1 {
                // The writer is changing the head's links right now.
                self.head_spins.fetch_add(1, Ordering::Relaxed);
                failed += 1;
                std::hint::spin_loop();
                continue 'restart;
            }
            let mut level = MAX_H;
            while level > 0 {
                let l = level - 1;
                let next = x.link(l).load(Ordering::Relaxed);
                before_check(next.cast_const());
                if !x.hdr().unchanged(xv) {
                    self.restarts.fetch_add(1, Ordering::Relaxed);
                    failed += 1;
                    continue 'restart;
                }
                let Some(n) = NodePtr::new(next) else {
                    level -= 1;
                    continue;
                };
                before_deref(next.cast_const());
                // `n` points at a node, possibly a newer incarnation in another list (type-stable
                // pools); the versions decide. It has more than `l` links: it was linked at level
                // `l`, and a node never changes height.
                let nv = n.hdr().version.load(Ordering::Acquire);
                let born = n.hdr().born.load(Ordering::Relaxed);
                // `n` belongs to this read only if it was not changed while its `born` was read (n
                // unchanged and even) and was still `x`'s successor after that (x unchanged).
                // M4: drop the source node's re-check after reading `n`.
                if nv % 2 == 1
                    || !n.hdr().unchanged(nv)
                    || (!super::k3_mutant(4) && !x.hdr().unchanged(xv))
                {
                    self.restarts.fetch_add(1, Ordering::Relaxed);
                    failed += 1;
                    continue 'restart;
                }
                if born <= f {
                    x = n;
                    xv = nv;
                } else {
                    level -= 1;
                }
            }
            if x == list.head {
                return OlcLookup::Absent;
            }
            let node = x.hdr();
            let v = OlcVersion {
                born: node.born.load(Ordering::Relaxed),
                died: node.died.load(Ordering::Relaxed),
                slot: node.slot.load(Ordering::Relaxed),
            };
            if !node.unchanged(xv) {
                self.restarts.fetch_add(1, Ordering::Relaxed);
                failed += 1;
                continue 'restart;
            }
            return if f < v.died {
                OlcLookup::Found(v)
            } else {
                OlcLookup::Absent
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(born: u64, died: u64) -> OlcVersion {
        OlcVersion {
            born,
            died,
            slot: born as u32 * 10,
        }
    }

    fn found(l: OlcLookup) -> Option<OlcVersion> {
        match l {
            OlcLookup::Found(v) => Some(v),
            OlcLookup::Absent => None,
            OlcLookup::GaveUp => panic!("a search with no writer running gave up"),
        }
    }

    /// The `born` a raw pointer's node holds now, for the hooks.
    fn born_of(p: *const Node) -> Option<u64> {
        NodePtr::new(p.cast_mut()).map(|n| n.hdr().born.load(Ordering::Relaxed))
    }

    /// Inserts, removals and the predecessor query against a BTreeMap model, including node reuse
    /// across pages: every removed node goes back to its height's pool and the next insert of that
    /// height, in any page, takes it. The pools never exceed whole chunks for each height's peak.
    #[test]
    fn lists_match_a_model_with_immediate_reuse() {
        let lists = OlcLists::new();
        let mut model: Vec<std::collections::BTreeMap<u64, OlcVersion>> = vec![Default::default(); 3];
        let mut rng = 0x2545_F491_4F6C_DD1Du64;
        let mut next_born = [1u64; 3];
        for step in 0..20_000 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let page = (rng % 3) as usize;
            if rng % 5 < 3 {
                let born = next_born[page];
                next_born[page] += 1 + (rng >> 8) % 3;
                let ver = v(born, next_born[page]);
                lists.insert(page as u32, ver);
                model[page].insert(born, ver);
            } else if let Some(&born) = model[page].keys().nth(((rng >> 16) as usize) % model[page].len().max(1)) {
                assert_eq!(lists.remove(page as u32, born), model[page].remove(&born), "step {step}");
            }
            for (p, m) in model.iter().enumerate() {
                assert_eq!(lists.last(p as u32), m.values().next_back().copied(), "step {step}");
                for f in [0, 1, next_born[p] / 2, next_born[p].saturating_sub(1), next_born[p]] {
                    let want = m.range(..=f).next_back().map(|(_, v)| *v).filter(|v| f < v.died);
                    assert_eq!(found(lists.covering(p as u32, f)), want, "step {step} page {p} f {f}");
                    assert_eq!(lists.covering_locked(p as u32, f), want, "step {step} page {p} f {f}");
                }
            }
        }
        let live: u64 = model.iter().map(|m| m.len() as u64).sum();
        assert_eq!(lists.nodes_in_use(), live);
        assert_eq!(lists.pool_in_use(), live, "the pools lost track of removed nodes");
        assert_eq!(lists.counts().pool_in_use, lists.counts().in_use);
        assert!(lists.peak_in_use() >= live);
        assert!(lists.pool_bytes() <= lists.pool_bound_bytes(), "a pool grew while it had free nodes");
        assert_eq!(
            (lists.restarts(), lists.head_spins(), lists.fallbacks()),
            (0, 0, 0),
            "no writer ran during a read"
        );
    }

    /// Page 0 holds born 10, 20, ..., 80 (died born + 10), every node of height 1, so all eight share
    /// one pool and the reader walks level 0. Inside `hook`'s moment (when the reader holds a pointer
    /// to born 50) born 50 is removed and its node reused at once for page 1 as (52, 1000) — height 1
    /// too, so its pool hands node 50 straight back — and the hook asserts that reuse by pointer.
    fn reuse_fifty(lists: &OlcLists, next: *const Node, fired: &mut bool) {
        if *fired || born_of(next) != Some(50) {
            return;
        }
        *fired = true;
        assert!(lists.remove(0, 50).is_some());
        lists.insert_at_height(1, v(52, 1_000), Some(1));
        let first = lists.list(1).expect("page 1 has a list").head.link(0).load(Ordering::Relaxed);
        assert!(ptr::eq(first, next), "page 1's new node is not the node the reader holds");
    }

    fn fifty_list() -> OlcLists {
        let lists = OlcLists::new();
        for born in 1..=8 {
            lists.insert_at_height(0, v(born * 10, born * 10 + 10), Some(1));
        }
        lists
    }

    /// The first hook, before the source node's re-check: born 50 is removed and its node reused
    /// in page 1 below f (see `reuse_fifty`). The removal changes the reader's source node (born 40),
    /// so the reader restarts at its first check and answers from the list as it now is.
    #[test]
    fn a_reader_restarts_when_its_next_node_is_freed_and_reused_under_it() {
        let lists = fifty_list();
        let mut fired = false;
        let got = lists.covering_with(0, 55, None, |next| reuse_fifty(&lists, next, &mut fired), |_| {});
        assert!(fired);
        assert!(lists.restarts() >= 1, "the reader did not notice its path change");
        assert_eq!(found(got), None, "born 50 is gone and born 40 died at 50 <= 55");
        assert_eq!(found(lists.covering(0, 45)), Some(v(40, 50)));
        assert_eq!(found(lists.covering(1, 53)), Some(v(52, 1_000)));
        assert_eq!(lists.pool_in_use(), lists.nodes_in_use());
    }

    /// The twin at the second hook, after the source node's first re-check and before the pointed-to
    /// node is read: the same removal and reuse there leave the reused node even and unchanged while
    /// the reader reads it, so only the re-check of the source node AFTER the pointed-to node's `born`
    /// is read can notice. A reader without that re-check (mutant M4) steps onto the reused node, now
    /// in page 1, and answers (52, 1000).
    #[test]
    fn a_reader_rechecks_its_source_after_reading_a_node_reused_under_it() {
        let lists = fifty_list();
        let mut fired = false;
        let got = lists.covering_with(0, 55, None, |_| {}, |next| reuse_fifty(&lists, next, &mut fired));
        assert!(fired);
        assert!(lists.restarts() >= 1, "the reader did not notice its path change");
        assert_eq!(found(got), None, "a reader that stepped onto the reused node answers (52, 1000)");
        assert_eq!(found(lists.covering(1, 53)), Some(v(52, 1_000)));
        assert_eq!(lists.pool_in_use(), lists.nodes_in_use());
    }

    /// A reader stalled inside a search holds nothing: while it is parked between reading a pointer
    /// and validating it, 3,000 versions are retained and removed in another page. At most nine
    /// nodes are ever live, so a pool that reuses needs one chunk per height; one that deferred the
    /// 3,000 frees past the stalled reader would need about three chunks of height 1. And the pools'
    /// own count of nodes handed out matches the lists' count. The reader then answers from its own
    /// page as it is.
    #[test]
    fn a_stalled_reader_holds_nothing() {
        let lists = OlcLists::new();
        for born in 1..=8 {
            lists.insert(0, v(born * 10, born * 10 + 10));
        }
        let mut fired = false;
        let got = lists.covering_with(
            0,
            55,
            None,
            |_| {
                if !fired {
                    fired = true;
                    for i in 0..3_000u64 {
                        lists.insert(1, v(100 + 2 * i, 101 + 2 * i));
                        assert_eq!(lists.remove(1, 100 + 2 * i), Some(v(100 + 2 * i, 101 + 2 * i)));
                    }
                }
            },
            |_| {},
        );
        assert!(fired);
        let chunks = lists.class_chunks();
        assert!(chunks.iter().all(|&c| c <= 1), "a pool grew past one chunk while a reader was stalled: {chunks:?}");
        assert_eq!(lists.pool_in_use(), lists.nodes_in_use(), "the pools lost track of removed nodes");
        assert!(lists.pool_bytes() <= lists.pool_bound_bytes());
        assert_eq!(found(got), Some(v(50, 60)));
        assert_eq!(lists.nodes_in_use(), 8);
    }

    /// A reader that keeps failing gives up after MAX_FAILED_LAPS attempts instead of spinning on a
    /// writer stopped mid-change: here the head is held odd, as a writer stopped between
    /// `begin_write` and `end_write` on it would leave it. The bounded search gives up and counts a
    /// fallback and 64 head spins; once the head is even again the search answers.
    #[test]
    fn a_reader_gives_up_on_a_writer_stopped_mid_change() {
        let lists = OlcLists::new();
        lists.insert(0, v(10, 20));
        let head = lists.list(0).expect("page 0 has a list").head;
        head.hdr().begin_write();
        assert_eq!(lists.covering(0, 15), OlcLookup::GaveUp);
        assert_eq!(lists.fallbacks(), 1);
        assert_eq!(lists.head_spins(), u64::from(MAX_FAILED_LAPS));
        head.hdr().end_write();
        assert_eq!(found(lists.covering(0, 15)), Some(v(10, 20)));
    }
}
