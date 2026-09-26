//! F-K3v (lane r11-k3-trunklock, PREREG amendment 3.2): the trunk's retained versions of each page as
//! a skip list with ONE writer and optimistic readers over type-stable nodes.
//!
//! F-K3 keeps these lists in crossbeam's lock-free skip list, whose removed nodes are freed by epoch
//! reclamation: a reader that stalls while pinned (descheduled, stopped) keeps every node removed
//! after it pinned, so the garbage grows with the stall. Here every write already happens under the
//! trunk's lock, so a list needs no lock-free WRITE protocol, only readers that take no lock and
//! hold nothing:
//!
//! * **Type-stable nodes.** Nodes come from a pool the lists own and are never returned to the
//!   allocator while the lists live; a removed node goes back to the pool at once and may be reused
//!   by the next insert, in any page's list (type-stable memory, Greenwald and Cheriton, OSDI 1996;
//!   immediate reuse checked by versions, as in VBR, Sheffi, Herlihy and Petrank, 2021). A pointer
//!   a reader holds therefore always points at a node, never at freed memory.
//! * **Versions.** Every node carries a version that the writer makes odd before it changes the node
//!   and even again after, and that only ever grows; a free node's version is odd. A reader reads a
//!   node between two loads of its version and trusts what it read only if they are equal and even —
//!   a seqlock per node, taken hand over hand down the list (optimistic lock coupling, Leis et al.,
//!   DaMoN 2016). Unlinking a node changes its predecessor, so a reader that followed the pointer
//!   finds the predecessor's version moved and starts again from the head.
//! * **Nothing held.** A stalled reader holds no epoch and no reference: its next validation fails
//!   and it restarts. Garbage is 0 whatever readers do; the pool's size is the most versions the
//!   lists ever held at once.
//!
//! A reader's answer — the version of a page a child forked at `f` sees — is validated like every
//! other hop, and it cannot go stale after that: the store keeps that version while the reading
//! branch lives (see the store's "Reads of rewritten trunk pages without the trunk's lock").

use std::ptr::{self, NonNull};
use std::sync::atomic::{fence, AtomicPtr, AtomicU32, AtomicU64, Ordering};

use crate::sync::Mutex;

use super::radix::Radix;

/// Tower height cap. With p = 1/4 a list of n versions needs about log4 n levels: 7 at the 12.7k
/// versions per page of a steady 10^6-branch trunk (r11-space P2), so 12 is ample.
const MAX_H: usize = 12;
/// Nodes allocated per pool refill.
const CHUNK: usize = 1024;

/// One retained version, as the store keeps it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OlcVersion {
    pub(crate) born: u64,
    pub(crate) died: u64,
    pub(crate) slot: u32,
}

/// A list node. Every field is an atomic so that a reader racing the writer reads stale values, not
/// undefined behaviour; the version tells it whether they belong together.
struct Node {
    version: AtomicU64,
    born: AtomicU64,
    died: AtomicU64,
    slot: AtomicU32,
    height: AtomicU32,
    next: [AtomicPtr<Node>; MAX_H],
}

impl Node {
    /// A free node: odd version, no links.
    fn free() -> Self {
        Self {
            version: AtomicU64::new(1),
            born: AtomicU64::new(0),
            died: AtomicU64::new(0),
            slot: AtomicU32::new(0),
            height: AtomicU32::new(0),
            next: std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
        }
    }

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

/// The head of one page's list: a node that is never freed and holds no version of its own.
struct List {
    head: Node,
}

impl Default for List {
    fn default() -> Self {
        let head = Node::free();
        // The head is never free: even from the start, with every level empty.
        head.version.store(0, Ordering::Relaxed);
        head.height.store(MAX_H as u32, Ordering::Relaxed);
        Self { head }
    }
}

/// A node pointer the writer keeps in the pool. Only the writer (under the trunk lock) uses it.
#[derive(Clone, Copy)]
struct NodePtr(NonNull<Node>);

// SAFETY: a `NodePtr` points into a chunk the pool owns and never frees while the lists live; only
// the writer, under the trunk lock (and so under the pool's mutex), dereferences it mutably.
unsafe impl Send for NodePtr {}

/// The writer's state: node chunks (never freed until the lists drop), the free nodes, and the
/// height generator.
struct Writer {
    chunks: Vec<Box<[Node]>>,
    free: Vec<NodePtr>,
    rng: u64,
}

impl Writer {
    fn alloc(&mut self) -> NodePtr {
        if self.free.is_empty() {
            let chunk: Box<[Node]> = (0..CHUNK).map(|_| Node::free()).collect();
            self.free
                .extend(chunk.iter().map(|n| NodePtr(NonNull::from(n))));
            self.chunks.push(chunk);
        }
        self.free.pop().expect("just refilled")
    }

    fn height(&mut self) -> usize {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        // p = 1/4: one more level per two trailing zero bits.
        (1 + (self.rng.trailing_zeros() as usize) / 2).min(MAX_H)
    }
}

/// F-K3v's per-page version lists (see the module doc).
pub(crate) struct OlcLists {
    lists: Radix<std::sync::OnceLock<List>>,
    /// The writer's state. Only a caller holding the trunk's lock takes it, so it is never contended;
    /// it is here, not in the trunk's lock, so that the lists are one self-contained structure.
    writer: Mutex<Writer>,
    /// Nodes in lists (allocated from the pool and not free), for the store's garbage count.
    in_use: AtomicU64,
    /// Reader restarts: validations that failed because the writer changed a node under a reader.
    /// Counted only on a restart, so a read that does not restart writes nothing shared.
    restarts: AtomicU64,
}

impl OlcLists {
    pub(crate) fn new() -> Self {
        Self {
            lists: Radix::new(),
            writer: Mutex::new(Writer {
                chunks: Vec::new(),
                free: Vec::new(),
                rng: 0x9E37_79B9_7F4A_7C15,
            }),
            in_use: AtomicU64::new(0),
            restarts: AtomicU64::new(0),
        }
    }

    pub(crate) fn nodes_in_use(&self) -> u64 {
        self.in_use.load(Ordering::Relaxed)
    }

    /// Bytes of every node the pool holds, in use or free.
    pub(crate) fn pool_bytes(&self) -> u64 {
        (self.writer.lock().chunks.len() * CHUNK * std::mem::size_of::<Node>()) as u64
    }

    pub(crate) fn restarts(&self) -> u64 {
        self.restarts.load(Ordering::Relaxed)
    }

    fn list(&self, page: u32) -> Option<&List> {
        self.lists.get(page)?.get()
    }

    /// The writer's descent: for every level, the last node whose `born` is below `born` (the head
    /// if none). The writer is the only one changing links, so it reads them without validation.
    fn preds<'a>(list: &'a List, born: u64) -> [&'a Node; MAX_H] {
        let mut preds = [&list.head; MAX_H];
        let mut x = &list.head;
        for l in (0..MAX_H).rev() {
            loop {
                let next = x.next[l].load(Ordering::Acquire);
                if next.is_null() {
                    break;
                }
                // SAFETY: linked nodes live in the pool's chunks.
                let n = unsafe { &*next };
                if n.born.load(Ordering::Relaxed) >= born {
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
        let list = self
            .lists
            .get_or_insert(page)
            .get_or_init(List::default);
        let mut w = self.writer.lock();
        let h = w.height();
        let NodePtr(p) = w.alloc();
        drop(w);
        // SAFETY: the node lives in a chunk these lists own until they drop.
        let n = unsafe { p.as_ref() };
        let preds = Self::preds(list, v.born);
        // A fresh incarnation: fields first, under the free node's odd version, then even.
        n.born.store(v.born, Ordering::Relaxed);
        n.died.store(v.died, Ordering::Relaxed);
        n.slot.store(v.slot, Ordering::Relaxed);
        n.height.store(h as u32, Ordering::Relaxed);
        for (l, pred) in preds.iter().enumerate() {
            let next = if l < h {
                pred.next[l].load(Ordering::Relaxed)
            } else {
                ptr::null_mut()
            };
            n.next[l].store(next, Ordering::Relaxed);
        }
        n.end_write();
        for (l, pred) in preds.iter().enumerate().take(h) {
            pred.begin_write();
            pred.next[l].store(ptr::from_ref(n).cast_mut(), Ordering::Relaxed);
            pred.end_write();
        }
        self.in_use.fetch_add(1, Ordering::Relaxed);
    }

    /// Unlink the version of `page` born at `born` and return its node to the pool. The caller
    /// holds the trunk's lock.
    pub(crate) fn remove(&self, page: u32, born: u64) -> Option<OlcVersion> {
        let list = self.list(page)?;
        let preds = Self::preds(list, born);
        let target = preds[0].next[0].load(Ordering::Acquire);
        if target.is_null() {
            return None;
        }
        // SAFETY: linked nodes live in the pool's chunks.
        let n = unsafe { &*target };
        if n.born.load(Ordering::Relaxed) != born {
            return None;
        }
        let v = OlcVersion {
            born,
            died: n.died.load(Ordering::Relaxed),
            slot: n.slot.load(Ordering::Relaxed),
        };
        let h = n.height.load(Ordering::Relaxed) as usize;
        // Top down, so a reader never reaches the node through a level it is already gone from
        // below; each predecessor's version moves, so a reader that followed it there restarts.
        for l in (0..h).rev() {
            let pred = preds[l];
            debug_assert!(ptr::eq(pred.next[l].load(Ordering::Relaxed), target));
            pred.begin_write();
            pred.next[l].store(n.next[l].load(Ordering::Relaxed), Ordering::Relaxed);
            pred.end_write();
        }
        // Free: odd, and so refused by any reader that still holds it.
        n.begin_write();
        self.writer.lock().free.push(NodePtr(NonNull::from(n)));
        self.in_use.fetch_sub(1, Ordering::Relaxed);
        Some(v)
    }

    /// The newest version of `page`. The caller holds the trunk's lock.
    pub(crate) fn last(&self, page: u32) -> Option<OlcVersion> {
        let list = self.list(page)?;
        let preds = Self::preds(list, u64::MAX);
        let x = preds[0];
        if ptr::eq(x, &list.head) {
            return None;
        }
        Some(OlcVersion {
            born: x.born.load(Ordering::Relaxed),
            died: x.died.load(Ordering::Relaxed),
            slot: x.slot.load(Ordering::Relaxed),
        })
    }

    /// The version of `page` a child forked at `f` sees — the born-predecessor of `f`, if it was
    /// still current at `f` — without a lock and without writing anything shared (except a
    /// restart's count).
    pub(crate) fn covering(&self, page: u32, f: u64) -> Option<OlcVersion> {
        self.covering_with(page, f, |_| {})
    }

    /// `covering`, calling `between` with each pointer it reads, before the read is validated: the
    /// tests' way to change the list at exactly the moment a reader must notice.
    fn covering_with(
        &self,
        page: u32,
        f: u64,
        mut between: impl FnMut(*const Node),
    ) -> Option<OlcVersion> {
        let list = self.list(page)?;
        'restart: loop {
            let mut x = &list.head;
            let mut xv = x.version.load(Ordering::Acquire);
            if xv % 2 == 1 {
                // The writer is changing the head's links right now.
                std::hint::spin_loop();
                continue 'restart;
            }
            let mut level = MAX_H;
            while level > 0 {
                let l = level - 1;
                let next = x.next[l].load(Ordering::Relaxed);
                between(next);
                if !x.unchanged(xv) {
                    self.restarts.fetch_add(1, Ordering::Relaxed);
                    continue 'restart;
                }
                if next.is_null() {
                    level -= 1;
                    continue;
                }
                // SAFETY: nodes are never freed while the lists live (type-stable pool), so `next`
                // points at a node, possibly a newer incarnation; the versions decide.
                let n = unsafe { &*next };
                let nv = n.version.load(Ordering::Acquire);
                let born = n.born.load(Ordering::Relaxed);
                // `n` belongs to this read only if it was `x`'s successor (x unchanged) and was not
                // changed while its `born` was read (n unchanged and even).
                if nv % 2 == 1 || !n.unchanged(nv) || !x.unchanged(xv) {
                    self.restarts.fetch_add(1, Ordering::Relaxed);
                    continue 'restart;
                }
                if born <= f {
                    x = n;
                    xv = nv;
                } else {
                    level -= 1;
                }
            }
            if ptr::eq(x, &list.head) {
                return None;
            }
            let v = OlcVersion {
                born: x.born.load(Ordering::Relaxed),
                died: x.died.load(Ordering::Relaxed),
                slot: x.slot.load(Ordering::Relaxed),
            };
            if !x.unchanged(xv) {
                self.restarts.fetch_add(1, Ordering::Relaxed);
                continue 'restart;
            }
            return (f < v.died).then_some(v);
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

    /// Inserts, removals and the predecessor query against a BTreeMap model, including node reuse
    /// across pages: every removed node goes back to the pool and the next insert, in any page,
    /// takes it.
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
                    assert_eq!(lists.covering(p as u32, f), want, "step {step} page {p} f {f}");
                }
            }
        }
        let live: u64 = model.iter().map(|m| m.len() as u64).sum();
        assert_eq!(lists.nodes_in_use(), live);
        assert_eq!(lists.restarts(), 0, "no writer ran during a read");
    }

    /// The validation, forced to fire: between reading a pointer and validating it, the reader's
    /// path is changed — the node it is about to step to is removed and its node reused in another
    /// page's list. The reader must restart (counted) and still answer from the list as it now is.
    #[test]
    fn a_reader_restarts_when_its_next_node_is_freed_and_reused_under_it() {
        let lists = OlcLists::new();
        for born in 1..=8 {
            lists.insert(0, v(born * 10, born * 10 + 10));
        }
        let mut fired = false;
        let got = lists.covering_with(0, 55, |next| {
            // SAFETY: nodes live as long as `lists`.
            let born = (!next.is_null()).then(|| unsafe { (*next).born.load(Ordering::Relaxed) });
            if !fired && born == Some(50) {
                fired = true;
                // The reader has just read a pointer to the version it wants (born 50): remove it
                // and reuse its node in another page's list before the reader validates.
                assert!(lists.remove(0, 50).is_some());
                lists.insert(1, v(1_000, 2_000));
            }
        });
        assert!(fired);
        assert!(lists.restarts() >= 1, "the reader did not notice its path change");
        assert_eq!(got, None, "born 50 is gone and born 40 died at 50 <= 55");
        assert_eq!(lists.covering(0, 45), Some(v(40, 50)));
        assert_eq!(lists.covering(1, 1_500), Some(v(1_000, 2_000)));
    }
}
