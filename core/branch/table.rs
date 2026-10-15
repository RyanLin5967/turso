//! The branch table: branch states by id, in fixed-size chunks that never move. F8′ -- r11-ever's F8
//! (turso 5d580203f, "the branch table is a slot map ... chunks that never move, so churn leaves no
//! tombstones and never rehashes"), ported to the durable store for the githost-shape lane WITHOUT F8's
//! generational id recycling: the durable store's ids are minted from `next_id`, persisted in its log
//! and snapshot, and never reused, so the table indexes chunks by the id itself.
//!
//! Why: `std::collections::HashMap` grows by reallocating and moving EVERY entry, under the store's one
//! mutex. In the git-hosting shape the table only grows (refs are never deleted), so every doubling is a
//! fork that stalls for Theta(N): githost-shape measured one fork moving 458,125 branch states at
//! N = 5 x 10^5 (raw/port2_probeT_1024_500000.txt). Here growing allocates one chunk and, at worst,
//! reallocates the chunk DIRECTORY (N / 1024 pointers); no branch state ever moves. A chunk whose
//! states are all gone is freed.
//!
//! L2 slice (fastest-engine, budget create_allocates_no_table_of_every_branch_under_the_store_mutex,
//! 64 KiB per create under the store mutex): a chunk of 1024 branch states was 376,832 bytes
//! (artie raw l8-chunk_1c58a6f25.txt), allocated whole by the create that first needs it. A chunk is
//! now CHUNK = 128 states (47,104 bytes for a 368-byte state), and the directory is TWO-LEVEL
//! (r11-adversarial's F-R1b): fixed segments of SEG = 1024 chunk slots (24 KiB each) behind a top
//! `Vec` of segments, so no create allocates more than one chunk, one segment, or a top doubling of
//! N / 131,072 pointers (8 at 10^6). `growth()` counts the top's doublings.

use super::BranchId;

pub(crate) const CHUNK: usize = 128;
/// Chunk slots per directory segment.
const SEG: usize = 1024;

struct Chunk<T> {
    entries: Box<[Option<(BranchId, T)>]>,
    live: usize,
}

type Segment<T> = Box<[Option<Chunk<T>>]>;

pub(crate) struct BranchTable<T> {
    dir: Vec<Option<Segment<T>>>,
    len: usize,
    chunks: usize,
    /// Directory reallocations, and the chunk pointers they moved (observation only).
    dir_grows: u64,
    dir_moved: u64,
    /// Chunks allocated since the table was made (r13-compose I6, observation only).
    chunk_allocs: u64,
}

impl<T> BranchTable<T> {
    pub(crate) fn new() -> Self {
        Self {
            dir: Vec::new(),
            len: 0,
            chunks: 0,
            dir_grows: 0,
            dir_moved: 0,
            chunk_allocs: 0,
        }
    }

    /// (segment, chunk within it, slot within the chunk).
    fn at(id: &BranchId) -> (usize, usize, usize) {
        let i = usize::try_from(id.0).expect("a branch id fits usize");
        let c = i / CHUNK;
        (c / SEG, c % SEG, i % CHUNK)
    }

    pub(crate) fn get(&self, id: &BranchId) -> Option<&T> {
        let (g, c, i) = Self::at(id);
        self.dir.get(g)?.as_ref()?[c].as_ref()?.entries[i].as_ref().map(|(_, v)| v)
    }

    pub(crate) fn get_mut(&mut self, id: &BranchId) -> Option<&mut T> {
        let (g, c, i) = Self::at(id);
        self.dir.get_mut(g)?.as_mut()?[c].as_mut()?.entries[i].as_mut().map(|(_, v)| v)
    }

    pub(crate) fn contains_key(&self, id: &BranchId) -> bool {
        self.get(id).is_some()
    }

    pub(crate) fn insert(&mut self, id: BranchId, val: T) -> Option<T> {
        let (g, c, i) = Self::at(&id);
        if g >= self.dir.len() {
            let (len, cap) = (self.dir.len(), self.dir.capacity());
            self.dir.resize_with(g + 1, || None);
            if self.dir.capacity() != cap {
                self.dir_grows += 1;
                self.dir_moved += len as u64;
            }
        }
        let segment = self.dir[g].get_or_insert_with(|| (0..SEG).map(|_| None).collect());
        let chunk = segment[c].get_or_insert_with(|| Chunk {
            entries: (0..CHUNK).map(|_| None).collect(),
            live: 0,
        });
        let old = chunk.entries[i].replace((id, val)).map(|(_, v)| v);
        if old.is_none() {
            if chunk.live == 0 {
                self.chunks += 1;
                self.chunk_allocs += 1;
            }
            chunk.live += 1;
            self.len += 1;
        }
        old
    }

    pub(crate) fn remove(&mut self, id: &BranchId) -> Option<T> {
        let (g, c, i) = Self::at(id);
        let segment = self.dir.get_mut(g)?.as_mut()?;
        let chunk = segment[c].as_mut()?;
        let (_, val) = chunk.entries[i].take()?;
        chunk.live -= 1;
        self.len -= 1;
        if chunk.live == 0 {
            segment[c] = None;
            self.chunks -= 1;
            // A segment whose chunks are all gone is freed too.
            if segment.iter().all(Option::is_none) {
                self.dir[g] = None;
            }
        }
        Some(val)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Slots in allocated chunks.
    pub(crate) fn capacity(&self) -> usize {
        self.chunks * CHUNK
    }

    /// `(chunks allocated now, chunks allocated since the table was made)` (r13-compose I6).
    pub(crate) fn chunk_stats(&self) -> (u64, u64) {
        (self.chunks as u64, self.chunk_allocs)
    }

    /// Test builds: chunks allocated now.
    #[cfg(test)]
    pub(crate) fn chunks_for_test(&self) -> usize {
        self.chunks
    }

    /// `(top directory reallocations, segment pointers they moved)` since the table was made.
    pub(crate) fn growth(&self) -> (u64, u64) {
        (self.dir_grows, self.dir_moved)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&BranchId, &T)> {
        self.dir
            .iter()
            .flatten()
            .flat_map(|g| g.iter().flatten())
            .flat_map(|c| c.entries.iter().flatten().map(|(id, v)| (id, v)))
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &T> {
        self.iter().map(|(_, v)| v)
    }

    /// `iter`, adding to `scanned` every directory entry and every chunk slot the walk visits, empty
    /// or not, as far as the walk is driven (r13-compose: A2.F5's `walk_slots_scanned`, reported
    /// for B_ALL only; review wf_5c230f31 M: a walk's cost is the slots, not the live items).
    pub(crate) fn iter_scanned<'a>(
        &'a self,
        scanned: &'a std::cell::Cell<u64>,
    ) -> impl Iterator<Item = (&'a BranchId, &'a T)> + 'a {
        self.dir
            .iter()
            .flatten()
            .flat_map(|g| g.iter())
            .inspect(move |_| scanned.set(scanned.get() + 1))
            .flatten()
            .flat_map(move |c| {
                c.entries
                    .iter()
                    .inspect(move |_| scanned.set(scanned.get() + 1))
                    .flatten()
                    .map(|(id, v)| (id, v))
            })
    }
}

impl<'a, T> IntoIterator for &'a BranchTable<T> {
    type Item = (&'a BranchId, &'a T);
    type IntoIter = Box<dyn Iterator<Item = (&'a BranchId, &'a T)> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

/// As `HashMap`'s: panics when the id has no state.
impl<T> std::ops::Index<&BranchId> for BranchTable<T> {
    type Output = T;

    fn index(&self, id: &BranchId) -> &T {
        self.get(id).expect("no branch state for this id")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Against a BTreeMap model under inserts and removes, including whole chunks emptied and refilled;
    /// iteration is ascending by id; no state moves (a pointer taken before thousands of inserts still
    /// names the same state).
    #[test]
    fn a_branch_table_matches_a_model_and_never_moves_a_state() {
        let mut t: BranchTable<u64> = BranchTable::new();
        let mut m: BTreeMap<u64, u64> = BTreeMap::new();
        t.insert(BranchId(1), 11);
        m.insert(1, 11);
        let pinned = t.get(&BranchId(1)).unwrap() as *const u64;
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        for step in 2..60_000u64 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            t.insert(BranchId(step), step * 3);
            m.insert(step, step * 3);
            if x % 3 == 0 && step > 2 {
                let victim = 2 + x % (step - 1);
                assert_eq!(t.remove(&BranchId(victim)), m.remove(&victim));
            }
            if step % 7_000 == 0 {
                // Empty a whole chunk, then refill part of it.
                for id in (step - 3_000)..(step - 1_000) {
                    assert_eq!(t.remove(&BranchId(id)), m.remove(&id));
                }
            }
        }
        assert_eq!(t.get(&BranchId(1)).unwrap() as *const u64, pinned, "a state moved");
        assert_eq!(t.len(), m.len());
        let got: Vec<(u64, u64)> = t.iter().map(|(id, &v)| (id.0, v)).collect();
        let want: Vec<(u64, u64)> = m.iter().map(|(&k, &v)| (k, v)).collect();
        assert_eq!(got, want);
        for id in 0..60_100u64 {
            assert_eq!(t.get(&BranchId(id)).copied(), m.get(&id).copied());
        }
        assert!(t.capacity() >= t.len());
    }

    /// The two-level directory (L2 slice): ids across several segments (CHUNK x SEG = 131,072 ids
    /// each), a whole segment emptied (freed) and refilled, sparse ids far apart; against the model,
    /// with every chunk this table allocates at most 64 KiB for a 368-byte state.
    #[test]
    fn a_branch_table_spans_segments_and_frees_an_empty_one() {
        let span = (CHUNK * SEG) as u64;
        let mut t: BranchTable<u64> = BranchTable::new();
        let mut m: BTreeMap<u64, u64> = BTreeMap::new();
        let ids: Vec<u64> = (0..300).map(|k| k * 1_009).chain([span - 1, span, span + 1, 3 * span + 7, 10 * span]).collect();
        for &id in &ids {
            assert_eq!(t.insert(BranchId(id), id + 5), m.insert(id, id + 5));
        }
        // Empty segment 1 (it held span, span + 1, and the k * 1009 ids in it), then refill one id.
        let seg1: Vec<u64> = m.keys().copied().filter(|id| id / span == 1).collect();
        assert!(!seg1.is_empty(), "premise: segment 1 holds ids");
        for id in &seg1 {
            assert_eq!(t.remove(&BranchId(*id)), m.remove(id));
        }
        assert!(t.dir.get(1).is_none_or(Option::is_none), "an empty segment was not freed");
        assert_eq!(t.insert(BranchId(span + 2), 9), m.insert(span + 2, 9));
        let got: Vec<(u64, u64)> = t.iter().map(|(id, &v)| (id.0, v)).collect();
        let want: Vec<(u64, u64)> = m.iter().map(|(&k, &v)| (k, v)).collect();
        assert_eq!(got, want);
        for &id in ids.iter().chain([span + 2, 2 * span, 11 * span].iter()) {
            assert_eq!(t.get(&BranchId(id)).copied(), m.get(&id).copied(), "id {id}");
        }
        assert_eq!(t.len(), m.len());
        assert!(CHUNK * 368 <= 64 << 10, "premise: a chunk of 368-byte states fits 64 KiB");
    }
}
