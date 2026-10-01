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
//! Residual, stated: the directory is a `Vec` and doubles, moving N / 1024 pointers at a doubling (977
//! at 10^6). r11-adversarial's F-R1b bounds that too, with fixed segments behind a two-level directory;
//! at this lane's N it is a few microseconds, and `growth()` counts it.

use super::BranchId;

const CHUNK: usize = 1024;

struct Chunk<T> {
    entries: Box<[Option<(BranchId, T)>]>,
    live: usize,
}

pub(crate) struct BranchTable<T> {
    dir: Vec<Option<Chunk<T>>>,
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

    fn at(id: &BranchId) -> (usize, usize) {
        let i = usize::try_from(id.0).expect("a branch id fits usize");
        (i / CHUNK, i % CHUNK)
    }

    pub(crate) fn get(&self, id: &BranchId) -> Option<&T> {
        let (c, i) = Self::at(id);
        self.dir.get(c)?.as_ref()?.entries[i].as_ref().map(|(_, v)| v)
    }

    pub(crate) fn get_mut(&mut self, id: &BranchId) -> Option<&mut T> {
        let (c, i) = Self::at(id);
        self.dir.get_mut(c)?.as_mut()?.entries[i].as_mut().map(|(_, v)| v)
    }

    pub(crate) fn contains_key(&self, id: &BranchId) -> bool {
        self.get(id).is_some()
    }

    pub(crate) fn insert(&mut self, id: BranchId, val: T) -> Option<T> {
        let (c, i) = Self::at(&id);
        if c >= self.dir.len() {
            let (len, cap) = (self.dir.len(), self.dir.capacity());
            self.dir.resize_with(c + 1, || None);
            if self.dir.capacity() != cap {
                self.dir_grows += 1;
                self.dir_moved += len as u64;
            }
        }
        let chunk = self.dir[c].get_or_insert_with(|| Chunk {
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
        let (c, i) = Self::at(id);
        let chunk = self.dir.get_mut(c)?.as_mut()?;
        let (_, val) = chunk.entries[i].take()?;
        chunk.live -= 1;
        self.len -= 1;
        if chunk.live == 0 {
            self.dir[c] = None;
            self.chunks -= 1;
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

    /// `(directory reallocations, chunk pointers they moved)` since the table was made.
    pub(crate) fn growth(&self) -> (u64, u64) {
        (self.dir_grows, self.dir_moved)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&BranchId, &T)> {
        self.dir
            .iter()
            .flatten()
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
}
