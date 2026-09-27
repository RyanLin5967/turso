//! The branch table: branch states by id, in a slot map.
//!
//! An id is a slot index and that slot's generation: `id = generation << 32 | slot`. A lookup indexes
//! the slot and compares the generation; freeing a slot bumps its generation and puts the slot on a
//! free list, so ids are recycled without ever naming two branches. Generational indices (the
//! slot-map pattern of ECS entity ids and the `slotmap` crate).
//!
//! Why not a hash map: `std::collections::HashMap` deletes by leaving a tombstone wherever the probe
//! window around the slot is full, and when tombstones use up the table's growth allowance an insert
//! REHASHES THE WHOLE TABLE in place. Under steady churn at a fixed number of branches that recurs
//! forever at a constant rate: lane r11-ever measured 41 in-place rehashes per 10^7 forks at 10^4
//! live branches, each a 0.41–0.73 ms fork stall under the store's one mutex (raw t_e1_10k). A slot map has no
//! probe sequence, so it has no tombstones and never rehashes.
//!
//! Slots live in fixed-size chunks that never move, so growing the table allocates one chunk and
//! moves only the chunk pointers: no insert copies the states already stored (the doubling rehash of
//! turso_sota's R1 moved every entry).
//!
//! A slot whose generation would wrap is retired, never reused: the dangerous state (a stale id
//! naming a new branch) is not representable. That costs one slot per 2^32 reuses of it.
//!
//! **Dense values (F9, lane r12-f9-shrink).** Before F9 each slot held its value inline, so after a
//! grow to 10^6 branches and a shrink to 10^3 the table kept 10^6 value-sized slots (~300 B each):
//! a chunk can be freed only when all its slots are vacant, and survivors of a random shrink keep
//! most chunks. Now the values sit packed in a separate array, the slot map's dense variant (the
//! packed array behind an ID lookup table, Frykholm, Bitsquid 2011; `slotmap::DenseSlotMap`): the
//! slot holds its generation and the value's dense index, and a removal moves the last value into
//! the hole, so values occupy `ceil(len / CHUNK)` chunks. Those chunks never move either (growth
//! allocates one), and an emptied last chunk is freed, except for one spare kept against a count
//! that oscillates across a chunk boundary (jemalloc's spare chunk).
//!
//! **The slots shrink too (lane r12-f9-shrink amendment 5).** A slot is 8 bytes (generation and
//! link), and a table that kept every slot it ever used would hold 8 bytes per branch of the peak.
//! So vacant slots are handed out lowest first from a bitmap (address-ordered first fit, as in the
//! arena), which gathers the occupied slots at the bottom, and chunks of slots at the top that hold
//! no occupied or retired slot are freed, keeping one vacant chunk above the highest used one (the
//! spare rule again), with the chunk list shrunk under hysteresis. A freed chunk's generations must
//! not be forgotten, or a stale id could name the next occupant: the table keeps one generation
//! floor, the largest generation any freed slot had reached, and every slot re-created above it
//! starts there. Every id ever issued for a freed slot carries a generation below its slot's
//! (each removal bumps it), so below the floor, and every id issued after carries the floor or
//! more: no stale id can match.

use super::BranchId;

const CHUNK: usize = 1024;

/// A link's top bit marks an occupied slot; the rest is then the value's dense index.
const OCCUPIED: u32 = 1 << 31;
/// The link of a vacant slot. Not an index any slot can have (slots stay below it).
const NO_SLOT: u32 = OCCUPIED - 1;
const WORDS: usize = CHUNK / 64;

struct Entry {
    /// The generation the next (or current) occupant's id carries. Starts at 1, so no id is 0
    /// (`BranchId::TRUNK`).
    generation: u32,
    /// Occupied: `OCCUPIED | dense index`. Vacant or retired: `NO_SLOT`.
    link: u32,
}

pub(crate) struct BranchTable<T> {
    /// Slots by index, in chunks that never move; unpinned chunks at the top are freed (`trim`).
    chunks: Vec<Box<[Entry]>>,
    /// Slots below this have been handed out at least once since their chunk was created.
    high_water: u32,
    /// One bit per slot of every chunk, set while the slot is below `high_water`, vacant and not
    /// retired: the set first fit searches.
    vacant_bits: Vec<u64>,
    vacant_count: usize,
    /// One bit per chunk, set while it has a vacant slot.
    chunk_has_vacant: Vec<u64>,
    /// No chunk below this has a vacant slot.
    first_vacant_chunk: usize,
    /// Per chunk, its occupied and retired slots: a chunk is freed only at 0.
    chunk_pinned: Vec<u16>,
    /// The generation a re-created slot starts at: 1, then the largest generation any freed slot
    /// had reached.
    gen_floor: u32,
    /// Values with the slot that owns each, packed: every chunk but the last holds exactly
    /// `CHUNK`, the last holds at least one. Each chunk is allocated with capacity `CHUNK` and never
    /// pushed past it, so it never reallocates.
    dense: Vec<Vec<(u32, T)>>,
    /// One empty value chunk kept allocated.
    spare: Option<Vec<(u32, T)>>,
    len: usize,
    /// Slots taken out of use because their generation reached `u32::MAX`.
    retired: u64,
}

impl<T> BranchTable<T> {
    pub(crate) fn new() -> Self {
        Self {
            chunks: Vec::new(),
            high_water: 0,
            vacant_bits: Vec::new(),
            vacant_count: 0,
            chunk_has_vacant: Vec::new(),
            first_vacant_chunk: 0,
            chunk_pinned: Vec::new(),
            gen_floor: 1,
            dense: Vec::new(),
            spare: None,
            len: 0,
            retired: 0,
        }
    }

    fn entry(&self, slot: u32) -> Option<&Entry> {
        self.chunks
            .get(slot as usize / CHUNK)
            .map(|c| &c[slot as usize % CHUNK])
    }

    fn entry_mut(&mut self, slot: u32) -> Option<&mut Entry> {
        self.chunks
            .get_mut(slot as usize / CHUNK)
            .map(|c| &mut c[slot as usize % CHUNK])
    }

    fn split(id: BranchId) -> (u32, u32) {
        (id.0 as u32, (id.0 >> 32) as u32)
    }

    /// The dense index of the value `id` names, if `id` is live.
    fn dense_index(&self, id: &BranchId) -> Option<usize> {
        let (slot, generation) = Self::split(*id);
        let e = self.entry(slot)?;
        (e.generation == generation && e.link & OCCUPIED != 0)
            .then_some((e.link & !OCCUPIED) as usize)
    }

    /// The lowest vacant slot below `high_water`, if any.
    fn lowest_vacant(&self) -> Option<u32> {
        if self.vacant_count == 0 {
            return None;
        }
        let mut w = self.first_vacant_chunk / 64;
        let mut mask = !0u64 << (self.first_vacant_chunk % 64);
        let c = loop {
            let x = self
                .chunk_has_vacant
                .get(w)
                .expect("vacant_count > 0, so some chunk has a vacant slot")
                & mask;
            if x != 0 {
                break w * 64 + x.trailing_zeros() as usize;
            }
            w += 1;
            mask = !0;
        };
        let words = &self.vacant_bits[c * WORDS..(c + 1) * WORDS];
        let i = words.iter().position(|&w| w != 0).expect("the chunk has a vacant slot");
        Some(((c * WORDS + i) * 64 + words[i].trailing_zeros() as usize) as u32)
    }

    fn set_vacant(&mut self, slot: u32, on: bool) {
        let (s, c) = (slot as usize, slot as usize / CHUNK);
        let b = 1u64 << (s % 64);
        if on {
            self.vacant_bits[s / 64] |= b;
            self.vacant_count += 1;
            self.chunk_has_vacant[c / 64] |= 1u64 << (c % 64);
            self.first_vacant_chunk = self.first_vacant_chunk.min(c);
        } else {
            self.vacant_bits[s / 64] &= !b;
            self.vacant_count -= 1;
            if self.vacant_bits[c * WORDS..(c + 1) * WORDS].iter().all(|&w| w == 0) {
                self.chunk_has_vacant[c / 64] &= !(1u64 << (c % 64));
            }
        }
    }

    /// The id the next `insert` must use.
    pub(crate) fn vacant_id(&mut self) -> BranchId {
        let slot = match self.lowest_vacant() {
            Some(slot) => slot,
            None => {
                let slot = self.high_water;
                crate::turso_assert!(slot < NO_SLOT, "the branch table has no slot left");
                if slot as usize / CHUNK == self.chunks.len() {
                    let generation = if super::mutant::on(12) { 1 } else { self.gen_floor };
                    let chunk: Vec<Entry> = (0..CHUNK)
                        .map(|_| Entry {
                            generation,
                            link: NO_SLOT,
                        })
                        .collect();
                    self.chunks.push(chunk.into_boxed_slice());
                    let n = self.chunks.len();
                    self.vacant_bits.resize(n * WORDS, 0);
                    self.chunk_has_vacant.resize(n.div_ceil(64), 0);
                    self.chunk_pinned.resize(n, 0);
                }
                slot
            }
        };
        let generation = self.entry(slot).expect("allocated above").generation;
        BranchId(u64::from(generation) << 32 | u64::from(slot))
    }

    pub(crate) fn insert(&mut self, id: BranchId, val: T) {
        crate::turso_assert!(id == self.vacant_id(), "insert with an id vacant_id did not issue");
        let (slot, _) = Self::split(id);
        if slot < self.high_water {
            self.set_vacant(slot, false);
        } else {
            self.high_water += 1;
        }
        self.chunk_pinned[slot as usize / CHUNK] += 1;
        crate::turso_assert!(self.len < NO_SLOT as usize, "the branch table is full");
        let d = self.len;
        if self.dense.last().is_none_or(|c| c.len() == CHUNK) {
            let chunk = self.spare.take().unwrap_or_else(|| Vec::with_capacity(CHUNK));
            self.dense.push(chunk);
        }
        self.dense.last_mut().expect("pushed above").push((slot, val));
        let e = self.entry_mut(slot).expect("vacant slot exists");
        crate::turso_assert!(e.link & OCCUPIED == 0, "vacant slot is occupied");
        e.link = OCCUPIED | d as u32;
        self.len += 1;
    }

    pub(crate) fn get(&self, id: &BranchId) -> Option<&T> {
        let d = self.dense_index(id)?;
        Some(&self.dense[d / CHUNK][d % CHUNK].1)
    }

    pub(crate) fn get_mut(&mut self, id: &BranchId) -> Option<&mut T> {
        let d = self.dense_index(id)?;
        Some(&mut self.dense[d / CHUNK][d % CHUNK].1)
    }

    pub(crate) fn contains_key(&self, id: &BranchId) -> bool {
        self.dense_index(id).is_some()
    }

    pub(crate) fn remove(&mut self, id: &BranchId) -> Option<T> {
        let d = self.dense_index(id)?;
        let (slot, _) = Self::split(*id);
        // Move the last value into the hole: one move, whatever the table's size.
        let last = self
            .dense
            .last_mut()
            .and_then(|c| c.pop())
            .expect("a live id has a value");
        let val = if d == self.len - 1 {
            last.1
        } else {
            let moved = last.0;
            let (owner, val) = std::mem::replace(&mut self.dense[d / CHUNK][d % CHUNK], last);
            #[cfg(test)]
            assert_eq!(owner, slot, "the value at the hole belongs to the removed slot");
            #[cfg(not(test))]
            let _ = owner;
            if !super::mutant::on(4) {
                self.entry_mut(moved).expect("a stored value's slot exists").link =
                    OCCUPIED | d as u32;
            }
            val
        };
        if self.dense.last().is_some_and(|c| c.is_empty()) {
            let empty = self.dense.pop().expect("checked above");
            if self.spare.is_none() {
                self.spare = Some(empty);
            }
            if self.dense.capacity() > 64 && self.dense.capacity() > 4 * self.dense.len() {
                self.dense.shrink_to(2 * self.dense.len());
            }
        }
        let e = self.entry_mut(slot).expect("a live id's slot exists");
        e.link = NO_SLOT;
        let vacant = match e.generation.checked_add(1) {
            _ if super::mutant::on(5) => true,
            Some(next) if next != u32::MAX => {
                e.generation = next;
                true
            }
            _ => {
                e.generation = u32::MAX;
                self.retired += 1;
                false
            }
        };
        if vacant {
            self.set_vacant(slot, true);
            self.chunk_pinned[slot as usize / CHUNK] -= 1;
        }
        self.len -= 1;
        self.trim();
        Some(val)
    }

    /// Free the chunks of slots at the top while the two highest hold no occupied or retired slot,
    /// raising the generation floor to the largest generation each freed chunk had reached.
    fn trim(&mut self) {
        let before = self.chunks.len();
        loop {
            let n = self.chunks.len();
            let unpinned = |i: usize| self.chunk_pinned[i] == 0;
            let droppable = if super::mutant::on(13) {
                n >= 2 && unpinned(n - 2)
            } else {
                n >= 2 && unpinned(n - 1) && unpinned(n - 2)
            };
            if !droppable {
                break;
            }
            let chunk = self.chunks.pop().expect("n >= 2");
            let top = chunk.iter().map(|e| e.generation).max().expect("a chunk has slots");
            self.gen_floor = self.gen_floor.max(top);
            let base = (n - 1) * CHUNK;
            let hw = (self.high_water as usize).min(base);
            self.vacant_count -= (hw..self.high_water as usize)
                .filter(|&s| self.vacant_bits[s / 64] & (1u64 << (s % 64)) != 0)
                .count();
            self.high_water = hw as u32;
            self.vacant_bits.truncate((n - 1) * WORDS);
            self.chunk_pinned.truncate(n - 1);
            self.chunk_has_vacant.truncate((n - 1).div_ceil(64));
            if (n - 1) % 64 != 0 {
                if let Some(last) = self.chunk_has_vacant.last_mut() {
                    *last &= (1u64 << ((n - 1) % 64)) - 1;
                }
            }
        }
        if self.chunks.len() < before {
            self.first_vacant_chunk = self.first_vacant_chunk.min(self.chunks.len());
            fn shrink<V>(v: &mut Vec<V>) {
                if v.capacity() > 64 && v.capacity() > 4 * v.len() {
                    v.shrink_to(2 * v.len());
                }
            }
            shrink(&mut self.chunks);
            shrink(&mut self.vacant_bits);
            shrink(&mut self.chunk_has_vacant);
            shrink(&mut self.chunk_pinned);
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Slots allocated (chunks × chunk size): up by a chunk as the table grows, down as `trim`
    /// frees the top.
    pub(crate) fn capacity(&self) -> usize {
        self.chunks.len() * CHUNK
    }

    pub(crate) fn retired(&self) -> u64 {
        self.retired
    }

    /// `(value bytes, index bytes, entry bytes, value chunks)`: the value chunks allocated
    /// (spare included) at `CHUNK` values each, and the slots with the chunk-pointer vectors.
    /// Observation only.
    pub(crate) fn bytes(&self) -> (usize, usize, usize, usize) {
        let entry = std::mem::size_of::<(u32, T)>();
        let chunks = self.dense.len() + usize::from(self.spare.is_some());
        (
            chunks * CHUNK * entry,
            self.chunks.len() * CHUNK * std::mem::size_of::<Entry>()
                + self.chunks.capacity() * std::mem::size_of::<Box<[Entry]>>()
                + (self.vacant_bits.capacity() + self.chunk_has_vacant.capacity())
                    * std::mem::size_of::<u64>()
                + self.chunk_pinned.capacity() * std::mem::size_of::<u16>()
                + self.dense.capacity() * std::mem::size_of::<Vec<(u32, T)>>(),
            entry,
            chunks,
        )
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (BranchId, &T)> {
        self.dense.iter().flatten().map(|(slot, v)| {
            let generation = self.entry(*slot).expect("a stored value's slot exists").generation;
            (BranchId(u64::from(generation) << 32 | u64::from(*slot)), v)
        })
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &T> {
        self.dense.iter().flatten().map(|(_, v)| v)
    }
}

#[cfg(test)]
impl<T> BranchTable<T> {
    /// Every structural invariant of the slots, by brute force. Test only.
    fn check_slots(&self) {
        let hw = self.high_water as usize;
        let n = self.chunks.len();
        assert!(hw <= n * CHUNK, "high_water above the chunks");
        let (mut vacant, mut occupied, mut lowest) = (0, 0, None);
        for c in 0..n {
            let mut pinned = 0;
            let mut has_vacant = false;
            for i in 0..CHUNK {
                let s = c * CHUNK + i;
                let e = &self.chunks[c][i];
                let occ = e.link & OCCUPIED != 0;
                let vac = self.vacant_bits[s / 64] & (1u64 << (s % 64)) != 0;
                if s >= hw {
                    assert!(!occ && !vac, "slot {s} above high_water is in use or vacant-listed");
                    continue;
                }
                if occ {
                    assert!(!vac, "slot {s} occupied and vacant");
                    let d = (e.link & !OCCUPIED) as usize;
                    assert_eq!(self.dense[d / CHUNK][d % CHUNK].0 as usize, s, "dense back link");
                    occupied += 1;
                    pinned += 1;
                } else if vac {
                    vacant += 1;
                    has_vacant = true;
                } else {
                    assert_eq!(e.generation, u32::MAX, "slot {s} neither occupied, vacant nor retired");
                    pinned += 1;
                }
            }
            assert_eq!(self.chunk_pinned[c] as usize, pinned, "chunk {c}: pinned count");
            let bit = self.chunk_has_vacant[c / 64] & (1u64 << (c % 64)) != 0;
            assert_eq!(bit, has_vacant, "chunk {c}: has-vacant bit");
            if has_vacant && lowest.is_none() {
                lowest = Some(c);
            }
        }
        assert_eq!(vacant, self.vacant_count, "vacant_count");
        assert_eq!(occupied, self.len, "len");
        if let Some(l) = lowest {
            assert!(self.first_vacant_chunk <= l, "first-fit start above the lowest vacant chunk");
        }
        assert!(
            !(n >= 2 && self.chunk_pinned[n - 1] == 0 && self.chunk_pinned[n - 2] == 0),
            "two unpinned chunks left on top (trim)"
        );
    }
}

impl<T> std::ops::Index<&BranchId> for BranchTable<T> {
    type Output = T;
    fn index(&self, id: &BranchId) -> &T {
        self.get(id).expect("no branch with this id")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct Rng(u64);
    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    /// Random inserts and removes against a `HashMap` model: every live id reads its own value,
    /// every removed id reads nothing (even after its slot is reused), ids never repeat, and no id
    /// is the trunk's.
    #[test]
    fn ids_are_recycled_without_ever_naming_two_values() {
        let mut t = BranchTable::new();
        let mut model: HashMap<BranchId, u64> = HashMap::new();
        let mut dead: Vec<BranchId> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for step in 0..20_000u64 {
            if model.is_empty() || rng.below(3) != 0 && model.len() < 3000 {
                let id = t.vacant_id();
                assert!(!id.is_trunk() && seen.insert(id), "step {step}: id {id:?} reused");
                t.insert(id, step);
                model.insert(id, step);
            } else {
                let k = *model.keys().nth(rng.below(model.len() as u64) as usize).unwrap();
                assert_eq!(t.remove(&k), model.remove(&k), "step {step}");
                assert_eq!(t.remove(&k), None, "step {step}: removed twice");
                dead.push(k);
            }
            assert_eq!(t.len(), model.len());
        }
        for (id, v) in &model {
            assert_eq!(t.get(id), Some(v));
        }
        for id in &dead {
            assert!(t.get(id).is_none() && !t.contains_key(id), "a removed id still resolves");
        }
        let mut listed: Vec<(BranchId, u64)> = t.iter().map(|(id, &v)| (id, v)).collect();
        let mut want: Vec<(BranchId, u64)> = model.into_iter().collect();
        listed.sort();
        want.sort();
        assert_eq!(listed, want);
        assert!(t.get(&BranchId::TRUNK).is_none());
    }

    /// A slot whose generation would wrap is retired rather than reused.
    #[test]
    fn a_slot_at_the_last_generation_is_retired() {
        let mut t: BranchTable<u8> = BranchTable::new();
        let id = t.vacant_id();
        t.insert(id, 1);
        t.chunks[0][0].generation = u32::MAX - 1;
        let old = BranchId(u64::from(u32::MAX - 1) << 32);
        assert_eq!(t.remove(&old), Some(1));
        assert_eq!(t.retired(), 1);
        let next = t.vacant_id();
        assert_ne!(next.0 as u32, 0, "the retired slot was handed out again");
    }
}

#[cfg(test)]
mod f9_tests {
    use super::*;
    use std::collections::HashMap;

    /// F9 T4: grow to a peak, remove all but a scattered few, churn, grow again. After every step:
    /// every live id reads its own value, every removed id reads nothing, iteration lists exactly
    /// the live set, and the values occupy at most `ceil(len / CHUNK)` chunks plus the spare.
    #[test]
    fn values_stay_packed_through_a_peak_and_a_shrink() {
        let mut t: BranchTable<u64> = BranchTable::new();
        let mut model: HashMap<BranchId, u64> = HashMap::new();
        let mut dead: Vec<BranchId> = Vec::new();
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let mut below = |n: usize| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x % n as u64) as usize
        };
        let check = |t: &BranchTable<u64>, model: &HashMap<BranchId, u64>, dead: &[BranchId]| {
            assert_eq!(t.len(), model.len());
            let (_, _, _, chunks) = t.bytes();
            assert!(
                chunks <= model.len().div_ceil(CHUNK) + 1,
                "{chunks} value chunks for {} values",
                model.len()
            );
            for (id, v) in model {
                assert_eq!(t.get(id), Some(v), "a live id lost its value");
            }
            for id in dead.iter().rev().take(64) {
                assert!(t.get(id).is_none(), "a removed id still resolves");
            }
            let mut listed: Vec<(BranchId, u64)> = t.iter().map(|(id, &v)| (id, v)).collect();
            let mut want: Vec<(BranchId, u64)> = model.iter().map(|(&k, &v)| (k, v)).collect();
            listed.sort();
            want.sort();
            assert_eq!(listed, want, "iteration is not the live set");
        };
        let mut next = 0u64;
        for (peak, keep) in [(5_000usize, 7usize), (3_000, 1), (6_000, 1_100), (70_000, 3)] {
            while model.len() < peak {
                let id = t.vacant_id();
                t.insert(id, next);
                model.insert(id, next);
                next += 1;
            }
            check(&t, &model, &dead);
            let peak_cap = t.dense.capacity();
            let mut ids: Vec<BranchId> = model.keys().copied().collect();
            ids.sort();
            while model.len() > keep {
                let id = ids.swap_remove(below(ids.len()));
                assert_eq!(t.remove(&id), model.remove(&id));
                dead.push(id);
                if model.len() % 257 == 0 {
                    check(&t, &model, &dead);
                }
            }
            check(&t, &model, &dead);
            if peak_cap > 64 {
                assert!(t.dense.capacity() <= 64, "the value-chunk list kept its peak capacity");
            }
            for _ in 0..2_000 {
                let id = ids.swap_remove(below(ids.len()));
                assert_eq!(t.remove(&id), model.remove(&id));
                dead.push(id);
                let n = t.vacant_id();
                t.insert(n, next);
                model.insert(n, next);
                ids.push(n);
                next += 1;
            }
            check(&t, &model, &dead);
        }
    }

    /// F9 T5 (amendment 5): the slots shrink. After a peak and a shrink to scattered survivors,
    /// churn and then retire the survivors: the slot chunks fall to at most the chunk of the
    /// highest occupied slot plus one vacant chunk; and no id issued at any time, before or after
    /// a trim, is ever issued again or resolves after its removal. The trims must raise the
    /// generation floor, or the walk proves nothing.
    #[test]
    fn slots_shrink_after_a_peak_and_no_id_is_ever_reissued() {
        let mut t: BranchTable<u64> = BranchTable::new();
        let mut model: HashMap<BranchId, u64> = HashMap::new();
        let mut issued = std::collections::HashSet::new();
        let mut dead: Vec<BranchId> = Vec::new();
        let mut x = 0xD1B5_4A32_D192_ED03u64;
        let mut below = |n: usize| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x % n as u64) as usize
        };
        let mut next = 0u64;
        let mut add = |t: &mut BranchTable<u64>, model: &mut HashMap<BranchId, u64>| {
            let id = t.vacant_id();
            assert!(issued.insert(id), "id {id:?} issued twice");
            t.insert(id, next);
            model.insert(id, next);
            next += 1;
            id
        };
        for round in 0..3 {
            while model.len() < 20_000 {
                add(&mut t, &mut model);
            }
            t.check_slots();
            let mut ids: Vec<BranchId> = model.keys().copied().collect();
            ids.sort();
            while ids.len() > 5 {
                let id = ids.swap_remove(below(ids.len()));
                assert_eq!(t.remove(&id), model.remove(&id));
                dead.push(id);
            }
            t.check_slots();
            let survivors = ids.clone();
            for _ in 0..3_000 {
                let id = ids.swap_remove(below(ids.len()));
                assert_eq!(t.remove(&id), model.remove(&id));
                dead.push(id);
                ids.push(add(&mut t, &mut model));
                t.check_slots();
            }
            for id in survivors {
                if let Some(pos) = ids.iter().position(|&i| i == id) {
                    ids.swap_remove(pos);
                    assert_eq!(t.remove(&id), model.remove(&id));
                    dead.push(id);
                    ids.push(add(&mut t, &mut model));
                }
            }
            t.check_slots();
            let top = model.keys().map(|id| id.0 as u32 as usize).max().expect("live ids");
            assert!(
                t.capacity() <= (top / CHUNK + 2) * CHUNK,
                "round {round}: {} slots kept for a highest live slot of {top}",
                t.capacity()
            );
            for (id, v) in &model {
                assert_eq!(t.get(id), Some(v), "round {round}: a live id lost its value");
            }
            for id in &dead {
                assert!(t.get(id).is_none(), "round {round}: a removed id resolves");
            }
        }
        assert!(t.gen_floor > 1, "no trim raised the generation floor");
    }
}
