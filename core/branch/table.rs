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
//! that oscillates across a chunk boundary (jemalloc's spare chunk). The slots themselves stay:
//! 8 bytes per slot ever used, the price of never reissuing a generation. Their free list is
//! threaded through the vacant slots (as in `slotmap`), so it takes no memory of its own.

use super::BranchId;

const CHUNK: usize = 1024;

/// A link's top bit marks an occupied slot; the rest is then the value's dense index.
const OCCUPIED: u32 = 1 << 31;
/// End of the vacant-slot list. Not an index any slot can have (slots stay below it).
const NO_SLOT: u32 = OCCUPIED - 1;

struct Entry {
    /// The generation the next (or current) occupant's id carries. Starts at 1, so no id is 0
    /// (`BranchId::TRUNK`).
    generation: u32,
    /// Occupied: `OCCUPIED | dense index`. Vacant: the next vacant slot, or `NO_SLOT`.
    link: u32,
}

pub(crate) struct BranchTable<T> {
    /// Slots by index, in chunks that never move and are never freed.
    chunks: Vec<Box<[Entry]>>,
    /// Slots below this have been handed out at least once.
    high_water: u32,
    /// First vacant slot below `high_water` (the list runs through `Entry::link`), or `NO_SLOT`.
    free_head: u32,
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
            free_head: NO_SLOT,
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

    /// The id the next `insert` must use.
    pub(crate) fn vacant_id(&mut self) -> BranchId {
        let slot = if self.free_head != NO_SLOT {
            self.free_head
        } else {
            let slot = self.high_water;
            crate::turso_assert!(slot < NO_SLOT, "the branch table has no slot left");
            if slot as usize / CHUNK == self.chunks.len() {
                let chunk: Vec<Entry> = (0..CHUNK)
                    .map(|_| Entry {
                        generation: 1,
                        link: NO_SLOT,
                    })
                    .collect();
                self.chunks.push(chunk.into_boxed_slice());
            }
            slot
        };
        let generation = self.entry(slot).expect("allocated above").generation;
        BranchId(u64::from(generation) << 32 | u64::from(slot))
    }

    pub(crate) fn insert(&mut self, id: BranchId, val: T) {
        crate::turso_assert!(id == self.vacant_id(), "insert with an id vacant_id did not issue");
        let (slot, _) = Self::split(id);
        if self.free_head == slot {
            self.free_head = self.entry(slot).expect("vacant slot exists").link;
        } else {
            self.high_water += 1;
        }
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
            let (_, val) = std::mem::replace(&mut self.dense[d / CHUNK][d % CHUNK], last);
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
        let free_head = self.free_head;
        let e = self.entry_mut(slot).expect("a live id's slot exists");
        match e.generation.checked_add(1) {
            _ if super::mutant::on(5) => {
                e.link = free_head;
                self.free_head = slot;
            }
            Some(next) if next != u32::MAX => {
                e.generation = next;
                e.link = free_head;
                self.free_head = slot;
            }
            _ => {
                e.generation = u32::MAX;
                e.link = NO_SLOT;
                self.retired += 1;
            }
        }
        self.len -= 1;
        Some(val)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Slots allocated (chunks × chunk size). It changes only when a chunk is added.
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
        for (peak, keep) in [(5_000usize, 7usize), (3_000, 1), (6_000, 1_100)] {
            while model.len() < peak {
                let id = t.vacant_id();
                t.insert(id, next);
                model.insert(id, next);
                next += 1;
            }
            check(&t, &model, &dead);
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
}
