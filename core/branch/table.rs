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

use super::BranchId;

const CHUNK: usize = 1024;

struct Entry<T> {
    /// The generation the next (or current) occupant's id carries. Starts at 1, so no id is 0
    /// (`BranchId::TRUNK`).
    generation: u32,
    val: Option<T>,
}

pub(crate) struct BranchTable<T> {
    chunks: Vec<Box<[Entry<T>]>>,
    /// Slots below this have been handed out at least once.
    high_water: u32,
    free: Vec<u32>,
    len: usize,
    /// Slots taken out of use because their generation reached `u32::MAX`.
    retired: u64,
}

impl<T> BranchTable<T> {
    pub(crate) fn new() -> Self {
        Self {
            chunks: Vec::new(),
            high_water: 0,
            free: Vec::new(),
            len: 0,
            retired: 0,
        }
    }

    fn entry(&self, slot: u32) -> Option<&Entry<T>> {
        self.chunks
            .get(slot as usize / CHUNK)
            .map(|c| &c[slot as usize % CHUNK])
    }

    fn entry_mut(&mut self, slot: u32) -> Option<&mut Entry<T>> {
        self.chunks
            .get_mut(slot as usize / CHUNK)
            .map(|c| &mut c[slot as usize % CHUNK])
    }

    fn split(id: BranchId) -> (u32, u32) {
        (id.0 as u32, (id.0 >> 32) as u32)
    }

    /// The id the next `insert` must use.
    pub(crate) fn vacant_id(&mut self) -> BranchId {
        let slot = match self.free.last() {
            Some(&slot) => slot,
            None => {
                let slot = self.high_water;
                if slot as usize / CHUNK == self.chunks.len() {
                    let chunk: Vec<Entry<T>> = (0..CHUNK)
                        .map(|_| Entry {
                            generation: 1,
                            val: None,
                        })
                        .collect();
                    self.chunks.push(chunk.into_boxed_slice());
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
        if self.free.last() == Some(&slot) {
            self.free.pop();
        } else {
            self.high_water += 1;
        }
        let e = self.entry_mut(slot).expect("vacant slot exists");
        crate::turso_assert!(e.val.is_none(), "vacant slot is occupied");
        e.val = Some(val);
        self.len += 1;
    }

    pub(crate) fn get(&self, id: &BranchId) -> Option<&T> {
        let (slot, generation) = Self::split(*id);
        self.entry(slot)
            .filter(|e| e.generation == generation)
            .and_then(|e| e.val.as_ref())
    }

    pub(crate) fn get_mut(&mut self, id: &BranchId) -> Option<&mut T> {
        let (slot, generation) = Self::split(*id);
        self.entry_mut(slot)
            .filter(|e| e.generation == generation)
            .and_then(|e| e.val.as_mut())
    }

    pub(crate) fn contains_key(&self, id: &BranchId) -> bool {
        self.get(id).is_some()
    }

    pub(crate) fn remove(&mut self, id: &BranchId) -> Option<T> {
        let (slot, generation) = Self::split(*id);
        let e = self.entry_mut(slot).filter(|e| e.generation == generation)?;
        let val = e.val.take()?;
        match e.generation.checked_add(1) {
            Some(next) if next != u32::MAX => {
                e.generation = next;
                self.free.push(slot);
            }
            _ => {
                e.generation = u32::MAX;
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

    pub(crate) fn iter(&self) -> impl Iterator<Item = (BranchId, &T)> {
        self.chunks.iter().enumerate().flat_map(|(c, chunk)| {
            chunk.iter().enumerate().filter_map(move |(i, e)| {
                e.val.as_ref().map(|v| {
                    let slot = (c * CHUNK + i) as u64;
                    (BranchId(u64::from(e.generation) << 32 | slot), v)
                })
            })
        })
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &T> {
        self.iter().map(|(_, v)| v)
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
