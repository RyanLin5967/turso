//! LEAP L4 slice 4's name mirror, compact (lead, DECISIONS 2026-10-15: the 100 B per live branch
//! budget is kept, so the mirror shrinks before the child index is added).
//!
//! An exact map from every name in the catalog's `branch_name` to its branch, as
//! `NameIndex::catalog` keeps it. The std table of `Arc<str>` it replaces kept 64-86 B per name
//! (an `Arc` allocation per name, a 24 B entry): budget test
//! `leap_l4_the_name_mirror_keeps_at_most_48_bytes_per_name`. Here the names are interned in ONE
//! byte arena (each a LEB128 length, then its bytes) and the table is open addressing with linear
//! probing over 12 B entries (the name's arena offset, the branch id) and a 1 B tag per bucket (0
//! empty, else `0x80` and the hash's top 7 bits), at a load of at most 3/4. A removal shifts the
//! probe run back, so the table holds no tombstones. A removed name's bytes go on a free list of
//! their encoded length, which the next name of that length reuses; when the bytes no name uses
//! pass both the live bytes and `COMPACT_FLOOR`, the arena is rewritten (amortized O(1) per
//! removal: as many removals as live bytes come between two rewrites).

use super::BranchId;
use std::collections::HashMap;
use std::hash::Hasher;

/// Unused arena bytes below which the arena is never rewritten.
const COMPACT_FLOOR: usize = 1 << 20;

/// The longest probe a lookup may make (the mirror adversary's MED-1): a table that would need a
/// longer one is refused, and the catalog answers instead. Linear probing at load 3/4 over 2^21
/// buckets expects a longest run of about 400 (ln n / (a - 1 - ln a)).
pub(crate) const MAX_PROBE: usize = 4096;

/// The smallest table.
const MIN_BUCKETS: usize = 16;

#[derive(Default)]
pub(crate) struct NameMirror {
    /// Per bucket: 0 empty, else `0x80 | top 7 bits of the name's hash`.
    tags: Vec<u8>,
    /// Per bucket: [arena offset, id low 32 bits, id high 32 bits].
    entries: Vec<[u32; 3]>,
    /// The names, each its LEB128 length then its UTF-8 bytes.
    bytes: Vec<u8>,
    /// Arena spans no name uses, by their encoded length (length prefix included).
    free: HashMap<u32, Vec<u32>>,
    /// Bytes in `free` (or otherwise unused).
    garbage: usize,
    len: usize,
    /// Test builds (the mirror adversary's MED-1/MED-2 reds): a hash in place of the real one, and
    /// the arena rewrite's floor in place of `COMPACT_FLOOR`; the rewrites done.
    #[cfg(test)]
    pub(crate) hash_override: Option<fn(&[u8]) -> u64>,
    #[cfg(test)]
    pub(crate) floor_override: Option<usize>,
    #[cfg(test)]
    pub(crate) rewrites: u64,
}

fn fx_hash(name: &[u8]) -> u64 {
    let mut h = rustc_hash::FxHasher::default();
    h.write(name);
    // FxHash's low bits are weak for short keys; fold the high half in for the bucket index.
    let v = h.finish();
    v ^ (v >> 29)
}

fn tag(h: u64) -> u8 {
    0x80 | (h >> 57) as u8
}

fn leb_len(mut n: usize) -> usize {
    let mut k = 1;
    while n >= 0x80 {
        n >>= 7;
        k += 1;
    }
    k
}

impl NameMirror {
    fn hash(&self, name: &[u8]) -> u64 {
        #[cfg(test)]
        if let Some(h) = self.hash_override {
            return h(name);
        }
        fx_hash(name)
    }

    /// Test builds: whether the mirror refused a table it could not bound (none yet: the red).
    #[cfg(test)]
    pub(crate) fn refused(&self) -> bool {
        false
    }

    /// Test builds: the longest probe from a name's home bucket to its entry.
    #[cfg(test)]
    pub(crate) fn max_probe(&self) -> usize {
        let mask = self.tags.len().saturating_sub(1);
        (0..self.tags.len())
            .filter(|&b| self.tags[b] != 0)
            .map(|b| b.wrapping_sub(self.hash(self.name_bytes(self.entries[b][0])) as usize) & mask)
            .max()
            .unwrap_or(0)
    }

    /// A mirror of a scan's `(name, id)` pairs (each name once, as `branch_name` holds it), sized
    /// exactly: one arena allocation and one table, nothing grown.
    pub(crate) fn from_pairs<S: AsRef<str>>(pairs: impl ExactSizeIterator<Item = (S, u64)> + Clone) -> Self {
        let n = pairs.len();
        let arena: usize = pairs.clone().map(|(s, _)| leb_len(s.as_ref().len()) + s.as_ref().len()).sum();
        let mut m = NameMirror::default();
        m.bytes.reserve_exact(arena);
        m.resize(buckets_for(n));
        for (name, id) in pairs {
            m.insert(name.as_ref(), BranchId(id));
        }
        m
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn get(&self, name: &str) -> Option<BranchId> {
        self.find(name.as_bytes()).map(|b| self.id_at(b))
    }

    /// Insert or replace `name`'s branch.
    pub(crate) fn insert(&mut self, name: &str, id: BranchId) {
        if let Some(b) = self.find(name.as_bytes()) {
            self.set_id(b, id);
            return;
        }
        if (self.len + 1) * 4 > self.tags.len() * 3 {
            self.resize(buckets_for(self.len + 1).max(self.tags.len() * 2));
        }
        let off = self.intern(name.as_bytes());
        let h = self.hash(name.as_bytes());
        let mask = self.tags.len() - 1;
        let mut b = h as usize & mask;
        while self.tags[b] != 0 {
            b = (b + 1) & mask;
        }
        self.tags[b] = tag(h);
        self.entries[b] = [off, id.0 as u32, (id.0 >> 32) as u32];
        self.len += 1;
    }

    pub(crate) fn remove(&mut self, name: &str) -> Option<BranchId> {
        let mut i = self.find(name.as_bytes())?;
        let id = self.id_at(i);
        self.release(self.entries[i][0]);
        // Backward-shift deletion: an entry later in the run whose home is not cyclically in
        // (i, j] moves into the hole.
        let mask = self.tags.len() - 1;
        let mut j = i;
        loop {
            j = (j + 1) & mask;
            if self.tags[j] == 0 {
                break;
            }
            let k = self.hash(self.name_bytes(self.entries[j][0])) as usize & mask;
            let stays = if i <= j { i < k && k <= j } else { i < k || k <= j };
            if !stays {
                self.tags[i] = self.tags[j];
                self.entries[i] = self.entries[j];
                i = j;
            }
        }
        self.tags[i] = 0;
        self.entries[i] = [0; 3];
        self.len -= 1;
        #[cfg(test)]
        let floor = self.floor_override.unwrap_or(COMPACT_FLOOR);
        #[cfg(not(test))]
        let floor = COMPACT_FLOOR;
        if self.garbage > floor && self.garbage > self.bytes.len() - self.garbage {
            self.compact();
        }
        Some(id)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, BranchId)> + '_ {
        (0..self.tags.len()).filter(|&b| self.tags[b] != 0).map(|b| {
            let name = std::str::from_utf8(self.name_bytes(self.entries[b][0])).expect("interned from a str");
            (name, self.id_at(b))
        })
    }

    /// Whether this mirror holds exactly `other`'s names and branches.
    pub(crate) fn same_as(&self, other: &HashMap<std::sync::Arc<str>, BranchId>) -> bool {
        self.len == other.len() && self.iter().all(|(n, id)| other.get(n) == Some(&id))
    }

    fn find(&self, name: &[u8]) -> Option<usize> {
        if self.tags.is_empty() {
            return None;
        }
        let h = self.hash(name);
        let (mask, t) = (self.tags.len() - 1, tag(h));
        let mut b = h as usize & mask;
        loop {
            match self.tags[b] {
                0 => return None,
                x if x == t && self.name_bytes(self.entries[b][0]) == name => return Some(b),
                _ => b = (b + 1) & mask,
            }
        }
    }

    fn id_at(&self, b: usize) -> BranchId {
        let e = self.entries[b];
        BranchId(u64::from(e[1]) | (u64::from(e[2]) << 32))
    }

    fn set_id(&mut self, b: usize, id: BranchId) {
        self.entries[b][1] = id.0 as u32;
        self.entries[b][2] = (id.0 >> 32) as u32;
    }

    /// The name stored at arena offset `off`.
    fn name_bytes(&self, off: u32) -> &[u8] {
        let (len, at) = self.read_len(off as usize);
        &self.bytes[at..at + len]
    }

    fn read_len(&self, mut at: usize) -> (usize, usize) {
        let (mut len, mut shift) = (0usize, 0);
        loop {
            let byte = self.bytes[at];
            at += 1;
            len |= usize::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return (len, at);
            }
            shift += 7;
        }
    }

    /// Store `name` in the arena (a free span of its encoded length first), returning its offset.
    fn intern(&mut self, name: &[u8]) -> u32 {
        let enc = leb_len(name.len()) + name.len();
        let reused = self.free.get_mut(&(enc as u32)).and_then(Vec::pop);
        let off = match reused {
            Some(off) => {
                self.garbage -= enc;
                off as usize
            }
            None => {
                let off = self.bytes.len();
                self.bytes.resize(off + enc, 0);
                off
            }
        };
        let off_u32 = u32::try_from(off).expect("the name mirror's arena passed 4 GiB");
        let mut at = off;
        let mut n = name.len();
        loop {
            let byte = (n & 0x7f) as u8;
            n >>= 7;
            if n == 0 {
                self.bytes[at] = byte;
                at += 1;
                break;
            }
            self.bytes[at] = byte | 0x80;
            at += 1;
        }
        self.bytes[at..at + name.len()].copy_from_slice(name);
        off_u32
    }

    fn release(&mut self, off: u32) {
        let (len, at) = self.read_len(off as usize);
        let enc = at - off as usize + len;
        self.free.entry(enc as u32).or_default().push(off);
        self.garbage += enc;
    }

    /// Rewrite the arena with only the live names (`remove`'s amortized rule).
    fn compact(&mut self) {
        let mut bytes = Vec::with_capacity(self.bytes.len() - self.garbage);
        for b in 0..self.tags.len() {
            if self.tags[b] == 0 {
                continue;
            }
            let off = self.entries[b][0] as usize;
            let (len, at) = self.read_len(off);
            self.entries[b][0] = u32::try_from(bytes.len()).expect("the arena shrank");
            bytes.extend_from_slice(&self.bytes[off..at + len]);
        }
        self.bytes = bytes;
        self.free = HashMap::new();
        self.garbage = 0;
        #[cfg(test)]
        {
            self.rewrites += 1;
        }
    }

    fn resize(&mut self, buckets: usize) {
        let old_tags = std::mem::replace(&mut self.tags, vec![0; buckets]);
        let old_entries = std::mem::replace(&mut self.entries, vec![[0; 3]; buckets]);
        let mask = buckets - 1;
        for (t, e) in old_tags.into_iter().zip(old_entries) {
            if t == 0 {
                continue;
            }
            let mut b = self.hash(self.name_bytes(e[0])) as usize & mask;
            while self.tags[b] != 0 {
                b = (b + 1) & mask;
            }
            self.tags[b] = t;
            self.entries[b] = e;
        }
    }
}

/// The table size for `n` names: a power of two at a load of at most 3/4.
fn buckets_for(n: usize) -> usize {
    (n * 4).div_ceil(3).max(MIN_BUCKETS).next_power_of_two()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// xorshift64*.
    fn next(s: &mut u64) -> u64 {
        *s ^= *s >> 12;
        *s ^= *s << 25;
        *s ^= *s >> 27;
        s.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// The mirror equals a std map under seeded random inserts, replacements and removals of names
    /// of mixed length (some past 127 bytes: a two-byte length), across growth, backward shifts and
    /// arena rewrites; checked in full every 64 operations and at the end.
    #[test]
    fn the_compact_mirror_equals_a_std_map_under_random_operations() {
        for seed in [1u64, 7, 42, 0xdead_beef] {
            let mut s = seed;
            let mut m = NameMirror::default();
            let mut model: HashMap<Arc<str>, BranchId> = HashMap::new();
            for step in 0..60_000u64 {
                let r = next(&mut s);
                let pool = 4_000;
                let k = r % pool;
                let long = (r >> 20) % 50 == 0;
                let name = if long { format!("{k}-{}", "x".repeat(130)) } else { format!("n{k}") };
                if (r >> 40) % 3 == 0 {
                    let gone = m.remove(&name);
                    assert_eq!(gone, model.remove(name.as_str()), "seed {seed} step {step}: remove {name}");
                } else {
                    let id = BranchId(next(&mut s));
                    m.insert(&name, id);
                    model.insert(Arc::from(name.as_str()), id);
                }
                if step % 64 == 0 {
                    assert_eq!(m.tags.iter().filter(|&&t| t != 0).count(), m.len, "seed {seed} step {step}: the tags count the names");
                    assert!(m.same_as(&model), "seed {seed} step {step}: the mirror differs from the model");
                    assert_eq!(m.len(), model.len());
                    for (n, id) in &model {
                        assert_eq!(m.get(n), Some(*id), "seed {seed} step {step}: {n}");
                    }
                }
            }
            assert!(m.same_as(&model));
        }
    }

    /// The arena is rewritten once removals leave more unused bytes than live ones (past the floor),
    /// and every name still resolves after it.
    #[test]
    fn the_arena_is_rewritten_when_mostly_unused() {
        let mut m = NameMirror::default();
        let names: Vec<String> = (0..120_000).map(|i| format!("churn-name-{i:07}")).collect();
        for (i, n) in names.iter().enumerate() {
            m.insert(n, BranchId(i as u64));
        }
        let full = m.bytes.len();
        for n in &names[..100_000] {
            m.remove(n);
        }
        assert!(m.bytes.len() < full / 2, "premise: the arena was rewritten ({} of {full} bytes)", m.bytes.len());
        for (i, n) in names.iter().enumerate().skip(100_000) {
            assert_eq!(m.get(n), Some(BranchId(i as u64)));
        }
        assert_eq!(m.get(&names[0]), None);
    }

    /// MED-1 (the mirror adversary): 10^4 names that all hash alike (a planted hash) are refused, or
    /// every lookup stays within `MAX_PROBE`. Red before the bound: the probes grow with the names.
    #[test]
    fn colliding_names_are_refused_or_bounded() {
        let mut m = NameMirror { hash_override: Some(|_| 0x1234_5678), ..NameMirror::default() };
        for i in 0..10_000u64 {
            m.insert(&format!("flood-{i}"), BranchId(i));
            if m.refused() {
                break;
            }
        }
        assert!(
            m.refused() || m.max_probe() <= MAX_PROBE,
            "10^4 colliding names: longest probe {}, refused {}; budget {MAX_PROBE} or a refusal",
            m.max_probe(),
            m.refused()
        );
    }

    /// The homes the wrap cases plant: names "a", "b" and "c" at buckets 15, 15 and 0 of 16.
    fn planted(name: &[u8]) -> u64 {
        match name {
            b"a" | b"b" => 15,
            b"c" => 0,
            _ => 7,
        }
    }

    /// MED-2: a probe run that crosses index 0. Removing the run's first entry (at 15) shifts the
    /// two after it back across the wrap; removing the one that wrapped (at 0) shifts the third.
    #[test]
    fn a_removal_shifts_a_run_back_across_index_0() {
        for gone in ["a", "b", "c"] {
            let mut m = NameMirror { hash_override: Some(planted), ..NameMirror::default() };
            m.resize(16);
            for (i, n) in ["a", "b", "c"].iter().enumerate() {
                m.insert(n, BranchId(i as u64 + 1));
            }
            assert_eq!(m.tags.len(), 16, "premise: one table of 16 buckets");
            assert!(m.tags[15] != 0 && m.tags[0] != 0 && m.tags[1] != 0, "premise: the run is 15, 0, 1");
            m.remove(gone);
            for (i, n) in ["a", "b", "c"].iter().enumerate() {
                let want = (*n != gone).then_some(BranchId(i as u64 + 1));
                assert_eq!(m.get(n), want, "after removing {gone}: {n}");
            }
            assert_eq!(m.max_probe() <= 1, true, "after removing {gone}: the run was not shifted back");
        }
    }

    /// MED-2: the random operations with the rewrite's floor lowered to 256 bytes, so the arena is
    /// rewritten many times and freed spans are reused after each rewrite; equal to a std map
    /// throughout, and the rewrites are counted (a run with none fails).
    #[test]
    fn the_mirror_equals_a_std_map_across_arena_rewrites() {
        for seed in [3u64, 11, 0xfeed] {
            let mut s = seed;
            let mut m = NameMirror { floor_override: Some(256), ..NameMirror::default() };
            let mut model: HashMap<Arc<str>, BranchId> = HashMap::new();
            for step in 0..40_000u64 {
                let r = next(&mut s);
                let k = r % 1_500;
                let name = if (r >> 20) % 7 == 0 { format!("{k}-{}", "y".repeat(140)) } else { format!("r{k}") };
                if (r >> 40) % 2 == 0 {
                    assert_eq!(m.remove(&name), model.remove(name.as_str()), "seed {seed} step {step}: remove {name}");
                } else {
                    let id = BranchId(next(&mut s));
                    m.insert(&name, id);
                    model.insert(Arc::from(name.as_str()), id);
                }
                if step % 97 == 0 {
                    assert!(m.same_as(&model), "seed {seed} step {step}: the mirror differs from the model");
                }
            }
            assert!(m.same_as(&model));
            assert!(m.rewrites > 0, "seed {seed}: premise: the arena was rewritten");
        }
    }

    /// Ids above 2^32 survive the split into two halves.
    #[test]
    fn a_wide_id_round_trips() {
        let mut m = NameMirror::default();
        m.insert("wide", BranchId(u64::MAX - 3));
        assert_eq!(m.get("wide"), Some(BranchId(u64::MAX - 3)));
    }
}
