//! LEAP L4 slice 4's name mirror, compact (lead, DECISIONS 2026-10-15: the 100 B per live branch
//! budget is kept, so the mirror shrinks before the child index is added; the mirror adversary's
//! class fixes, DECISIONS f398de1ff0).
//!
//! An exact map from every name in the catalog's `branch_name` to its branch, as
//! `NameIndex::catalog` keeps it. The std table of `Arc<str>` it replaced kept 64-86 B per name.
//!
//! - **Names** live in 4 KiB chunks, each name a LEB128 length then its bytes, never split across
//!   chunks; a name's place is `chunk << 12 | position`. A removal only counts the span as dead.
//!   A chunk other than the one being filled whose live bytes fall below half its used bytes is
//!   evacuated at once (its live names copied to the filling chunk, their entries re-pointed, the
//!   chunk freed): at most 4 KiB of work, so no removal does O(N) work, and the dead bytes stay at
//!   most the live bytes plus the filling chunk.
//! - **The table** is open addressing with linear probing over 12 B entries (the name's place, the
//!   branch id) and a 1 B tag per bucket (0 empty, else `0x80` and the hash's top 7 bits), at a
//!   load of at most 3/4, with backward-shift removal (no tombstones).
//! - **The hash** is SipHash keyed per process (`RandomState`, as the name filter's), so a caller
//!   cannot choose names that collide; and a table that would need a probe past `MAX_PROBE` is
//!   REFUSED (`Refused`): the store then drops the mirror and the catalog answers.
//! - **Growth off the store mutex**: `with_room` copies the mirror into a table sized for more names
//!   (live names only, so the copy also compacts), for the checkpoint thread to build before its
//!   install takes the mutex; `insert` still grows in place when no room was made (`inline_grows`).

use super::BranchId;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::BuildHasher;

/// Bytes per name chunk; a place's low 12 bits.
const CHUNK: usize = 1 << 12;

/// The smallest table.
const MIN_BUCKETS: usize = 16;

/// The longest probe a lookup may make (the mirror adversary's MED-1): a table that would need a
/// longer one is refused, and the catalog answers instead. Linear probing at load 3/4 over 2^21
/// buckets expects a longest run of about 400 (ln n / (a - 1 - ln a)).
pub(crate) const MAX_PROBE: usize = 4096;

/// A table the mirror would have to probe past `MAX_PROBE` to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Refused;

pub(crate) struct NameMirror {
    hasher: RandomState,
    /// Per bucket: 0 empty, else `0x80 | top 7 bits of the name's hash`.
    tags: Vec<u8>,
    /// Per bucket: [the name's place, id low 32 bits, id high 32 bits].
    entries: Vec<[u32; 3]>,
    len: usize,
    /// The name chunks (`None`: evacuated, its index on `spare`), their used and live bytes.
    chunks: Vec<Option<Box<[u8]>>>,
    used: Vec<u16>,
    live: Vec<u16>,
    /// The chunk being filled, if any.
    filling: Option<usize>,
    spare: Vec<usize>,
    refused: bool,
    /// Grows done in place by `insert` (no room made beforehand): each O(N) where it runs.
    pub(crate) inline_grows: u64,
    /// Chunks evacuated.
    pub(crate) evacuations: u64,
    /// Test builds (the mirror adversary's reds): a hash in place of the real one.
    #[cfg(test)]
    pub(crate) hash_override: Option<fn(&[u8]) -> u64>,
}

impl Default for NameMirror {
    fn default() -> Self {
        NameMirror::with_hasher(RandomState::new())
    }
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

/// The table size for `n` names: a power of two at a load of at most 3/4.
fn buckets_for(n: usize) -> usize {
    (n * 4).div_ceil(3).max(MIN_BUCKETS).next_power_of_two()
}

impl NameMirror {
    pub(crate) fn with_hasher(hasher: RandomState) -> Self {
        NameMirror {
            hasher,
            tags: Vec::new(),
            entries: Vec::new(),
            len: 0,
            chunks: Vec::new(),
            used: Vec::new(),
            live: Vec::new(),
            filling: None,
            spare: Vec::new(),
            refused: false,
            inline_grows: 0,
            evacuations: 0,
            #[cfg(test)]
            hash_override: None,
        }
    }

    /// A mirror of a scan's `(name, id)` pairs (each name once, as `branch_name` holds it), its table
    /// sized for them at once.
    pub(crate) fn from_pairs<S: AsRef<str>>(
        hasher: RandomState,
        pairs: impl ExactSizeIterator<Item = (S, u64)>,
    ) -> Result<Self, Refused> {
        let mut m = NameMirror::with_hasher(hasher);
        let n = pairs.len();
        m.reserve_chunks(n);
        m.resize(buckets_for(n))?;
        for (name, id) in pairs {
            m.insert(name.as_ref(), BranchId(id))?;
        }
        Ok(m)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Whether `extra` more names would grow the table in place.
    pub(crate) fn needs_room(&self, extra: usize) -> bool {
        (self.len + extra) * 4 > self.tags.len() * 3
    }

    /// A copy with room for `extra` more names, its names packed into fresh chunks (the copy
    /// compacts): built off the store mutex, then swapped in.
    pub(crate) fn with_room(&self, extra: usize) -> Result<NameMirror, Refused> {
        let mut m = NameMirror::with_hasher(self.hasher.clone());
        #[cfg(test)]
        {
            m.hash_override = self.hash_override;
        }
        m.reserve_chunks(self.len + extra);
        m.resize(buckets_for(self.len + extra))?;
        for (name, id) in self.iter() {
            m.insert(name, id)?;
        }
        m.inline_grows = self.inline_grows;
        m.evacuations = self.evacuations;
        Ok(m)
    }

    pub(crate) fn get(&self, name: &str) -> Option<BranchId> {
        self.find(name.as_bytes()).map(|b| self.id_at(b))
    }

    /// Insert or replace `name`'s branch. `Err`: the table would need a probe past `MAX_PROBE`,
    /// and is refused (`refused`); the caller drops the mirror.
    pub(crate) fn insert(&mut self, name: &str, id: BranchId) -> Result<(), Refused> {
        if self.refused {
            return Err(Refused);
        }
        if let Some(b) = self.find(name.as_bytes()) {
            self.set_id(b, id);
            return Ok(());
        }
        if self.needs_room(1) {
            self.inline_grows += u64::from(!self.tags.is_empty());
            let buckets = buckets_for(self.len + 1).max(self.tags.len() * 2);
            self.resize(buckets)?;
        }
        let h = self.hash(name.as_bytes());
        let b = self.free_bucket(h)?;
        let place = self.intern(name.as_bytes());
        self.tags[b] = tag(h);
        self.entries[b] = [place, id.0 as u32, (id.0 >> 32) as u32];
        self.len += 1;
        Ok(())
    }

    pub(crate) fn remove(&mut self, name: &str) -> Option<BranchId> {
        let i = self.find(name.as_bytes())?;
        let id = self.id_at(i);
        let place = self.entries[i][0];
        self.unlink(i);
        self.release(place);
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

    /// Test builds: whether the mirror refused a table it could not bound.
    #[cfg(test)]
    pub(crate) fn refused(&self) -> bool {
        self.refused
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

    /// Test builds: arena bytes held (chunks allocated) and live.
    #[cfg(test)]
    pub(crate) fn arena_bytes(&self) -> (usize, usize) {
        let held = self.chunks.iter().filter(|c| c.is_some()).count() * CHUNK;
        (held, self.live.iter().map(|&l| l as usize).sum())
    }

    fn hash(&self, name: &[u8]) -> u64 {
        #[cfg(test)]
        if let Some(h) = self.hash_override {
            return h(name);
        }
        self.hasher.hash_one(name)
    }

    fn find(&self, name: &[u8]) -> Option<usize> {
        if self.tags.is_empty() {
            return None;
        }
        let h = self.hash(name);
        let (mask, t) = (self.tags.len() - 1, tag(h));
        let mut b = h as usize & mask;
        // Every run is at most MAX_PROBE + 1 long (inserts refuse longer), and at least a quarter of
        // the buckets are empty, so this ends.
        loop {
            match self.tags[b] {
                0 => return None,
                x if x == t && self.name_bytes(self.entries[b][0]) == name => return Some(b),
                _ => b = (b + 1) & mask,
            }
        }
    }

    /// The empty bucket a name hashing to `h` goes in, within `MAX_PROBE` of its home, or refused.
    fn free_bucket(&mut self, h: u64) -> Result<usize, Refused> {
        let mask = self.tags.len() - 1;
        let mut b = h as usize & mask;
        for _ in 0..=MAX_PROBE {
            if self.tags[b] == 0 {
                return Ok(b);
            }
            b = (b + 1) & mask;
        }
        self.refused = true;
        Err(Refused)
    }

    /// Empty bucket `i`, shifting the rest of its run back (an entry later in the run whose home
    /// is not cyclically in (i, j] moves into the hole).
    fn unlink(&mut self, mut i: usize) {
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
    }

    fn id_at(&self, b: usize) -> BranchId {
        let e = self.entries[b];
        BranchId(u64::from(e[1]) | (u64::from(e[2]) << 32))
    }

    fn set_id(&mut self, b: usize, id: BranchId) {
        self.entries[b][1] = id.0 as u32;
        self.entries[b][2] = (id.0 >> 32) as u32;
    }

    /// The span at `place`: (its encoded length, the name's bytes).
    fn span(&self, place: u32) -> (usize, &[u8]) {
        let (c, mut at) = ((place >> 12) as usize, (place & 0xfff) as usize);
        let chunk = self.chunks[c].as_deref().expect("a live place names a live chunk");
        let start = at;
        let (mut len, mut shift) = (0usize, 0);
        loop {
            let byte = chunk[at];
            at += 1;
            len |= usize::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        (at - start + len, &chunk[at..at + len])
    }

    fn name_bytes(&self, place: u32) -> &[u8] {
        self.span(place).1
    }

    /// Room in the chunk list for `names` more names, so filling does not move it.
    fn reserve_chunks(&mut self, names: usize) {
        let want = (names * 12).div_ceil(CHUNK) + 1;
        self.chunks.reserve(want);
        self.used.reserve(want);
        self.live.reserve(want);
    }

    /// Store `name` in the filling chunk (a fresh one when it does not fit), returning its place.
    fn intern(&mut self, name: &[u8]) -> u32 {
        let enc = leb_len(name.len()) + name.len();
        assert!(enc <= CHUNK, "a branch name of {} bytes", name.len());
        let c = match self.filling {
            Some(c) if usize::from(self.used[c]) + enc <= CHUNK => c,
            _ => {
                let chunk = vec![0u8; CHUNK].into_boxed_slice();
                let c = match self.spare.pop() {
                    Some(c) => {
                        self.chunks[c] = Some(chunk);
                        c
                    }
                    None => {
                        self.chunks.push(Some(chunk));
                        self.used.push(0);
                        self.live.push(0);
                        self.chunks.len() - 1
                    }
                };
                assert!(c < 1 << 20, "the name mirror passed 2^20 chunks (4 GiB)");
                self.filling = Some(c);
                c
            }
        };
        let at = usize::from(self.used[c]);
        let chunk = self.chunks[c].as_deref_mut().expect("the filling chunk is live");
        let (mut n, mut k) = (name.len(), at);
        loop {
            let byte = (n & 0x7f) as u8;
            n >>= 7;
            chunk[k] = if n == 0 { byte } else { byte | 0x80 };
            k += 1;
            if n == 0 {
                break;
            }
        }
        chunk[k..k + name.len()].copy_from_slice(name);
        self.used[c] += enc as u16;
        self.live[c] += enc as u16;
        ((c as u32) << 12) | at as u32
    }

    /// A removed name's span is dead: its chunk is evacuated once under half live.
    fn release(&mut self, place: u32) {
        let c = (place >> 12) as usize;
        let enc = self.span(place).0;
        self.live[c] -= enc as u16;
        if Some(c) != self.filling && 2 * usize::from(self.live[c]) < usize::from(self.used[c]) {
            self.evacuate(c);
        }
    }

    /// Copy chunk `c`'s live names to the filling chunk, re-point their entries, free `c`. A span is
    /// live when its name's entry points at it.
    fn evacuate(&mut self, c: usize) {
        let mut at = 0usize;
        let mut buf = [0u8; CHUNK];
        while at < usize::from(self.used[c]) {
            let place = ((c as u32) << 12) | at as u32;
            let (enc, name) = self.span(place);
            let len = name.len();
            buf[..len].copy_from_slice(name);
            if let Some(b) = self.find(&buf[..len]).filter(|&b| self.entries[b][0] == place) {
                self.entries[b][0] = self.intern(&buf[..len]);
            }
            at += enc;
        }
        self.chunks[c] = None;
        self.used[c] = 0;
        self.live[c] = 0;
        self.spare.push(c);
        self.evacuations += 1;
    }

    fn resize(&mut self, buckets: usize) -> Result<(), Refused> {
        let old_tags = std::mem::replace(&mut self.tags, vec![0; buckets]);
        let old_entries = std::mem::replace(&mut self.entries, vec![[0; 3]; buckets]);
        for (t, e) in old_tags.into_iter().zip(old_entries) {
            if t == 0 {
                continue;
            }
            let h = self.hash(self.name_bytes(e[0]));
            let b = self.free_bucket(h)?;
            self.tags[b] = t;
            self.entries[b] = e;
        }
        Ok(())
    }
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
    /// chunk evacuations; checked in full every 64 operations and at the end, evacuations counted.
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
                    m.insert(&name, id).unwrap();
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
            assert!(m.evacuations > 0, "seed {seed}: premise: a chunk was evacuated");
        }
    }

    /// MED-2: the arena shrinks under removals in insertion order (chunks evacuated), its held
    /// bytes at most twice the live ones plus two chunks, and every name still resolves.
    #[test]
    fn the_arena_is_rewritten_when_mostly_unused() {
        let mut m = NameMirror::default();
        let names: Vec<String> = (0..120_000).map(|i| format!("churn-name-{i:07}")).collect();
        for (i, n) in names.iter().enumerate() {
            m.insert(n, BranchId(i as u64)).unwrap();
        }
        let (full, _) = m.arena_bytes();
        for n in &names[..100_000] {
            m.remove(n);
        }
        let (held, live) = m.arena_bytes();
        assert!(held < full / 2, "premise: the arena shrank ({held} of {full} bytes)");
        assert!(held <= 2 * live + 2 * CHUNK, "held {held} B for {live} live B");
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
            if m.insert(&format!("flood-{i}"), BranchId(i)).is_err() {
                break;
            }
        }
        assert!(
            m.refused() || m.max_probe() <= MAX_PROBE,
            "10^4 colliding names: longest probe {}, refused {}; budget {MAX_PROBE} or a refusal",
            m.max_probe(),
            m.refused()
        );
        assert!(m.refused(), "premise: 10^4 names in one run passed the bound");
        assert!(m.max_probe() <= MAX_PROBE, "a refused mirror kept a run past the bound");
    }

    /// The real hash is keyed per mirror's hasher: two mirrors' hashers differ (a caller cannot
    /// precompute one), and a mirror's copy keeps its own.
    #[test]
    fn the_hash_is_keyed() {
        let (a, b) = (NameMirror::default(), NameMirror::default());
        assert_ne!(a.hash(b"some-name"), b.hash(b"some-name"), "two RandomStates hashed alike");
        let c = a.with_room(10).unwrap();
        assert_eq!(a.hash(b"some-name"), c.hash(b"some-name"));
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
            m.resize(16).unwrap();
            for (i, n) in ["a", "b", "c"].iter().enumerate() {
                m.insert(n, BranchId(i as u64 + 1)).unwrap();
            }
            assert_eq!(m.tags.len(), 16, "premise: one table of 16 buckets");
            assert!(m.tags[15] != 0 && m.tags[0] != 0 && m.tags[1] != 0, "premise: the run is 15, 0, 1");
            m.remove(gone);
            for (i, n) in ["a", "b", "c"].iter().enumerate() {
                let want = (*n != gone).then_some(BranchId(i as u64 + 1));
                assert_eq!(m.get(n), want, "after removing {gone}: {n}");
            }
            assert!(m.max_probe() <= 1, "after removing {gone}: the run was not shifted back");
        }
    }

    /// MED-2: random operations over a small name pool with long names among them, so chunks are
    /// evacuated many times and names are put into fresh chunks after each; equal to a std map
    /// throughout, and the evacuations are counted (a run with none fails). Two copies made with
    /// room (`with_room`) along the way equal the model too.
    #[test]
    fn the_mirror_equals_a_std_map_across_arena_rewrites() {
        for seed in [3u64, 11, 0xfeed] {
            let mut s = seed;
            let mut m = NameMirror::default();
            let mut model: HashMap<Arc<str>, BranchId> = HashMap::new();
            for step in 0..40_000u64 {
                let r = next(&mut s);
                let k = r % 1_500;
                let name = if (r >> 20) % 7 == 0 { format!("{k}-{}", "y".repeat(140)) } else { format!("r{k}") };
                if (r >> 40) % 2 == 0 {
                    assert_eq!(m.remove(&name), model.remove(name.as_str()), "seed {seed} step {step}: remove {name}");
                } else {
                    let id = BranchId(next(&mut s));
                    m.insert(&name, id).unwrap();
                    model.insert(Arc::from(name.as_str()), id);
                }
                if step % 97 == 0 {
                    assert!(m.same_as(&model), "seed {seed} step {step}: the mirror differs from the model");
                }
                // Two copies with room, late, so evacuations happen between them (each copy packs).
                if step == 20_000 || step == 30_000 {
                    m = m.with_room(1 + (r % 3_000) as usize).unwrap();
                    assert!(m.same_as(&model), "seed {seed} step {step}: a copy with room differs");
                }
            }
            assert!(m.same_as(&model));
            assert!(m.evacuations > 0, "seed {seed}: premise: chunks were evacuated");
        }
    }

    /// Ids above 2^32 survive the split into two halves.
    #[test]
    fn a_wide_id_round_trips() {
        let mut m = NameMirror::default();
        m.insert("wide", BranchId(u64::MAX - 3)).unwrap();
        assert_eq!(m.get("wide"), Some(BranchId(u64::MAX - 3)));
    }
}
