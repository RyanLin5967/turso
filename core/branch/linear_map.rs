//! A hash map that grows and shrinks one bucket at a time, so that no single insert or remove
//! relocates more than a bucket's worth of entries.
//!
//! A std `HashMap` doubles when it fills, moving every entry in the call that finds it full: under
//! the branch store's one mutex that call stalls every branch for Θ(n) (turso_sota R1: 39 ms at
//! n = 917,504). It also rebuilds in place, or doubles again, when tombstones use up its growth
//! allowance, at a CONSTANT entry count; and it never shrinks.
//!
//! This is linear hashing (Litwin, VLDB 1980): the table has `m = 2^level + split` buckets; a key
//! hashing to `h` lives in bucket `h mod 2^(level+1)` if `h mod 2^level < split`, else in
//! `h mod 2^level`. An insert that makes the table fuller than one entry per bucket splits bucket
//! `split` alone — its entries go to `split` or `2^level + split` by one more hash bit — and a
//! remove that leaves it emptier than one entry per four buckets merges the last bucket back into
//! its buddy (up to four at a time, so `m` tracks the entry count down as fast as it drops).
//!
//! Buckets are chains of boxed entries, each carrying its hash, so a split or merge relinks nodes
//! and never re-hashes or reallocates an entry. They live in segments of doubling size (1, 1, 2,
//! 4, …; the shape of Brodnik et al.'s resizable arrays, WADS 1999): allocating one is a zeroed
//! allocation, which the allocator backs lazily, and an empty chain is a null pointer, so a new
//! segment is ready without touching its buckets. The segment list itself holds at most 65
//! pointers. So nothing ever relocates more than one bucket, plus at most 64 segment pointers.
//!
//! The hash is std's `RandomState` (keyed SipHash), so a caller who chooses the keys — page numbers
//! come from the database — cannot aim collisions at one chain.

use std::alloc::{alloc_zeroed, dealloc, handle_alloc_error, Layout};
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};

struct Node<K, V> {
    hash: u64,
    key: K,
    value: V,
    next: Chain<K, V>,
}

type Chain<K, V> = Option<Box<Node<K, V>>>;

pub(crate) struct LinearMap<K, V> {
    /// Segment 0 holds bucket 0; segment `k >= 1` holds buckets `[2^(k-1), 2^k)`.
    segments: Vec<Box<[Chain<K, V>]>>,
    level: u32,
    split: usize,
    len: usize,
    hasher: RandomState,
}

impl<K, V> Default for LinearMap<K, V> {
    fn default() -> Self {
        Self {
            segments: Vec::new(),
            level: 0,
            split: 0,
            len: 0,
            hasher: RandomState::new(),
        }
    }
}

/// A segment of `size` empty buckets, from a zeroed allocation: the allocator backs a large one
/// lazily, so nothing touches its buckets until they are used.
fn zeroed_segment<K, V>(size: usize) -> Box<[Chain<K, V>]> {
    let layout = Layout::array::<Chain<K, V>>(size).expect("a segment size that fits");
    // SAFETY: `Chain` is `Option<Box<_>>`, whose `None` is guaranteed to be the null pointer, so
    // `size` zeroed elements are `size` valid `None`s. The pointer comes from the global
    // allocator with the layout `Box<[Chain]>` frees with, and `size >= 1`.
    unsafe {
        let ptr = alloc_zeroed(layout) as *mut Chain<K, V>;
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, size))
    }
}

/// Free a segment whose buckets are all empty without visiting them.
fn free_empty_segment<K, V>(seg: Box<[Chain<K, V>]>) {
    let layout = Layout::array::<Chain<K, V>>(seg.len()).expect("it was allocated");
    // SAFETY: every element is `None` (the caller moved every chain out), so skipping their drop
    // leaks nothing; the layout is the one the segment was allocated with.
    unsafe { dealloc(Box::into_raw(seg) as *mut u8, layout) }
}

/// Segment and offset of bucket `b`.
fn locate(b: usize) -> (usize, usize) {
    if b == 0 {
        (0, 0)
    } else {
        let k = (usize::BITS - b.leading_zeros()) as usize;
        (k, b - (1 << (k - 1)))
    }
}

impl<K: Hash + Eq, V> LinearMap<K, V> {
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Buckets in use.
    fn buckets(&self) -> usize {
        if self.segments.is_empty() {
            0
        } else {
            (1usize << self.level) + self.split
        }
    }

    fn address(&self, hash: u64) -> usize {
        let low = (hash as usize) & ((1usize << self.level) - 1);
        if low < self.split {
            (hash as usize) & ((1usize << (self.level + 1)) - 1)
        } else {
            low
        }
    }

    fn bucket(&self, b: usize) -> &Chain<K, V> {
        let (s, o) = locate(b);
        &self.segments[s][o]
    }

    fn bucket_mut(&mut self, b: usize) -> &mut Chain<K, V> {
        let (s, o) = locate(b);
        &mut self.segments[s][o]
    }

    fn hash(&self, k: &K) -> u64 {
        self.hasher.hash_one(k)
    }

    pub(crate) fn get(&self, k: &K) -> Option<&V> {
        if self.len == 0 {
            return None;
        }
        let hash = self.hash(k);
        let mut node = self.bucket(self.address(hash)).as_deref();
        while let Some(n) = node {
            if n.hash == hash && n.key == *k {
                return Some(&n.value);
            }
            node = n.next.as_deref();
        }
        None
    }

    pub(crate) fn get_mut(&mut self, k: &K) -> Option<&mut V> {
        if self.len == 0 {
            return None;
        }
        let hash = self.hash(k);
        let b = self.address(hash);
        let mut node = self.bucket_mut(b).as_deref_mut();
        while let Some(n) = node {
            if n.hash == hash && n.key == *k {
                return Some(&mut n.value);
            }
            node = n.next.as_deref_mut();
        }
        None
    }

    pub(crate) fn contains_key(&self, k: &K) -> bool {
        self.get(k).is_some()
    }

    /// Insert or replace; `moved` gains the entries a split relinked (observation only).
    pub(crate) fn insert(&mut self, k: K, v: V, moved: &mut u64) -> Option<V> {
        if self.segments.is_empty() {
            self.segments.push(zeroed_segment(1));
        }
        let hash = self.hash(&k);
        let b = self.address(hash);
        let mut node = self.bucket_mut(b).as_deref_mut();
        while let Some(n) = node {
            if n.hash == hash && n.key == k {
                return Some(std::mem::replace(&mut n.value, v));
            }
            node = n.next.as_deref_mut();
        }
        let head = self.bucket_mut(b);
        let next = head.take();
        *head = Some(Box::new(Node {
            hash,
            key: k,
            value: v,
            next,
        }));
        self.len += 1;
        if self.len > self.buckets() {
            *moved += self.grow();
        }
        None
    }

    /// Remove `k`; `moved` gains the entries merges relinked (observation only).
    pub(crate) fn remove(&mut self, k: &K, moved: &mut u64) -> Option<V> {
        if self.len == 0 {
            return None;
        }
        let hash = self.hash(k);
        let b = self.address(hash);
        let mut link = self.bucket_mut(b);
        let mut n = loop {
            let hit = match link.as_ref() {
                None => return None,
                Some(n) => n.hash == hash && n.key == *k,
            };
            if hit {
                let mut n = link.take().expect("matched Some");
                *link = n.next.take();
                break n;
            }
            link = &mut link.as_mut().expect("matched Some").next;
        };
        n.next = None;
        self.len -= 1;
        for _ in 0..4 {
            if self.buckets() > 1 && self.len * 4 < self.buckets() {
                *moved += self.shrink();
            } else {
                break;
            }
        }
        Some(n.value)
    }

    /// Add bucket `2^level + split` and move into it the entries of bucket `split` whose next hash
    /// bit is set. Returns the entries relinked.
    fn grow(&mut self) -> u64 {
        let new = self.buckets();
        let (s, _) = locate(new);
        if s == self.segments.len() {
            let size = if s == 0 { 1 } else { 1usize << (s - 1) };
            self.segments.push(zeroed_segment(size));
        }
        let bit = 1u64 << self.level;
        let mut chain = self.bucket_mut(self.split).take();
        let (mut stay, mut go): (Chain<K, V>, Chain<K, V>) = (None, None);
        let mut relinked = 0;
        while let Some(mut n) = chain {
            chain = n.next.take();
            relinked += 1;
            if n.hash & bit == 0 {
                n.next = stay.take();
                stay = Some(n);
            } else {
                n.next = go.take();
                go = Some(n);
            }
        }
        *self.bucket_mut(self.split) = stay;
        *self.bucket_mut(new) = go;
        self.split += 1;
        if self.split == 1usize << self.level {
            self.level += 1;
            self.split = 0;
        }
        relinked
    }

    /// Remove the last bucket, moving its entries into the bucket it was split from. Returns the
    /// entries relinked.
    fn shrink(&mut self) -> u64 {
        if self.split == 0 {
            self.level -= 1;
            self.split = 1usize << self.level;
        }
        self.split -= 1;
        let last = (1usize << self.level) + self.split;
        let mut chain = self.bucket_mut(last).take();
        let mut relinked = 0;
        while let Some(mut n) = chain {
            chain = n.next.take();
            relinked += 1;
            let head = self.bucket_mut(self.split);
            n.next = head.take();
            *head = Some(n);
        }
        let (s, o) = locate(last);
        if o == 0 && s > 0 {
            // Bucket `last` opened segment `s`; with it gone, the segment is empty.
            free_empty_segment(self.segments.pop().expect("segment s exists"));
        }
        relinked
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        let buckets = self.buckets();
        (0..buckets).flat_map(move |b| {
            let mut node = self.bucket(b).as_deref();
            std::iter::from_fn(move || {
                let n = node?;
                node = n.next.as_deref();
                Some((&n.key, &n.value))
            })
        })
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, v)| v)
    }

    /// Every entry, leaving the map empty.
    pub(crate) fn drain(&mut self) -> Vec<(K, V)> {
        let mut out = Vec::with_capacity(self.len);
        for seg in &mut self.segments {
            for head in seg.iter_mut() {
                let mut chain = head.take();
                while let Some(mut n) = chain {
                    chain = n.next.take();
                    out.push((n.key, n.value));
                }
            }
        }
        self.segments.clear();
        self.level = 0;
        self.split = 0;
        self.len = 0;
        out
    }

    /// Every value, leaving the map empty.
    pub(crate) fn drain_values(&mut self) -> Vec<V> {
        let mut out = Vec::with_capacity(self.len);
        for seg in &mut self.segments {
            for head in seg.iter_mut() {
                let mut chain = head.take();
                while let Some(mut n) = chain {
                    chain = n.next.take();
                    out.push(n.value);
                }
            }
        }
        self.segments.clear();
        self.level = 0;
        self.split = 0;
        self.len = 0;
        out
    }
}

impl<K, V> Drop for LinearMap<K, V> {
    fn drop(&mut self) {
        // Unlink chains iteratively: a recursive `Box` drop down a chain is bounded by the chain's
        // length, which the hash keeps short, but nothing here needs to rely on that.
        for seg in &mut self.segments {
            for head in seg.iter_mut() {
                let mut chain = head.take();
                while let Some(mut n) = chain {
                    chain = n.next.take();
                }
            }
        }
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

    /// Random inserts, replaces and removes against a std `HashMap`, through growth to 20,000
    /// entries and shrinkage back to zero, with the bucket count held in `[len, 4·len]` and no
    /// single call relinking more than a handful of chains.
    #[test]
    fn matches_a_hashmap_through_growth_and_shrinkage() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut map: LinearMap<u32, u64> = LinearMap::default();
        let mut model: HashMap<u32, u64> = HashMap::new();
        let mut max_moved = 0;
        for step in 0..200_000u64 {
            let growing = step < 100_000;
            let k = rng.below(40_000) as u32;
            let mut moved = 0;
            if rng.below(3) != 0 && growing || rng.below(5) == 0 && !growing {
                assert_eq!(map.insert(k, step, &mut moved), model.insert(k, step));
            } else {
                assert_eq!(map.remove(&k, &mut moved), model.remove(&k));
            }
            max_moved = max_moved.max(moved);
            assert_eq!(map.len(), model.len());
            if map.len() > 0 {
                assert!(map.buckets() >= map.len(), "step {step}: fewer buckets than entries");
                assert!(map.buckets() <= 4 * map.len() + 4, "step {step}: {} buckets for {}", map.buckets(), map.len());
            }
            if step % 5_000 == 0 {
                for (k, v) in &model {
                    assert_eq!(map.get(k), Some(v));
                }
                let mut seen: Vec<(u32, u64)> = map.iter().map(|(&k, &v)| (k, v)).collect();
                seen.sort();
                let mut want: Vec<(u32, u64)> = model.iter().map(|(&k, &v)| (k, v)).collect();
                want.sort();
                assert_eq!(seen, want);
            }
        }
        for k in 0..40_000u32 {
            let mut moved = 0;
            assert_eq!(map.remove(&k, &mut moved), model.remove(&k));
            max_moved = max_moved.max(moved);
        }
        assert!(map.is_empty());
        assert!(max_moved <= 64, "one call relinked {max_moved} entries");
    }
}
