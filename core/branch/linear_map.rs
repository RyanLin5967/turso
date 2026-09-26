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
//! and never re-hashes or reallocates an entry. They live in segments: doubling ones (1, 1, 2, 4,
//! … buckets) up to 8,192 buckets, so that a small map stays small, and past that FIXED segments of
//! 8,192 buckets (64 KB) behind a two-level directory (Larson, "Dynamic hash tables", CACM 1988).
//! A segment is a zeroed allocation, which the allocator backs lazily, and an empty chain is a
//! null pointer, so a new segment is ready without touching its buckets. So no insert or remove
//! relinks more than a bucket's entries or allocates or frees more than 64 KB of table. (Doubling
//! segments all the way up freed a 4 MB segment in the one remove that emptied it: measured at
//! 8 ms under the store mutex, PREREG amendment 6.)
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

/// Buckets below this live in doubling segments; from it on, in fixed segments of this many.
const FIXED: usize = 1 << 13;
/// Fixed segments per second-level directory box.
const DIR: usize = 1024;

/// Where bucket `b` lives.
enum Loc {
    /// `segments[k][offset]`.
    Small(usize, usize),
    /// `big[top][inner]`, at `offset`.
    Big(usize, usize, usize),
}

pub(crate) struct LinearMap<K, V> {
    /// Segment 0 holds bucket 0; segment `k` in `1..=13` holds buckets `[2^(k-1), 2^k)`.
    segments: Vec<Box<[Chain<K, V>]>>,
    /// Fixed segment `j` holds buckets `[8192·(j+1), 8192·(j+2))`, at `big[j / 1024][j % 1024]`.
    big: Vec<Box<[Option<Box<[Chain<K, V>]>>]>>,
    level: u32,
    split: usize,
    len: usize,
    hasher: RandomState,
    /// Observation only: bytes of bucket segments allocated and freed over the map's life.
    seg_bytes: [u64; 2],
}

impl<K, V> Default for LinearMap<K, V> {
    fn default() -> Self {
        Self {
            segments: Vec::new(),
            big: Vec::new(),
            level: 0,
            split: 0,
            len: 0,
            hasher: RandomState::new(),
            seg_bytes: [0; 2],
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

/// Where bucket `b` lives.
fn locate(b: usize) -> Loc {
    if b == 0 {
        Loc::Small(0, 0)
    } else if b < FIXED {
        let k = (usize::BITS - b.leading_zeros()) as usize;
        Loc::Small(k, b - (1 << (k - 1)))
    } else {
        let j = b / FIXED - 1;
        Loc::Big(j / DIR, j % DIR, b % FIXED)
    }
}

impl<K: Hash + Eq, V> LinearMap<K, V> {
    /// Bytes of bucket segments allocated and freed so far (observation only).
    pub(crate) fn segment_bytes(&self) -> [u64; 2] {
        self.seg_bytes
    }

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
        match locate(b) {
            Loc::Small(k, o) => &self.segments[k][o],
            Loc::Big(t, i, o) => &self.big[t][i].as_ref().expect("an allocated segment")[o],
        }
    }

    fn bucket_mut(&mut self, b: usize) -> &mut Chain<K, V> {
        match locate(b) {
            Loc::Small(k, o) => &mut self.segments[k][o],
            Loc::Big(t, i, o) => &mut self.big[t][i].as_mut().expect("an allocated segment")[o],
        }
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
            self.seg_bytes[0] += std::mem::size_of::<Chain<K, V>>() as u64;
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
        match locate(new) {
            Loc::Small(k, _) if k == self.segments.len() => {
                let size = if k == 0 { 1 } else { 1usize << (k - 1) };
                self.segments.push(zeroed_segment(size));
                self.seg_bytes[0] += (size * std::mem::size_of::<Chain<K, V>>()) as u64;
            }
            Loc::Big(t, i, 0) => {
                if t == self.big.len() {
                    // At most 512 boxes of 1,024 pointers each cover every bucket a u32 hash can
                    // address, so this vector's own growth is bounded.
                    self.big
                        .push(std::iter::repeat_with(|| None).take(DIR).collect());
                }
                self.big[t][i] = Some(zeroed_segment(FIXED));
                self.seg_bytes[0] += (FIXED * std::mem::size_of::<Chain<K, V>>()) as u64;
            }
            _ => {}
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
        // Bucket `last` opened its segment if it sits at offset 0; with it gone, the segment is empty.
        match locate(last) {
            Loc::Small(k, 0) if k > 0 => {
                let seg = self.segments.pop().expect("segment k exists");
                self.seg_bytes[1] += (seg.len() * std::mem::size_of::<Chain<K, V>>()) as u64;
                free_empty_segment(seg);
            }
            Loc::Big(t, i, 0) => {
                let seg = self.big[t][i].take().expect("an allocated segment");
                self.seg_bytes[1] += (seg.len() * std::mem::size_of::<Chain<K, V>>()) as u64;
                free_empty_segment(seg);
                if i == 0 {
                    self.big.pop();
                }
            }
            _ => {}
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
}

impl<K, V> Drop for LinearMap<K, V> {
    fn drop(&mut self) {
        // Unlink chains iteratively: a recursive `Box` drop down a chain is bounded by the chain's
        // length, which the hash keeps short, but nothing here needs to rely on that.
        let big = self.big.iter_mut().flat_map(|t| t.iter_mut().flatten());
        for seg in self.segments.iter_mut().chain(big) {
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
        let mut max_seg_bytes = 0;
        for step in 0..200_000u64 {
            let growing = step < 100_000;
            let k = rng.below(40_000) as u32;
            let seg_before = map.segment_bytes();
            let mut moved = 0;
            if rng.below(3) != 0 && growing || rng.below(5) == 0 && !growing {
                assert_eq!(map.insert(k, step, &mut moved), model.insert(k, step));
            } else {
                assert_eq!(map.remove(&k, &mut moved), model.remove(&k));
            }
            max_moved = max_moved.max(moved);
            let seg_after = map.segment_bytes();
            max_seg_bytes = max_seg_bytes
                .max(seg_after[0] - seg_before[0])
                .max(seg_after[1] - seg_before[1]);
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
        let segment = (FIXED * std::mem::size_of::<Chain<u32, u64>>()) as u64;
        assert!(
            max_seg_bytes <= segment,
            "one call allocated or freed {max_seg_bytes} bytes of table"
        );
        assert!(
            map.segment_bytes()[0] > 2 * segment,
            "the map never grew past its fixed-segment boundary, so the test says nothing about it"
        );
    }
}
