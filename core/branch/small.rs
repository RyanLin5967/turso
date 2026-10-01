//! A branch's small per-branch maps, stored inside the branch state (lane r12-f9-shrink, F9-F).
//!
//! Every branch keeps `current` (page to version) and `current_by_born` (the same versions by
//! birth). Most branches hold a handful of pages, and as a `HashMap` and a `BTreeSet` each such
//! branch cost two heap allocations (~320 B measured with the table's own ~50 B per branch of the
//! trunk's child map: 371 B per branch, lane r12-f9-shrink raw/A.txt). After a peak of 10^6
//! branches and a shrink to 10^3, the process heap kept the freed ones: ~102 MB of empty regions
//! the system allocator keeps dirty and ~23 MB of regions pinned by the survivors' objects (raw
//! D1_B_r2, amendment 3), 11x the footprint of a run that never peaked.
//!
//! The published answer is to give objects that die together storage that dies with them, freed
//! wholesale (APR pools; Tofte and Talpin's region inference, 1997). For a branch's maps the region
//! is the branch state itself, which the branch table packs into chunks it frees as they empty
//! (F9): up to `N` entries live inline in the state (the small-size optimization of LLVM's
//! `SmallVector`/`SmallDenseMap` and the `smallvec` crate), so a small branch allocates nothing
//! from the heap, and its maps go when its state goes. A larger branch spills to a `HashMap` /
//! `BTreeSet` as before, and returns inline once it is back to `N / 2` entries or fewer
//! (hysteresis, so a size oscillating at `N` does not allocate and free per operation).

use std::collections::{btree_set, hash_map, BTreeSet, HashMap};
use std::hash::Hash;

/// A map holding up to `N` entries inline, then a `HashMap`.
pub(crate) enum SmallMap<K, V, const N: usize> {
    Inline { len: u8, items: [(K, V); N] },
    Heap(HashMap<K, V>),
}

impl<K: Copy + Default, V: Copy + Default, const N: usize> Default for SmallMap<K, V, N> {
    fn default() -> Self {
        Self::Inline {
            len: 0,
            items: [(K::default(), V::default()); N],
        }
    }
}

impl<K, V, const N: usize> SmallMap<K, V, N>
where
    K: Copy + Default + Eq + Hash,
    V: Copy + Default,
{
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Inline { len, .. } => *len as usize,
            Self::Heap(m) => m.len(),
        }
    }

    pub(crate) fn get(&self, k: &K) -> Option<&V> {
        match self {
            Self::Inline { len, items } => items[..*len as usize]
                .iter()
                .find(|(key, _)| key == k)
                .map(|(_, v)| v),
            Self::Heap(m) => m.get(k),
        }
    }

    pub(crate) fn contains_key(&self, k: &K) -> bool {
        self.get(k).is_some()
    }

    pub(crate) fn insert(&mut self, k: K, v: V) -> Option<V> {
        match self {
            Self::Inline { len, items } => {
                let n = *len as usize;
                if let Some(slot) = items[..n].iter_mut().find(|(key, _)| *key == k) {
                    return Some(std::mem::replace(&mut slot.1, v));
                }
                if n < N {
                    items[n] = (k, v);
                    *len += 1;
                    return None;
                }
                let mut m: HashMap<K, V> = items.iter().copied().collect();
                m.insert(k, v);
                *self = Self::Heap(m);
                None
            }
            Self::Heap(m) => m.insert(k, v),
        }
    }

    pub(crate) fn remove(&mut self, k: &K) -> Option<V> {
        match self {
            Self::Inline { len, items } => {
                let n = *len as usize;
                let i = items[..n].iter().position(|(key, _)| key == k)?;
                let v = items[i].1;
                if !super::mutant::on(14) {
                    items[i] = items[n - 1];
                }
                items[n - 1] = (K::default(), V::default());
                *len -= 1;
                Some(v)
            }
            Self::Heap(m) => {
                let v = m.remove(k);
                if m.len() <= N / 2 {
                    let mut back = Self::default();
                    for (k, v) in m.drain() {
                        back.insert(k, v);
                    }
                    *self = back;
                }
                v
            }
        }
    }

    pub(crate) fn iter(&self) -> Iter<'_, K, V> {
        match self {
            Self::Inline { len, items } => Iter::Inline(items[..*len as usize].iter()),
            Self::Heap(m) => Iter::Heap(m.iter()),
        }
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, v)| v)
    }

    /// Every entry, by value, leaving the map empty and inline.
    pub(crate) fn drain(&mut self) -> IntoIter<K, V, N> {
        std::mem::take(self).into_iter()
    }

    /// Whether the map is inline (no heap allocation). Observation and tests.
    pub(crate) fn is_inline(&self) -> bool {
        matches!(self, Self::Inline { .. })
    }
}

impl<K, V, const N: usize> std::ops::Index<&K> for SmallMap<K, V, N>
where
    K: Copy + Default + Eq + Hash,
    V: Copy + Default,
{
    type Output = V;
    fn index(&self, k: &K) -> &V {
        self.get(k).expect("no entry with this key")
    }
}

pub(crate) enum Iter<'a, K, V> {
    Inline(std::slice::Iter<'a, (K, V)>),
    Heap(hash_map::Iter<'a, K, V>),
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Inline(it) => it.next().map(|(k, v)| (k, v)),
            Self::Heap(it) => it.next(),
        }
    }
}

pub(crate) enum IntoIter<K, V, const N: usize> {
    Inline(std::iter::Take<std::array::IntoIter<(K, V), N>>),
    Heap(hash_map::IntoIter<K, V>),
}

impl<K, V, const N: usize> Iterator for IntoIter<K, V, N> {
    type Item = (K, V);
    fn next(&mut self) -> Option<(K, V)> {
        match self {
            Self::Inline(it) => it.next(),
            Self::Heap(it) => it.next(),
        }
    }
}

impl<K, V, const N: usize> IntoIterator for SmallMap<K, V, N> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V, N>;
    fn into_iter(self) -> IntoIter<K, V, N> {
        match self {
            Self::Inline { len, items } => IntoIter::Inline(items.into_iter().take(len as usize)),
            Self::Heap(m) => IntoIter::Heap(m.into_iter()),
        }
    }
}

impl<'a, K, V, const N: usize> IntoIterator for &'a SmallMap<K, V, N>
where
    K: Copy + Default + Eq + Hash,
    V: Copy + Default,
{
    type Item = (&'a K, &'a V);
    type IntoIter = Iter<'a, K, V>;
    fn into_iter(self) -> Iter<'a, K, V> {
        self.iter()
    }
}

/// An ordered set holding up to `N` elements inline (sorted), then a `BTreeSet`.
pub(crate) enum SmallSet<T, const N: usize> {
    Inline { len: u8, items: [T; N] },
    Heap(BTreeSet<T>),
}

impl<T: Copy + Default, const N: usize> Default for SmallSet<T, N> {
    fn default() -> Self {
        Self::Inline {
            len: 0,
            items: [T::default(); N],
        }
    }
}

impl<T: Copy + Default + Ord, const N: usize> SmallSet<T, N> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Inline { len, .. } => *len as usize,
            Self::Heap(s) => s.len(),
        }
    }

    pub(crate) fn contains(&self, x: &T) -> bool {
        match self {
            Self::Inline { len, items } => items[..*len as usize].binary_search(x).is_ok(),
            Self::Heap(s) => s.contains(x),
        }
    }

    pub(crate) fn insert(&mut self, x: T) -> bool {
        match self {
            Self::Inline { len, items } => {
                let n = *len as usize;
                let Err(i) = items[..n].binary_search(&x) else {
                    return false;
                };
                if n < N {
                    if super::mutant::on(15) {
                        items[n] = x;
                    } else {
                        items.copy_within(i..n, i + 1);
                        items[i] = x;
                    }
                    *len += 1;
                    return true;
                }
                let mut s: BTreeSet<T> = items.iter().copied().collect();
                s.insert(x);
                *self = Self::Heap(s);
                true
            }
            Self::Heap(s) => s.insert(x),
        }
    }

    pub(crate) fn remove(&mut self, x: &T) -> bool {
        match self {
            Self::Inline { len, items } => {
                let n = *len as usize;
                let Ok(i) = items[..n].binary_search(x) else {
                    return false;
                };
                items.copy_within(i + 1..n, i);
                items[n - 1] = T::default();
                *len -= 1;
                true
            }
            Self::Heap(s) => {
                let removed = s.remove(x);
                if s.len() <= N / 2 {
                    let mut back = Self::default();
                    for x in std::mem::take(s) {
                        back.insert(x);
                    }
                    *self = back;
                }
                removed
            }
        }
    }

    pub(crate) fn clear(&mut self) {
        *self = Self::default();
    }

    /// Elements at or after `from`, in order.
    pub(crate) fn range(&self, range: std::ops::RangeFrom<T>) -> Range<'_, T> {
        match self {
            Self::Inline { len, items } => {
                let live = &items[..*len as usize];
                let i = live.partition_point(|x| *x < range.start);
                Range::Inline(live[i..].iter())
            }
            Self::Heap(s) => Range::Heap(s.range(range)),
        }
    }

    /// Whether the set is inline (no heap allocation). Observation and tests.
    pub(crate) fn is_inline(&self) -> bool {
        matches!(self, Self::Inline { .. })
    }
}

pub(crate) enum Range<'a, T> {
    Inline(std::slice::Iter<'a, T>),
    Heap(btree_set::Range<'a, T>),
}

impl<'a, T> Iterator for Range<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<&'a T> {
        match self {
            Self::Inline(it) => it.next(),
            Self::Heap(it) => it.next(),
        }
    }
}

pub(crate) enum SetIntoIter<T, const N: usize> {
    Inline(std::iter::Take<std::array::IntoIter<T, N>>),
    Heap(btree_set::IntoIter<T>),
}

impl<T, const N: usize> Iterator for SetIntoIter<T, N> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        match self {
            Self::Inline(it) => it.next(),
            Self::Heap(it) => it.next(),
        }
    }
}

impl<T, const N: usize> IntoIterator for SmallSet<T, N> {
    type Item = T;
    type IntoIter = SetIntoIter<T, N>;
    fn into_iter(self) -> SetIntoIter<T, N> {
        match self {
            Self::Inline { len, items } => SetIntoIter::Inline(items.into_iter().take(len as usize)),
            Self::Heap(s) => SetIntoIter::Heap(s.into_iter()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Rng(u64);
    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    /// F9-F T6: random inserts, overwrites and removes against `HashMap` and `BTreeSet` models,
    /// with sizes swinging across the spill point both ways: every read, iteration, range and
    /// drain agrees with the model, and the maps are inline whenever they have held N/2 or fewer
    /// since their last spill-back. The walk must spill and return, or it proves nothing.
    #[test]
    fn small_maps_agree_with_the_std_maps_across_spills() {
        walk::<2>();
        walk::<4>();
    }

    fn walk<const N: usize>() {
        let mut m: SmallMap<u32, u64, N> = SmallMap::default();
        let mut s: SmallSet<(u64, u32), N> = SmallSet::default();
        let mut mm: HashMap<u32, u64> = HashMap::new();
        let mut ms: BTreeSet<(u64, u32)> = BTreeSet::new();
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let (mut spills, mut returns, mut set_spills, mut set_returns) = (0, 0, 0, 0);
        for step in 0..200_000u64 {
            let keys = 1 + (step / 2_000) % 12;
            let k = rng.below(keys) as u32;
            let was_inline = m.is_inline();
            let set_was_inline = s.is_inline();
            if rng.below(2) == 0 {
                assert_eq!(m.insert(k, step), mm.insert(k, step), "step {step}");
                let e = (u64::from(k % 3), k);
                assert_eq!(s.insert(e), ms.insert(e), "step {step}");
            } else {
                assert_eq!(m.remove(&k), mm.remove(&k), "step {step}");
                let e = (u64::from(k % 3), k);
                assert_eq!(s.remove(&e), ms.remove(&e), "step {step}");
            }
            spills += usize::from(was_inline && !m.is_inline());
            returns += usize::from(!was_inline && m.is_inline());
            set_spills += usize::from(set_was_inline && !s.is_inline());
            set_returns += usize::from(!set_was_inline && s.is_inline());
            assert_eq!(m.len(), mm.len());
            assert_eq!(s.len(), ms.len());
            if m.len() <= N / 2 {
                assert!(m.is_inline(), "N={N} step {step}: {} entries on the heap", m.len());
            }
            if s.len() <= N / 2 {
                assert!(s.is_inline(), "N={N} step {step}: {} elements on the heap", s.len());
            }
            if m.len() > N {
                assert!(!m.is_inline(), "N={N} step {step}: {} entries inline", m.len());
            }
            if step % 97 == 0 {
                let mut got: Vec<(u32, u64)> = m.iter().map(|(&k, &v)| (k, v)).collect();
                let mut want: Vec<(u32, u64)> = mm.iter().map(|(&k, &v)| (k, v)).collect();
                got.sort();
                want.sort();
                assert_eq!(got, want, "step {step}: iteration");
                for k in 0..12 {
                    assert_eq!(m.get(&k), mm.get(&k));
                    assert_eq!(m.contains_key(&k), mm.contains_key(&k));
                }
                let from = (rng.below(3), rng.below(12) as u32);
                let got: Vec<_> = s.range(from..).copied().collect();
                let want: Vec<_> = ms.range(from..).copied().collect();
                assert_eq!(got, want, "step {step}: range");
                for x in &ms {
                    assert!(s.contains(x));
                }
            }
            if step % 9_973 == 0 {
                let mut got: Vec<(u32, u64)> = m.drain().collect();
                let mut want: Vec<(u32, u64)> = mm.drain().collect();
                got.sort();
                want.sort();
                assert_eq!(got, want, "step {step}: drain");
                assert!(m.is_inline() && m.len() == 0);
                let got: Vec<_> = std::mem::take(&mut s).into_iter().collect();
                let want: Vec<_> = std::mem::take(&mut ms).into_iter().collect();
                assert_eq!(got, want, "step {step}: set by value");
            }
        }
        assert!(spills > 10 && returns > 10, "N={N}: spills {spills}, returns {returns}");
        assert!(
            set_spills > 10 && set_returns > 10,
            "N={N}: set spills {set_spills}, returns {set_returns}"
        );
    }
}
