//! A map from a `u32` key to `T` that any thread can read without a lock (F5's `Radix`, turso
//! a7adf8704 store.rs): three levels of 2^10, 2^10 and 2^12 entries over the key, each installed once
//! and never freed or moved until the map drops. A lookup is three acquire loads and no write, so
//! readers share the lines they read instead of taking them from one another.

use std::sync::OnceLock;

pub(crate) struct Radix<T> {
    top: OnceLock<Box<[OnceLock<Box<[OnceLock<Box<[T]>>]>>]>>,
}

impl<T: Default> Radix<T> {
    const TOP: usize = 1 << 10;
    const MID: usize = 1 << 10;
    const LEAF: usize = 1 << 12;

    pub(crate) fn new() -> Self {
        Self {
            top: OnceLock::new(),
        }
    }

    fn split(key: u32) -> (usize, usize, usize) {
        let key = key as usize;
        (key >> 22, (key >> 12) & (Self::MID - 1), key & (Self::LEAF - 1))
    }

    pub(crate) fn get(&self, key: u32) -> Option<&T> {
        let (t, m, l) = Self::split(key);
        let leaf = self.top.get()?[t].get()?[m].get()?;
        Some(&leaf[l])
    }

    pub(crate) fn get_or_insert(&self, key: u32) -> &T {
        let (t, m, l) = Self::split(key);
        let top = self
            .top
            .get_or_init(|| (0..Self::TOP).map(|_| OnceLock::new()).collect());
        let mid = top[t].get_or_init(|| (0..Self::MID).map(|_| OnceLock::new()).collect());
        let leaf = mid[m].get_or_init(|| (0..Self::LEAF).map(|_| T::default()).collect());
        &leaf[l]
    }
}
