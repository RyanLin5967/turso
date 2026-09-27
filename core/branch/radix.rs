//! A map from page number to `T` that any thread can read without a lock. Moved out of `store`
//! unchanged (lane r11-k3-trunklock) so that F-K3v's version lists (`olc`) can use it too.

use crate::sync::OnceLock;

/// A map from page number to `T` that any thread can read without a lock: three levels of 2^10,
/// 2^10 and 2^12 entries over the 32-bit page number, each installed once and never freed or moved
/// until the store drops. A lookup is three acquire loads and no write, so readers share the lines
/// they read instead of taking them from one another.
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

    fn split(page: u32) -> (usize, usize, usize) {
        let page = page as usize;
        (page >> 22, (page >> 12) & (Self::MID - 1), page & (Self::LEAF - 1))
    }

    pub(crate) fn get(&self, page: u32) -> Option<&T> {
        let (t, m, l) = Self::split(page);
        let leaf = self.top.get()?[t].get()?[m].get()?;
        Some(&leaf[l])
    }

    pub(crate) fn get_or_insert(&self, page: u32) -> &T {
        let (t, m, l) = Self::split(page);
        let top = self
            .top
            .get_or_init(|| (0..Self::TOP).map(|_| OnceLock::new()).collect());
        let mid = top[t].get_or_init(|| (0..Self::MID).map(|_| OnceLock::new()).collect());
        let leaf = mid[m].get_or_init(|| (0..Self::LEAF).map(|_| T::default()).collect());
        &leaf[l]
    }
}
