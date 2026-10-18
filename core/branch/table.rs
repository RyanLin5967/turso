//! The branch table: branch states by id, in fixed-size chunks that never move. F8′ -- r11-ever's F8
//! (turso 5d580203f, "the branch table is a slot map ... chunks that never move, so churn leaves no
//! tombstones and never rehashes"), ported to the durable store for the githost-shape lane WITHOUT F8's
//! generational id recycling: the durable store's ids are minted from `next_id`, persisted in its log
//! and snapshot, and never reused, so the table indexes chunks by the id itself.
//!
//! Why: `std::collections::HashMap` grows by reallocating and moving EVERY entry, under the store's one
//! mutex. In the git-hosting shape the table only grows (refs are never deleted), so every doubling is a
//! fork that stalls for Theta(N): githost-shape measured one fork moving 458,125 branch states at
//! N = 5 x 10^5 (raw/port2_probeT_1024_500000.txt). Here growing allocates one chunk and, at worst,
//! reallocates the chunk DIRECTORY (N / 1024 pointers); no branch state ever moves. A chunk whose
//! states are all gone is freed.
//!
//! L2 slice (fastest-engine, budget create_allocates_no_table_of_every_branch_under_the_store_mutex,
//! 64 KiB per create under the store mutex): a chunk of 1024 branch states was 376,832 bytes
//! (artie raw l8-chunk_1c58a6f25.txt), allocated whole by the create that first needs it. A chunk is
//! now CHUNK = 64 states (23,552 bytes for a 368-byte state), and the directory is TWO-LEVEL
//! (r11-adversarial's F-R1b): fixed segments of SEG = 1024 chunk slots (24 KiB each) behind a top
//! `Vec` of segments, so the create that needs a new segment AND a new chunk allocates about 47 KiB
//! (128 states a chunk measured 73,236 bytes there, over the budget: artie raw
//! l10-d0growth_22c5c5da3.txt), and a top doubling moves N / 65,536 pointers (16 at 10^6).
//! `growth()` counts the top's doublings.

use super::BranchId;
use std::collections::BTreeMap;

/// A state's row as a checkpoint writes it (L2(d)'s copy-on-write, `BranchTable::freeze`).
pub(crate) trait Preimage {
    type Row;
    fn preimage(&self, id: BranchId) -> Self::Row;
}

impl Preimage for u64 {
    type Row = u64;
    fn preimage(&self, _: BranchId) -> u64 {
        *self
    }
}

/// Test builds: changes that reached a state whose row a freeze still owed (`BranchTable::preserve`),
/// a test's premise that its window was hit.
#[cfg(test)]
pub(crate) static COW_PRESERVES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The rows a fuzzy checkpoint's capture froze (L2(d), O(1) under the store mutex): the ids it
/// covers and their dirty bits, swapped in whole at the capture. Each is encoded once, as of the
/// capture: by the table, the moment before the first change to its state (`get_mut`, `insert` over
/// it, `remove`), or by the writer, in batches (`encode_frozen`), from a state nothing has changed
/// since the capture. So every row is exact to the capture point, which the replay of the log
/// suffix after it needs (a Commit replayed onto a row that already holds it frees the slot it maps:
/// fastest-engine NOTES, "L2(d)").
pub(crate) struct Frozen<R> {
    pub(crate) pending: BTreeMap<BranchId, u8>,
    pub(crate) done: Vec<(R, u8)>,
}

pub(crate) const CHUNK: usize = 64;
/// Chunk slots per directory segment.
const SEG: usize = 1024;

/// LEAP L2(b) (ASYNC-DESIGN R1.2, "the branch table never allocates on the create path"): the
/// emptied chunks and segments kept, and the spares the store's background writer allocates off
/// the store mutex (`BranchStore::refill_table_pool`), so an insert takes one instead of
/// allocating it in its caller's hold. 16 chunks are 1,024 ids, about 376 KB at a 368-byte state;
/// the writer refills at every flight's landing once the pool falls to `CHUNK_POOL_LOW`.
pub(crate) const CHUNK_POOL: usize = 16;
const CHUNK_POOL_LOW: usize = 8;
pub(crate) const SEG_POOL: usize = 1;

/// Chunks and segments allocated off the store mutex for a table's pool (`BranchTable::new_spares`).
pub(crate) struct Spares<T> {
    chunks: Vec<Chunk<T>>,
    segments: Vec<Segment<T>>,
}

struct Chunk<T> {
    entries: Box<[Option<(BranchId, T)>]>,
    live: usize,
}

type Segment<T> = Box<[Option<Chunk<T>>]>;

pub(crate) struct BranchTable<T: Preimage> {
    /// A fuzzy checkpoint's frozen rows, encoded on first change (see [`Frozen`]).
    frozen: Option<Frozen<T::Row>>,
    dir: Vec<Option<Segment<T>>>,
    len: usize,
    chunks: usize,
    /// Directory reallocations, and the chunk pointers they moved (observation only).
    dir_grows: u64,
    dir_moved: u64,
    /// Chunks allocated since the table was made (r13-compose I6, observation only).
    chunk_allocs: u64,
    /// LEAP L2(b)'s pool: emptied or pre-allocated chunks and segments, at most `CHUNK_POOL` and
    /// `SEG_POOL` (their capacity is reserved at `new`, so keeping one never allocates).
    spare_chunks: Vec<Chunk<T>>,
    spare_segments: Vec<Segment<T>>,
    /// Chunks and segments an insert had to allocate itself, the pool being empty (observation).
    inline_allocs: u64,
}

impl<T: Preimage> BranchTable<T> {
    pub(crate) fn new() -> Self {
        Self {
            frozen: None,
            dir: Vec::new(),
            len: 0,
            chunks: 0,
            dir_grows: 0,
            dir_moved: 0,
            chunk_allocs: 0,
            spare_chunks: Vec::with_capacity(CHUNK_POOL),
            spare_segments: Vec::with_capacity(SEG_POOL),
            inline_allocs: 0,
        }
    }

    fn new_chunk() -> Chunk<T> {
        Chunk {
            entries: (0..CHUNK).map(|_| None).collect(),
            live: 0,
        }
    }

    fn new_segment() -> Segment<T> {
        (0..SEG).map(|_| None).collect()
    }

    /// The spares that bring a pool to `CHUNK_POOL` chunks and `SEG_POOL` segments, given what it
    /// needs (`pool_need`), allocated here: called off the store mutex. Writing every slot touches
    /// every page, so an insert that takes one faults nothing in.
    pub(crate) fn new_spares((chunks, segments): (usize, usize)) -> Spares<T> {
        Spares {
            chunks: (0..chunks).map(|_| Self::new_chunk()).collect(),
            segments: (0..segments).map(|_| Self::new_segment()).collect(),
        }
    }

    /// The chunks and segments the pool lacks.
    pub(crate) fn pool_need(&self) -> (usize, usize) {
        (
            CHUNK_POOL.saturating_sub(self.spare_chunks.len()),
            SEG_POOL.saturating_sub(self.spare_segments.len()),
        )
    }

    /// Whether the pool is low enough that the writer should refill it.
    pub(crate) fn pool_low(&self) -> bool {
        self.spare_chunks.len() <= CHUNK_POOL_LOW || self.spare_segments.is_empty()
    }

    /// Keep `spares` in the pool, up to its bounds (no allocation: the capacity is reserved). What
    /// does not fit is handed back, for the caller to free after it lets the store mutex go.
    pub(crate) fn add_spares(&mut self, mut spares: Spares<T>) -> Spares<T> {
        while self.spare_chunks.len() < CHUNK_POOL {
            let Some(c) = spares.chunks.pop() else { break };
            self.spare_chunks.push(c);
        }
        while self.spare_segments.len() < SEG_POOL {
            let Some(g) = spares.segments.pop() else { break };
            self.spare_segments.push(g);
        }
        spares
    }

    /// Chunks and segments inserts allocated themselves, the pool being empty (LEAP L2(b)).
    pub(crate) fn inline_allocs(&self) -> u64 {
        self.inline_allocs
    }

    /// Test builds: empty the pool (engine-rev-l2b HIGH 3's schedule: a hold that leaves it low).
    #[cfg(test)]
    pub(crate) fn drain_pool_for_test(&mut self) {
        self.spare_chunks.clear();
        self.spare_segments.clear();
    }

    /// The ids the pool's spare chunks cover.
    pub(crate) fn spare_ids(&self) -> u64 {
        (self.spare_chunks.len() * CHUNK) as u64
    }

    /// (segment, chunk within it, slot within the chunk).
    fn at(id: &BranchId) -> (usize, usize, usize) {
        let i = usize::try_from(id.0).expect("a branch id fits usize");
        let c = i / CHUNK;
        (c / SEG, c % SEG, i % CHUNK)
    }

    pub(crate) fn get(&self, id: &BranchId) -> Option<&T> {
        let (g, c, i) = Self::at(id);
        self.dir.get(g)?.as_ref()?[c].as_ref()?.entries[i].as_ref().map(|(_, v)| v)
    }

    pub(crate) fn get_mut(&mut self, id: &BranchId) -> Option<&mut T> {
        self.preserve(id);
        let (g, c, i) = Self::at(id);
        self.dir.get_mut(g)?.as_mut()?[c].as_mut()?.entries[i].as_mut().map(|(_, v)| v)
    }

    pub(crate) fn contains_key(&self, id: &BranchId) -> bool {
        self.get(id).is_some()
    }

    pub(crate) fn insert(&mut self, id: BranchId, val: T) -> Option<T> {
        self.preserve(&id);
        let (g, c, i) = Self::at(&id);
        if g >= self.dir.len() {
            let (len, cap) = (self.dir.len(), self.dir.capacity());
            self.dir.resize_with(g + 1, || None);
            if self.dir.capacity() != cap {
                self.dir_grows += 1;
                self.dir_moved += len as u64;
            }
        }
        // LEAP L2(b): a missing segment or chunk comes from the pool; one is built here, in the
        // caller's hold, only when the pool is empty (counted). Mutant `chunk_inline` (test and
        // fe-mutants builds): always built here, as before.
        let inline = super::store::fe_mutant("chunk_inline");
        if self.dir[g].is_none() {
            let segment = match self.spare_segments.pop().filter(|_| !inline) {
                Some(segment) => segment,
                None => {
                    self.inline_allocs += 1;
                    Self::new_segment()
                }
            };
            self.dir[g] = Some(segment);
        }
        if self.dir[g].as_ref().is_some_and(|segment| segment[c].is_none()) {
            let chunk = match self.spare_chunks.pop().filter(|_| !inline) {
                Some(chunk) => chunk,
                None => {
                    self.inline_allocs += 1;
                    Self::new_chunk()
                }
            };
            self.dir[g].as_mut().expect("placed above")[c] = Some(chunk);
        }
        let segment = self.dir[g].as_mut().expect("placed above");
        let chunk = segment[c].as_mut().expect("placed above");
        let old = chunk.entries[i].replace((id, val)).map(|(_, v)| v);
        if old.is_none() {
            if chunk.live == 0 {
                self.chunks += 1;
                self.chunk_allocs += 1;
            }
            chunk.live += 1;
            self.len += 1;
        }
        old
    }

    pub(crate) fn remove(&mut self, id: &BranchId) -> Option<T> {
        self.preserve(id);
        let (g, c, i) = Self::at(id);
        let segment = self.dir.get_mut(g)?.as_mut()?;
        let chunk = segment[c].as_mut()?;
        let (_, val) = chunk.entries[i].take()?;
        chunk.live -= 1;
        self.len -= 1;
        if chunk.live == 0 {
            let emptied = segment[c].take().expect("the chunk just emptied");
            self.chunks -= 1;
            // An emptied chunk (every slot None again) is kept for the next insert while the pool
            // has room (LEAP L2(b)), else freed.
            if self.spare_chunks.len() < CHUNK_POOL {
                self.spare_chunks.push(emptied);
            }
            // A segment whose chunks are all gone is kept or freed the same way.
            if segment.iter().all(Option::is_none) {
                let emptied = self.dir[g].take().expect("the segment just emptied");
                if self.spare_segments.len() < SEG_POOL {
                    self.spare_segments.push(emptied);
                }
            }
        }
        Some(val)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// The moment before `id`'s state changes: if a frozen checkpoint still owes its row, encode
    /// it now, as of the capture. O(1) when nothing is frozen or `id` is not owed.
    #[inline]
    fn preserve(&mut self, id: &BranchId) {
        // Mutant `cow_preserve_skipped` (test builds only): a change reaches a frozen state with no
        // pre-image, so the writer encodes it as changed (`FE_CAPTURE_VERIFY` must see it).
        #[cfg(test)]
        if self.frozen.as_ref().is_some_and(|f| f.pending.contains_key(id)) {
            COW_PRESERVES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if super::store::fe_mutant("cow_preserve_skipped") {
                return;
            }
        }
        let Some(what) = self.frozen.as_mut().and_then(|f| f.pending.remove(id)) else {
            return;
        };
        if let Some(row) = self.get(id).map(|v| v.preimage(*id)) {
            if let Some(f) = self.frozen.as_mut() {
                f.done.push((row, what));
            }
        }
    }

    /// A fuzzy checkpoint's capture: freeze the rows of `dirty` (swapped in, O(1)). Refused, the
    /// map handed back, while another freeze is still owed rows.
    pub(crate) fn freeze(&mut self, dirty: BTreeMap<BranchId, u8>) -> Result<(), BTreeMap<BranchId, u8>> {
        if self.frozen.is_some() {
            return Err(dirty);
        }
        self.frozen = Some(Frozen { pending: dirty, done: Vec::new() });
        Ok(())
    }

    /// The writer's batch: encode up to `max` owed rows from states unchanged since the capture,
    /// appended to `out` (the writer's own vector, grown outside the store mutex between batches,
    /// so no batch reallocates every row so far: 4,718,592 B in one hold at 19k rows, raw l104).
    /// `true`, the freeze ended and its pre-images moved into `out`, once none is owed; the rows of
    /// ids with no state (removed before the capture) are skipped, as the capture skipped them.
    pub(crate) fn encode_frozen(&mut self, max: usize, out: &mut Vec<(T::Row, u8)>) -> bool {
        for _ in 0..max {
            let Some((id, what)) = self.frozen.as_mut().and_then(|f| f.pending.pop_first()) else {
                break;
            };
            if let Some(row) = self.get(&id).map(|v| v.preimage(id)) {
                out.push((row, what));
            }
        }
        if self.frozen.as_ref().is_some_and(|f| f.pending.is_empty()) {
            if let Some(f) = self.frozen.take() {
                out.extend(f.done);
            }
            return true;
        }
        self.frozen.is_none()
    }

    /// A freeze that will not be written (a capture undone): its ids and dirty bits, owed or
    /// encoded, to mark dirty again. Empty if nothing is frozen.
    pub(crate) fn unfreeze(&mut self, id_of: impl Fn(&T::Row) -> BranchId) -> Vec<(BranchId, u8)> {
        let Some(f) = self.frozen.take() else {
            return Vec::new();
        };
        f.pending.into_iter().chain(f.done.iter().map(|(r, w)| (id_of(r), *w))).collect()
    }

    /// Rows still owed by a freeze (an id the eviction must keep).
    pub(crate) fn frozen_owes(&self, id: &BranchId) -> bool {
        self.frozen.as_ref().is_some_and(|f| f.pending.contains_key(id))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Slots in allocated chunks.
    pub(crate) fn capacity(&self) -> usize {
        self.chunks * CHUNK
    }

    /// `(chunks allocated now, chunks allocated since the table was made)` (r13-compose I6).
    pub(crate) fn chunk_stats(&self) -> (u64, u64) {
        (self.chunks as u64, self.chunk_allocs)
    }

    /// Test builds: chunks allocated now.
    #[cfg(test)]
    pub(crate) fn chunks_for_test(&self) -> usize {
        self.chunks
    }

    /// `(top directory reallocations, segment pointers they moved)` since the table was made.
    pub(crate) fn growth(&self) -> (u64, u64) {
        (self.dir_grows, self.dir_moved)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&BranchId, &T)> {
        self.dir
            .iter()
            .flatten()
            .flat_map(|g| g.iter().flatten())
            .flat_map(|c| c.entries.iter().flatten().map(|(id, v)| (id, v)))
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &T> {
        self.iter().map(|(_, v)| v)
    }

    /// `iter`, adding to `scanned` every directory entry and every chunk slot the walk visits, empty
    /// or not, as far as the walk is driven (r13-compose: A2.F5's `walk_slots_scanned`, reported
    /// for B_ALL only; review wf_5c230f31 M: a walk's cost is the slots, not the live items).
    pub(crate) fn iter_scanned<'a>(
        &'a self,
        scanned: &'a std::cell::Cell<u64>,
    ) -> impl Iterator<Item = (&'a BranchId, &'a T)> + 'a {
        self.dir
            .iter()
            .flatten()
            .flat_map(|g| g.iter())
            .inspect(move |_| scanned.set(scanned.get() + 1))
            .flatten()
            .flat_map(move |c| {
                c.entries
                    .iter()
                    .inspect(move |_| scanned.set(scanned.get() + 1))
                    .flatten()
                    .map(|(id, v)| (id, v))
            })
    }
}

impl<'a, T: Preimage> IntoIterator for &'a BranchTable<T> {
    type Item = (&'a BranchId, &'a T);
    type IntoIter = Box<dyn Iterator<Item = (&'a BranchId, &'a T)> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

/// As `HashMap`'s: panics when the id has no state.
impl<T: Preimage> std::ops::Index<&BranchId> for BranchTable<T> {
    type Output = T;

    fn index(&self, id: &BranchId) -> &T {
        self.get(id).expect("no branch state for this id")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Against a BTreeMap model under inserts and removes, including whole chunks emptied and refilled;
    /// iteration is ascending by id; no state moves (a pointer taken before thousands of inserts still
    /// names the same state).
    #[test]
    fn a_branch_table_matches_a_model_and_never_moves_a_state() {
        let mut t: BranchTable<u64> = BranchTable::new();
        let mut m: BTreeMap<u64, u64> = BTreeMap::new();
        t.insert(BranchId(1), 11);
        m.insert(1, 11);
        let pinned = t.get(&BranchId(1)).unwrap() as *const u64;
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        for step in 2..60_000u64 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            t.insert(BranchId(step), step * 3);
            m.insert(step, step * 3);
            if x % 3 == 0 && step > 2 {
                let victim = 2 + x % (step - 1);
                assert_eq!(t.remove(&BranchId(victim)), m.remove(&victim));
            }
            if step % 7_000 == 0 {
                // Empty a whole chunk, then refill part of it.
                for id in (step - 3_000)..(step - 1_000) {
                    assert_eq!(t.remove(&BranchId(id)), m.remove(&id));
                }
            }
        }
        assert_eq!(t.get(&BranchId(1)).unwrap() as *const u64, pinned, "a state moved");
        assert_eq!(t.len(), m.len());
        let got: Vec<(u64, u64)> = t.iter().map(|(id, &v)| (id.0, v)).collect();
        let want: Vec<(u64, u64)> = m.iter().map(|(&k, &v)| (k, v)).collect();
        assert_eq!(got, want);
        for id in 0..60_100u64 {
            assert_eq!(t.get(&BranchId(id)).copied(), m.get(&id).copied());
        }
        assert!(t.capacity() >= t.len());
    }

    /// The two-level directory (L2 slice): ids across several segments (CHUNK x SEG = 65,536 ids
    /// each), a whole segment emptied (freed) and refilled, sparse ids far apart; against the model,
    /// with every chunk this table allocates at most 64 KiB for a 368-byte state.
    #[test]
    fn a_branch_table_spans_segments_and_frees_an_empty_one() {
        let span = (CHUNK * SEG) as u64;
        let mut t: BranchTable<u64> = BranchTable::new();
        let mut m: BTreeMap<u64, u64> = BTreeMap::new();
        let ids: Vec<u64> = (0..300).map(|k| k * 1_009).chain([span - 1, span, span + 1, 3 * span + 7, 10 * span]).collect();
        for &id in &ids {
            assert_eq!(t.insert(BranchId(id), id + 5), m.insert(id, id + 5));
        }
        // Empty segment 1 (it held span, span + 1, and the k * 1009 ids in it), then refill one id.
        let seg1: Vec<u64> = m.keys().copied().filter(|id| id / span == 1).collect();
        assert!(!seg1.is_empty(), "premise: segment 1 holds ids");
        for id in &seg1 {
            assert_eq!(t.remove(&BranchId(*id)), m.remove(id));
        }
        assert!(t.dir.get(1).is_none_or(Option::is_none), "an empty segment was not freed");
        assert_eq!(t.insert(BranchId(span + 2), 9), m.insert(span + 2, 9));
        let got: Vec<(u64, u64)> = t.iter().map(|(id, &v)| (id.0, v)).collect();
        let want: Vec<(u64, u64)> = m.iter().map(|(&k, &v)| (k, v)).collect();
        assert_eq!(got, want);
        for &id in ids.iter().chain([span + 2, 2 * span, 11 * span].iter()) {
            assert_eq!(t.get(&BranchId(id)).copied(), m.get(&id).copied(), "id {id}");
        }
        assert_eq!(t.len(), m.len());
        assert!(CHUNK * 368 + SEG * 24 <= 64 << 10, "premise: a chunk of 368-byte states and a segment fit 64 KiB together");
    }

    /// LEAP L2(b)'s pool: inserts that need a segment and chunks take them from the pool (none
    /// built inline while it lasts), one built inline once it is empty is counted, an emptied chunk
    /// goes back to the pool, and the pool never holds more than its bounds (the surplus is handed
    /// back).
    #[test]
    fn a_branch_table_takes_its_chunks_from_the_pool_and_keeps_emptied_ones() {
        let mut t: BranchTable<u64> = BranchTable::new();
        assert_eq!(t.pool_need(), (CHUNK_POOL, SEG_POOL));
        assert!(t.pool_low());
        let surplus = t.add_spares(BranchTable::new_spares((CHUNK_POOL + 3, SEG_POOL + 1)));
        assert_eq!((surplus.chunks.len(), surplus.segments.len()), (3, 1), "the surplus was kept");
        assert_eq!(t.pool_need(), (0, 0));
        let ids = (CHUNK * CHUNK_POOL) as u64;
        for id in 0..ids {
            t.insert(BranchId(id), id);
        }
        assert_eq!(t.inline_allocs(), 0, "an insert built a chunk or segment while the pool lasted");
        assert_eq!(t.pool_need(), (CHUNK_POOL, SEG_POOL));
        t.insert(BranchId(ids), ids);
        assert_eq!(t.inline_allocs(), 1, "the chunk built on an empty pool was not counted");
        for id in 0..CHUNK as u64 {
            t.remove(&BranchId(id));
        }
        assert_eq!(t.pool_need(), (CHUNK_POOL - 1, SEG_POOL), "the emptied chunk was not kept");
        t.insert(BranchId(5), 5);
        assert_eq!(t.inline_allocs(), 1, "a re-filled chunk was built though one was kept");
        assert_eq!(t.get(&BranchId(5)), Some(&5));
        assert_eq!(t.len(), CHUNK * CHUNK_POOL - CHUNK + 2);
    }

    /// L2(d)'s copy-on-write: a frozen row is encoded as of the freeze whichever way the state is
    /// next reached (get_mut, an insert over it, a remove) or by the writer's batches, exactly once
    /// each; an unfrozen id is never encoded; an undone freeze gives every id back.
    #[test]
    fn a_frozen_row_is_its_value_at_the_freeze_however_it_is_next_changed() {
        let mut t: BranchTable<u64> = BranchTable::new();
        for i in 1..=6 {
            t.insert(BranchId(i), i * 10);
        }
        let dirty: BTreeMap<BranchId, u8> = (1..=5).map(|i| (BranchId(i), i as u8)).collect();
        assert!(t.freeze(dirty).is_ok());
        assert!(t.freeze(BTreeMap::new()).is_err(), "a second freeze was accepted while rows were owed");
        *t.get_mut(&BranchId(1)).unwrap() = 111;
        *t.get_mut(&BranchId(1)).unwrap() = 1111;
        t.insert(BranchId(2), 222);
        t.remove(&BranchId(3));
        *t.get_mut(&BranchId(6)).unwrap() = 666;
        let mut rows = Vec::new();
        while !t.encode_frozen(1, &mut rows) {}
        rows.sort();
        assert_eq!(rows, vec![(10, 1), (20, 2), (30, 3), (40, 4), (50, 5)], "rows not as of the freeze");
        let mut again = Vec::new();
        assert!(t.encode_frozen(64, &mut again) && again.is_empty(), "a freeze ended twice");
        assert_eq!(t[&BranchId(1)], 1111);
        assert!(t.freeze((1..=2).map(|i| (BranchId(i), 9)).collect()).is_ok());
        *t.get_mut(&BranchId(1)).unwrap() = 7;
        let mut back = t.unfreeze(|r| BranchId(*r / 1111));
        back.sort();
        assert_eq!(back, vec![(BranchId(1), 9), (BranchId(2), 9)], "an undone freeze lost an id");
    }
}
