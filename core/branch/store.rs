//! Per-branch page spaces, and the rule that decides which version of a page a branch sees.
//!
//! # The model
//!
//! Every node in the branch tree — the trunk and each branch — carries an `epoch` that its own
//! forks advance: a child forked from a node records the node's epoch at that moment as its
//! `fork_epoch`, and the node's epoch then increments. A version of a page that a node wrote in
//! epoch `born` is visible to that node's children forked at any epoch `>= born`, until the node
//! overwrites it in epoch `died`; after that it is visible only to children forked in
//! `[born, died)`. A branch therefore sees, for each page:
//!
//! 1. its own current version, if it has written the page; else
//! 2. its parent's version at the branch's `fork_epoch` — the parent's current version if it was
//!    born at or before that epoch, else the parent's RETAINED version covering it; else
//! 3. the same question one level up, down to the trunk, whose current version lives in the WAL
//!    and the database file and is read by the ordinary pager path.
//!
//! # Where the copies come from — the write ticket
//!
//! A version is retained, or a branch gets its own copy, at exactly one moment: the first
//! [`crate::storage::pager::Pager::add_dirty`] of a page in a transaction, which is also the only
//! place a [`crate::storage::pager::WriteTicket`] can be minted. So every page write is preceded by
//! the copy decision by construction:
//!
//! * a **branch** writing a page it does not yet own copies the page into a fresh slot of its own
//!   page space; writing a page it owns while a live child can still see the current version moves
//!   that version to the retained set and copies into a fresh slot;
//! * the **trunk** writing a page that a live child can still see copies the pre-image into a slot
//!   and retains it, before the write reaches the WAL — so neither the commit nor a later
//!   checkpoint that moves the new version into the database file can reach the child.
//!
//! # Reclamation
//!
//! A retained version is garbage once no live child of its node forked inside `[born, died)`.
//! Removing the child forked at `f` can only make versions containing `f` garbage, and a version
//! containing `f` becomes garbage exactly when it also lies strictly between `f`'s neighbouring
//! live siblings — which is a range query over `retained_by_born`, not a scan.
//!
//! A branch whose handle has been dropped but that still has a live child or an open connection
//! is kept (its versions are still read through); it is freed the moment the last of those goes,
//! and freeing it may in turn free its parent.
//!
//! # What this does not do
//!
//! * One `Mutex` guards every branch. Correct, and a known wall under concurrent writers on
//!   different branches; the benchmark this lane ships is single-threaded and says so.
//! * Resolution walks the ancestor chain: cost grows with DEPTH, not with the number of branches.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;

use super::arena::{Arena, Slot};
use super::{BranchId, BranchStats, Reaped};
use crate::schema::Schema;
use crate::storage::pager::PageRef;
use crate::sync::atomic::{AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex};
use crate::{LimboError, Result};

pub(crate) struct BranchStore {
    inner: Mutex<StoreInner>,
    /// Live children of the trunk. Read without the lock on every trunk first-write so that a
    /// database with no branches pays one atomic load per written page and nothing else.
    ///
    /// The unlocked read is sound because the only transition that matters — 0 to 1 — happens in
    /// a trunk fork, which holds the trunk's WAL write lock; a trunk writer reading this holds the
    /// same lock. A 1-to-0 transition (a reap) racing the read only makes the writer take the lock
    /// and find nothing to do.
    trunk_children: AtomicUsize,
}

struct StoreInner {
    arena: Option<Arena>,
    next_id: u64,
    trunk: TrunkState,
    branches: HashMap<BranchId, BranchState>,
}

#[derive(Default)]
struct Lineage {
    /// Advanced by each fork of this node; the pre-increment value is the child's fork epoch.
    epoch: u64,
    /// Live children by fork epoch. Fork epochs are unique within a parent.
    children: BTreeMap<u64, BranchId>,
    /// Superseded versions kept because a live child forked while they were current.
    retained: HashMap<u32, Vec<Retained>>,
    /// The same versions keyed by `born`, for the reclamation range query: born -> [(page, died)].
    retained_by_born: BTreeMap<u64, Vec<(u32, u64)>>,
}

#[derive(Clone, Copy)]
struct Retained {
    born: u64,
    died: u64,
    slot: Slot,
}

struct TrunkState {
    lineage: Lineage,
    /// The trunk epoch of its last write to each page. Absent means "before the first fork that
    /// was live at the time", i.e. epoch 0, which is the conservative answer: it can only cause a
    /// retention that was not strictly needed, never skip one that was.
    written: HashMap<u32, u64>,
}

struct BranchState {
    parent: BranchId,
    fork_epoch: u64,
    lineage: Lineage,
    /// The branch's current version of every page it has written.
    current: HashMap<u32, Owned>,
    /// The branch's committed schema. Shared with the parent at fork (an `Arc` clone), replaced by
    /// a committed DDL on the branch.
    schema: Arc<Schema>,
    /// The `Branch` handle is alive.
    handle: bool,
    /// A connection is open on this branch.
    open: bool,
    /// A write transaction on this branch is in progress.
    writer: bool,
}

#[derive(Clone, Copy)]
struct Owned {
    slot: Slot,
    born: u64,
}

impl Lineage {
    /// True if a live child forked in `[from, to)` can see a version current over that range.
    fn has_child_in(&self, from: u64, to: u64) -> bool {
        from < to && self.children.range(from..to).next().is_some()
    }

    fn retain(&mut self, page: u32, v: Retained) {
        self.retained.entry(page).or_default().push(v);
        self.retained_by_born
            .entry(v.born)
            .or_default()
            .push((page, v.died));
    }

    /// The retained version of `page` visible to a child forked at `f`.
    fn retained_at(&self, page: u32, f: u64) -> Option<Slot> {
        self.retained
            .get(&page)?
            .iter()
            .find(|v| v.born <= f && f < v.died)
            .map(|v| v.slot)
    }

    /// Detach the child forked at `f` and release every retained version that only it could see.
    fn child_gone(&mut self, f: u64, arena: &mut Arena) -> usize {
        let removed = self.children.remove(&f);
        crate::turso_assert!(removed.is_some(), "detached a child the parent does not list");
        let lo = self.children.range(..f).next_back().map(|(&e, _)| e);
        let hi = self.children.range(f..).next().map(|(&e, _)| e);
        // A version containing f is garbage iff its range [born, died) now holds no live child:
        // born > lo (nothing live below) and died <= hi (nothing live above).
        let from = match lo {
            Some(lo) => Bound::Excluded(lo),
            None => Bound::Unbounded,
        };
        let mut dead = Vec::new();
        for (&born, entries) in self.retained_by_born.range((from, Bound::Included(f))) {
            for &(page, died) in entries {
                if died > f && hi.is_none_or(|hi| died <= hi) {
                    dead.push((born, page));
                }
            }
        }
        for &(born, page) in &dead {
            let versions = self.retained.get_mut(&page).expect("indexed version is listed");
            let at = versions
                .iter()
                .position(|v| v.born == born)
                .expect("indexed version is listed");
            let v = versions.swap_remove(at);
            if versions.is_empty() {
                self.retained.remove(&page);
            }
            let by_born = self
                .retained_by_born
                .get_mut(&born)
                .expect("listed version is indexed");
            by_born.retain(|&(p, _)| p != page);
            if by_born.is_empty() {
                self.retained_by_born.remove(&born);
            }
            arena.release(v.slot);
        }
        dead.len()
    }

    fn release_all(self, arena: &mut Arena) -> Vec<Slot> {
        let mut slots = Vec::new();
        for (_, versions) in self.retained {
            slots.extend(versions.into_iter().map(|v| v.slot));
        }
        for &slot in &slots {
            arena.release(slot);
        }
        slots
    }
}

fn gone(id: BranchId) -> LimboError {
    LimboError::InternalError(format!("branch {} does not exist", id.0))
}

impl BranchStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(StoreInner {
                arena: None,
                next_id: 1,
                trunk: TrunkState {
                    lineage: Lineage::default(),
                    written: HashMap::new(),
                },
                branches: HashMap::new(),
            }),
            trunk_children: AtomicUsize::new(0),
        }
    }

    pub(crate) fn trunk_has_children(&self) -> bool {
        self.trunk_children.load(Ordering::Acquire) > 0
    }

    /// Whether any branch state exists at all, including one kept alive only by a live child.
    /// Paths that rewrite the trunk without passing through `add_dirty` refuse while this holds.
    pub(crate) fn has_branches(&self) -> bool {
        !self.inner.lock().branches.is_empty()
    }

    /// Fork a child of the trunk. The caller must hold the trunk's WAL write lock: a trunk write
    /// transaction in flight across the fork would commit pages whose copy decision was taken for
    /// the previous epoch, and the new child would see them.
    pub(crate) fn fork_trunk(&self, schema: Arc<Schema>, page_size: usize) -> Result<BranchId> {
        let mut inner = self.inner.lock();
        match &inner.arena {
            None => inner.arena = Some(Arena::new(page_size)),
            Some(arena) if arena.page_size() != page_size => {
                return Err(LimboError::InternalError(format!(
                    "branch arena holds {}-byte pages but the database now uses {page_size}",
                    arena.page_size()
                )));
            }
            Some(_) => {}
        }
        let id = BranchId(inner.next_id);
        inner.next_id += 1;
        let f = inner.trunk.lineage.epoch;
        inner.trunk.lineage.epoch += 1;
        inner.trunk.lineage.children.insert(f, id);
        inner.branches.insert(id, BranchState::new(BranchId::TRUNK, f, schema));
        self.trunk_children.fetch_add(1, Ordering::AcqRel);
        Ok(id)
    }

    /// Fork a child of a branch. Refused while the parent has a write transaction in progress, for
    /// the same reason a trunk fork takes the WAL write lock.
    pub(crate) fn fork_branch(&self, parent: BranchId) -> Result<BranchId> {
        let mut inner = self.inner.lock();
        let id = BranchId(inner.next_id);
        let st = inner.branches.get_mut(&parent).ok_or_else(|| gone(parent))?;
        if st.writer {
            return Err(LimboError::Busy);
        }
        let f = st.lineage.epoch;
        st.lineage.epoch += 1;
        st.lineage.children.insert(f, id);
        let schema = st.schema.clone();
        inner.next_id += 1;
        inner.branches.insert(id, BranchState::new(parent, f, schema));
        Ok(id)
    }

    /// Mark the branch open for a connection and return its committed schema. One connection per
    /// branch: two would each hold a private page cache of the same page space, and nothing would
    /// tell one that the other had committed — a silently stale read, so it is refused.
    pub(crate) fn open(&self, id: BranchId) -> Result<Arc<Schema>> {
        let mut inner = self.inner.lock();
        let st = inner.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if st.open {
            return Err(LimboError::InvalidArgument(format!(
                "branch {} already has an open connection; a branch serves one connection at a \
                 time, because a second one's page cache would silently miss the first one's \
                 commits",
                id.0
            )));
        }
        st.open = true;
        Ok(st.schema.clone())
    }

    /// The connection on `id` has gone. Releases its write lock if a transaction was abandoned.
    pub(crate) fn close(&self, id: BranchId) {
        let mut inner = self.inner.lock();
        if let Some(st) = inner.branches.get_mut(&id) {
            st.open = false;
            st.writer = false;
        }
        self.collect(&mut inner, id);
    }

    /// The `Branch` handle has gone.
    pub(crate) fn release_handle(&self, id: BranchId) -> Reaped {
        let mut inner = self.inner.lock();
        let Some(st) = inner.branches.get_mut(&id) else {
            return Reaped {
                freed_pages: 0,
                deferred: false,
            };
        };
        st.handle = false;
        let freed_pages = self.collect(&mut inner, id);
        Reaped {
            freed_pages,
            deferred: inner.branches.contains_key(&id),
        }
    }

    pub(crate) fn begin_write(&self, id: BranchId) -> Result<()> {
        let mut inner = self.inner.lock();
        let st = inner.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if st.writer {
            return Err(LimboError::Busy);
        }
        st.writer = true;
        Ok(())
    }

    pub(crate) fn end_write(&self, id: BranchId) {
        if let Some(st) = self.inner.lock().branches.get_mut(&id) {
            st.writer = false;
        }
    }

    pub(crate) fn holds_writer(&self, id: BranchId) -> bool {
        self.inner
            .lock()
            .branches
            .get(&id)
            .is_some_and(|st| st.writer)
    }

    pub(crate) fn schema(&self, id: BranchId) -> Result<Arc<Schema>> {
        let inner = self.inner.lock();
        Ok(inner.branches.get(&id).ok_or_else(|| gone(id))?.schema.clone())
    }

    /// The copy decision for a branch's first write to `page` in a transaction. `pre_image` is the
    /// page as the branch sees it now — the version this write supersedes.
    pub(crate) fn first_write_branch(
        &self,
        id: BranchId,
        page: u32,
        pre_image: &[u8],
    ) -> Result<()> {
        let mut inner = self.inner.lock();
        let StoreInner {
            arena, branches, ..
        } = &mut *inner;
        let arena = arena.as_mut().expect("a branch exists, so the arena does");
        let st = branches.get_mut(&id).ok_or_else(|| gone(id))?;
        crate::turso_assert!(st.writer, "branch page written outside a write transaction");
        let epoch = st.lineage.epoch;
        match st.current.get(&page).copied() {
            None => {
                let slot = arena.alloc();
                arena.page_mut(slot).copy_from_slice(pre_image);
                st.current.insert(page, Owned { slot, born: epoch });
            }
            Some(owned) if owned.born == epoch => {}
            Some(owned) => {
                if st.lineage.has_child_in(owned.born, epoch) {
                    let slot = arena.alloc();
                    arena.page_mut(slot).copy_from_slice(pre_image);
                    st.lineage.retain(
                        page,
                        Retained {
                            born: owned.born,
                            died: epoch,
                            slot: owned.slot,
                        },
                    );
                    st.current.insert(page, Owned { slot, born: epoch });
                } else {
                    st.current.insert(
                        page,
                        Owned {
                            slot: owned.slot,
                            born: epoch,
                        },
                    );
                }
            }
        }
        Ok(())
    }

    /// The copy decision for the trunk's first write to `page` in a transaction: if a live child
    /// can still see the version about to be overwritten, keep a copy of it for that child.
    pub(crate) fn first_write_trunk(&self, page: u32, pre_image: &[u8]) {
        let mut inner = self.inner.lock();
        let StoreInner { arena, trunk, .. } = &mut *inner;
        let epoch = trunk.lineage.epoch;
        let born = trunk.written.get(&page).copied().unwrap_or(0);
        if born >= epoch {
            return;
        }
        if trunk.lineage.has_child_in(born, epoch) {
            let arena = arena.as_mut().expect("the trunk has a child, so the arena exists");
            let slot = arena.alloc();
            arena.page_mut(slot).copy_from_slice(pre_image);
            trunk.lineage.retain(
                page,
                Retained {
                    born,
                    died: epoch,
                    slot,
                },
            );
        }
        trunk.written.insert(page, epoch);
    }

    /// Commit a branch's dirty pages into the slots their copy decisions allocated.
    pub(crate) fn commit_pages(&self, id: BranchId, pages: &[PageRef]) -> Result<()> {
        let mut inner = self.inner.lock();
        let StoreInner {
            arena, branches, ..
        } = &mut *inner;
        let st = branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if pages.is_empty() {
            return Ok(());
        }
        let arena = arena.as_mut().expect("a branch exists, so the arena does");
        for page in pages {
            let no = page.get().id as u32;
            let owned = st.current.get(&no).copied().ok_or_else(|| {
                LimboError::InternalError(format!(
                    "branch {} committed page {no} with no copy decision behind it",
                    id.0
                ))
            })?;
            crate::turso_assert!(
                owned.born == st.lineage.epoch,
                "a committed branch page was decided in an earlier epoch"
            );
            arena
                .page_mut(owned.slot)
                .copy_from_slice(page.get_contents().as_slice());
        }
        Ok(())
    }

    pub(crate) fn set_schema(&self, id: BranchId, schema: Arc<Schema>) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.branches.get_mut(&id).ok_or_else(|| gone(id))?.schema = schema;
        Ok(())
    }

    /// Fill `out` with `page` as branch `id` sees it, if that version lives in the arena. `false`
    /// means the branch sees the trunk's current version, which the caller reads through the
    /// ordinary WAL / database-file path.
    pub(crate) fn resolve_into(&self, id: BranchId, page: u32, out: &mut [u8]) -> Result<bool> {
        let inner = self.inner.lock();
        let Some(slot) = inner.resolve(id, page)? else {
            return Ok(false);
        };
        out.copy_from_slice(
            inner
                .arena
                .as_ref()
                .expect("a slot resolved, so the arena exists")
                .page(slot),
        );
        Ok(true)
    }

    pub(crate) fn stats(&self) -> BranchStats {
        let inner = self.inner.lock();
        BranchStats {
            live_branches: inner.branches.len(),
            arena_slots_in_use: inner.arena.as_ref().map_or(0, |a| a.in_use()),
            arena_slots_free: inner.arena.as_ref().map_or(0, |a| a.free_count()),
        }
    }

    pub(crate) fn owned_slots(&self, id: BranchId) -> Vec<u32> {
        let inner = self.inner.lock();
        let Some(st) = inner.branches.get(&id) else {
            return Vec::new();
        };
        let mut slots: Vec<u32> = st.current.values().map(|o| o.slot).collect();
        for versions in st.lineage.retained.values() {
            slots.extend(versions.iter().map(|v| v.slot));
        }
        slots
    }

    pub(crate) fn slots_in_use(&self) -> Vec<u32> {
        self.inner
            .lock()
            .arena
            .as_ref()
            .map_or_else(Vec::new, |a| a.slots_in_use())
    }

    pub(crate) fn slot_is_free(&self, slot: u32) -> bool {
        self.inner
            .lock()
            .arena
            .as_ref()
            .is_some_and(|a| a.is_free(slot))
    }

    /// Free `id` if nothing can reach it any more, then its parent if that freed the parent's last
    /// reason to exist. Returns the number of arena pages released.
    fn collect(&self, inner: &mut StoreInner, mut id: BranchId) -> usize {
        let mut freed = 0;
        loop {
            let Some(st) = inner.branches.get(&id) else {
                return freed;
            };
            if st.handle || st.open || !st.lineage.children.is_empty() {
                return freed;
            }
            let st = inner.branches.remove(&id).expect("just looked it up");
            let arena = inner.arena.as_mut().expect("a branch existed, so the arena does");
            for owned in st.current.values() {
                arena.release(owned.slot);
                freed += 1;
            }
            freed += st.lineage.release_all(arena).len();
            if st.parent.is_trunk() {
                freed += inner.trunk.lineage.child_gone(st.fork_epoch, arena);
                self.trunk_children.fetch_sub(1, Ordering::AcqRel);
                return freed;
            }
            let parent = inner
                .branches
                .get_mut(&st.parent)
                .expect("a live branch's parent is kept while the branch lives");
            freed += parent.lineage.child_gone(st.fork_epoch, arena);
            id = st.parent;
        }
    }
}

impl StoreInner {
    fn resolve(&self, id: BranchId, page: u32) -> Result<Option<Slot>> {
        let mut node = id;
        // A branch sees all of its own versions; its ancestors only as of the fork.
        let mut at = u64::MAX;
        loop {
            if node.is_trunk() {
                if let Some(slot) = self.trunk.lineage.retained_at(page, at) {
                    return Ok(Some(slot));
                }
                let born = self.trunk.written.get(&page).copied().unwrap_or(0);
                if born > at {
                    // The trunk overwrote this page after the fork and nothing was retained: the
                    // ordinary read path would return the NEW version. Refuse rather than serve it.
                    return Err(LimboError::Corrupt(format!(
                        "branch {} would read trunk page {page} written after its fork; the \
                         pre-image was not retained",
                        id.0
                    )));
                }
                return Ok(None);
            }
            let st = self.branches.get(&node).ok_or_else(|| gone(node))?;
            if let Some(owned) = st.current.get(&page) {
                if owned.born <= at {
                    return Ok(Some(owned.slot));
                }
            }
            if let Some(slot) = st.lineage.retained_at(page, at) {
                return Ok(Some(slot));
            }
            at = st.fork_epoch;
            node = st.parent;
        }
    }
}

impl BranchState {
    fn new(parent: BranchId, fork_epoch: u64, schema: Arc<Schema>) -> Self {
        Self {
            parent,
            fork_epoch,
            lineage: Lineage::default(),
            current: HashMap::new(),
            schema,
            handle: true,
            open: false,
            writer: false,
        }
    }
}
