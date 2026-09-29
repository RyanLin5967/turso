//! Per-branch page spaces, and the rule that decides which version of a page a branch sees.
//!
//! # The model
//!
//! Every node in the branch tree — the trunk and each branch — carries an `epoch` that its own
//! forks advance: a child forked from a node records the node's epoch at that moment as its
//! `fork_epoch`, and the node's epoch then increments. A child's own epochs START one past its
//! `fork_epoch` (epoch inheritance), so along any line of descent the epochs are nested: every
//! value an ancestor gave a version its descendant can read is below every value the descendant
//! uses. That order is what lets a splice (see "Reclamation") move versions between a node and its
//! child without rewriting a single epoch, and it needs no store-wide clock. A version of a page
//! that a node wrote in epoch `born` is visible to that node's children forked at any epoch
//! `>= born`, until the node overwrites it in epoch `died`; after that it is visible only to
//! children forked in `[born, died)`. A branch therefore sees, for each page:
//!
//! 1. its own current version, if it has written the page; else
//! 2. its parent's version at the branch's `fork_epoch` — the parent's current version if it was
//!    born at or before that epoch, else the parent's RETAINED version covering it; else
//! 3. the same question one level up, down to the trunk, whose current version lives in the WAL
//!    and the database file and is read by the ordinary pager path.
//!
//! # Resolution without the walk — the page map
//!
//! Steps 2 and 3 are answered for every level between a branch and the trunk at once, and frozen,
//! at the moment the branch forks: nothing a branch sees through its ancestors can change after its
//! fork (an ancestor's later write retains the version the branch saw, in the same slot). So each
//! branch carries `inherited`, a persistent [`PageMap`] of every arena page it sees through its
//! ancestors, which is its parent's `view` at the fork: the parent's own `inherited` plus the
//! parent's current pages, kept up to date by the parent's writes once it has forked a child. A
//! fork clones the parent's `view` in O(1) and a write path-copies O(log P) trie nodes, so a
//! lookup costs the same at depth 1000 as at depth 1. The first fork builds the `view` from the
//! current versions born after `inherited` was taken only (`inherited_at`): a splice's moved-in
//! versions are already in `inherited`. A page no branch in the chain wrote is the
//! trunk's, as of `trunk_at`, the fork epoch at which the branch's ancestry leaves the trunk.
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
//! live siblings `lo` and `hi`: `born > lo` and `died <= hi`. The versions are indexed both by
//! `born` and by `died` — ZFS's deadlists, which key a dead block by the interval that killed it
//! and split it by birth — so that each side of that query is a range, not a scan:
//!
//! * with no older live sibling (the oldest child, which is whom uniform-TTL lease expiry reaps),
//!   the garbage is exactly the versions with `died` in `(f, hi]`;
//! * with no younger one (the newest child), exactly those with `born` in `(lo, f]`;
//! * with both, each range also holds survivors, and the two are walked in lockstep until the
//!   shorter one ends (see [`Lineage::garbage`]).
//!
//! A branch whose handle has been dropped but that still has an open connection, or two or more
//! kept children, is kept (its versions are still read through); it is freed the moment the last of
//! those goes, and freeing it may in turn free its parent.
//!
//! # Splicing a zombie out — the chain collapse
//!
//! A branch whose handle has gone, with no open connection and exactly ONE kept child `c`, is
//! spliced out instead of kept: `c` takes its place under its parent, at the same fork epoch, and
//! inherits the versions it was reading through it. Without this, every branch an agent forked from
//! and then dropped stayed resident for as long as any descendant lived, so a workload that forks
//! from its newest branch and expires its oldest kept every branch it ever created (lane r11-ever).
//! It is the chain collapse of QEMU's `block-stream` / `block-commit` and of ZFS's `zfs promote`
//! (whose `zfs destroy -d` is the deferral this replaces):
//!
//! 1. the zombie's versions born after `c`'s fork are invisible to `c` and are freed (a range of
//!    `current_by_born`);
//! 2. each of its retained versions holds `c`'s fork epoch (`c` is its only child), so it is what `c`
//!    reads of that page and becomes the zombie's current version of it;
//! 3. what remains is exactly what `c` reads through `inherited`. It is merged into `c`'s own
//!    versions by iterating the SMALLER of the two `current` maps and probing the larger (union by
//!    size; `block-stream` moves the base into the top, `block-commit` the top into the base), and
//!    the merged map is moved into `c` by swap. A zombie version of a page `c` has written is kept as
//!    a retained version of `c`, `[born, c's first own version)`, if a child of `c` forked inside
//!    that range, and freed otherwise. Epochs are inherited (see "The model"): every zombie version
//!    `c` reads was born at or before `c`'s fork epoch, and `c`'s own epochs start above it, so every
//!    zombie version is born before every epoch of `c` and no key changes.
//!
//! After every store call, every zombie has at least two kept children, so the kept states number at
//! most twice the live ones.
//!
//! # What this does not do
//!
//! * One `Mutex` guards every branch. Correct, and a known wall under concurrent writers on
//!   different branches; the benchmark this lane ships is single-threaded and says so.
//! * The persistent page maps are an index over slots the lineages own; they own nothing. A
//!   branch's `inherited` names only slots its ancestors keep for it (see "Resolution without the
//!   walk"), so dropping a map never frees a page and keeping one never pins a page. One exception,
//!   harmless by construction: after a splice, a child's `inherited` can still name a zombie slot
//!   that the splice freed because the child had overwritten that page. Every reader consults the
//!   branch's `current` before its `inherited` (`resolve`, `fork_branch`'s view), so such an entry
//!   is never read; code that walks `inherited` alone must skip pages present in `current`.
//!
//! # Per-page version order (the fat node)
//!
//! Within one node, one page's retained versions have non-empty, pairwise disjoint `[born, died)`
//! ranges: the trunk retains `[written, epoch)` and then sets `written = epoch`, and a branch
//! retains `[owned.born, epoch)` and re-bears its current version at `epoch`. So `born` is unique
//! per (node, page), and the version a child forked at `f` sees is the one with the greatest
//! `born <= f`, provided `f < died`. The versions are kept in a map ordered by `born`, which makes
//! that lookup a predecessor search and a release a removal by key — Driscoll, Sarnak, Sleator and
//! Tarjan's fat node (JCSS 1989) with a search tree over its version stamps. [`Lineage::retain`]
//! refuses a version that would break the disjointness the search relies on.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;

use super::arena::{Arena, Slot};
use super::page_map::PageMap;
use super::table::BranchTable;
use super::{BranchId, BranchResident, BranchStats, BranchWork, Reaped};
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
    /// Branch states by id: a slot map, so churn leaves no tombstones and never rehashes (F8,
    /// see [`super::table`]).
    branches: BranchTable<BranchState>,
    /// Observation only; see [`BranchWork`].
    work: BranchWork,
}

#[derive(Default)]
struct Lineage {
    /// The epoch this node's next write is born in: one past its latest fork epoch, or before it
    /// has forked, one past the fork epoch it was created with (a splice later lowers `fork_epoch`
    /// to its zombie parent's and leaves this alone).
    epoch: u64,
    /// Live children by fork epoch. Fork epochs are unique within a parent.
    children: BTreeMap<u64, BranchId>,
    /// Superseded versions kept because a live child forked while they were current, per page and
    /// ordered by `born` (see "Per-page version order" above).
    retained: HashMap<u32, BTreeMap<u64, Retained>>,
    /// The same versions as `(born, page, died)`, for the reclamation range query by birth.
    by_born: BTreeSet<(u64, u32, u64)>,
    /// The same versions as `(died, page, born)`, for the reclamation range query by death.
    by_died: BTreeSet<(u64, u32, u64)>,
}

/// No page has this number (SQLite's largest is `u32::MAX - 1`; [`Lineage::retain`] refuses it), so
/// `(e, NO_PAGE, u64::MAX)` sorts after every index entry whose first field is `e`.
const NO_PAGE: u32 = u32::MAX;

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
    /// The branch's current version of every page it has written, or read through a spliced-out
    /// ancestor (see "Splicing a zombie out").
    current: HashMap<u32, Owned>,
    /// `current` as `(born, page)`, so the versions born after a given epoch are a range.
    current_by_born: BTreeSet<(u64, u32)>,
    /// The branch's committed schema. Shared with the parent at fork (an `Arc` clone), replaced by
    /// a committed DDL on the branch.
    schema: Arc<Schema>,
    /// The `Branch` handle is alive.
    handle: bool,
    /// A connection is open on this branch.
    open: bool,
    /// A write transaction on this branch is in progress.
    writer: bool,
    /// The fork epoch at which this branch's ancestry leaves the trunk: its own fork epoch if its
    /// parent is the trunk, else its parent's `trunk_at`.
    trunk_at: u64,
    /// Every arena page this branch sees through its ancestors: its parent's `view` at the fork.
    inherited: PageMap,
    /// The fork epoch at which `inherited` was taken. A splice changes `fork_epoch` and leaves this
    /// and `inherited` alone. Every current version born at or below it is one a splice moved in
    /// from a zombie, which `inherited` already names with the same slot (the zombie's view at this
    /// epoch held the version the child read through it); every other current version is the
    /// branch's own, born above it (epoch inheritance). So the view build needs only those above.
    inherited_at: u64,
    /// `inherited` plus this branch's current pages, for its children to inherit. Built at the
    /// branch's first fork and kept current by its writes from then on; `None` until it forks.
    view: Option<PageMap>,
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
        let versions = self.retained.entry(page).or_default();
        // A splice can retain a version OLDER than the ones already kept, so both neighbours are
        // checked: the predecessor must die by `v.born`, the successor be born at `v.died` or later.
        crate::turso_assert!(
            v.born < v.died
                && versions
                    .range(..=v.born)
                    .next_back()
                    .is_none_or(|(_, prev)| prev.died <= v.born && prev.born != v.born)
                && versions
                    .range(v.born..)
                    .next()
                    .is_none_or(|(_, next)| v.died <= next.born),
            "a retained version overlaps another of the same page; the born-ordered lookup would \
             return the wrong one"
        );
        crate::turso_assert!(page != NO_PAGE, "page number u32::MAX is the index sentinel");
        versions.insert(v.born, v);
        self.by_born.insert((v.born, page, v.died));
        self.by_died.insert((v.died, page, v.born));
    }

    /// The retained version of `page` visible to a child forked at `f`: the born-predecessor of
    /// `f`, if it was still current at `f`. `examined` counts the versions compared against `f` —
    /// at most one; the O(log V) descent that finds it is not counted.
    fn retained_at(&self, page: u32, f: u64, examined: &mut u64) -> Option<Slot> {
        let (_, v) = self.retained.get(&page)?.range(..=f).next_back()?;
        *examined += 1;
        (f < v.died).then_some(v.slot)
    }

    /// Detach the child forked at `f` and release every retained version that only it could see.
    fn child_gone(&mut self, f: u64, arena: &mut Arena, work: &mut BranchWork) -> usize {
        let removed = self.children.remove(&f);
        crate::turso_assert!(removed.is_some(), "detached a child the parent does not list");
        let lo = self.children.range(..f).next_back().map(|(&e, _)| e);
        let hi = self.children.range(f..).next().map(|(&e, _)| e);
        let dead = self.garbage(f, lo, hi, work);
        for &(born, page, died) in &dead {
            let versions = self.retained.get_mut(&page).expect("indexed version is listed");
            let v = versions.remove(&born).expect("indexed version is listed");
            work.gc_examined += 1;
            if versions.is_empty() {
                self.retained.remove(&page);
            }
            let indexed = self.by_born.remove(&(born, page, died))
                && self.by_died.remove(&(died, page, born));
            crate::turso_assert!(indexed, "a released version was missing from an index");
            arena.release(v.slot);
        }
        dead.len()
    }

    /// The versions that held `f` and no other live child, as `(born, page, died)`, once `f` has
    /// left `children`; `lo` and `hi` are its former neighbours there.
    ///
    /// Every retained version holds at least one live child's fork epoch: it is retained only if
    /// one forked inside it, and this function hands it back the moment the last one goes. So a
    /// version with `born > lo` and `died <= hi` held `f` and nothing else, and one holding `f`
    /// that reaches back to `lo` or on to `hi` is still needed. That makes the garbage
    /// `{born > lo, died <= hi}`, and each index answers one side of it:
    ///
    /// * `lo` absent: `died` in `(f, hi]`. Every such version was born at or before `f` (it holds
    ///   a live child, and there is none below `f`), so every entry the range yields is garbage.
    ///   This is the oldest child — the victim of uniform-TTL expiry — and the cost is what it frees.
    /// * `hi` absent: `born` in `(lo, f]`, every entry garbage by the same argument.
    /// * both: garbage lies in both ranges, and each also yields survivors (versions reaching past
    ///   `hi`, or back past `lo`). The ranges are walked in lockstep and the first to end is
    ///   filtered, so the walk costs twice the SMALLER range, never the larger.
    ///
    /// `gc_range_entries` counts every entry either range yields.
    fn garbage(
        &self,
        f: u64,
        lo: Option<u64>,
        hi: Option<u64>,
        work: &mut BranchWork,
    ) -> Vec<(u64, u32, u64)> {
        // `born` in (lo, f] and `died` in (f, hi], as bounds on the two indexes' first field.
        let after = |e: u64| (e, NO_PAGE, u64::MAX);
        let born_from = lo.map_or(Bound::Unbounded, |lo| Bound::Excluded(after(lo)));
        let born_to = Bound::Included(after(f));
        let died_from = Bound::Excluded(after(f));
        let died_to = hi.map_or(Bound::Unbounded, |hi| Bound::Included(after(hi)));
        let only_f = |born: u64, died: u64| {
            lo.is_none_or(|lo| born > lo)
                && born <= f
                && f < died
                && hi.is_none_or(|hi| died <= hi)
        };
        let mut by_born = self.by_born.range((born_from, born_to));
        let mut by_died = self
            .by_died
            .range((died_from, died_to))
            .map(|&(died, page, born)| (born, page, died));
        let mut seen: [Vec<(u64, u32, u64)>; 2] = Default::default();
        // Which ranges to walk: the one side's own range when a neighbour is missing, else both.
        let walk = match (lo, hi) {
            (None, _) => [false, true],
            (Some(_), None) => [true, false],
            (Some(_), Some(_)) => [true, true],
        };
        let finished = 'walk: loop {
            for side in 0..2 {
                if !walk[side] {
                    continue;
                }
                let next = if side == 0 {
                    by_born.next().copied()
                } else {
                    by_died.next()
                };
                match next {
                    Some(v) => {
                        work.gc_range_entries += 1;
                        seen[side].push(v);
                    }
                    None => break 'walk side,
                }
            }
        };
        let mut dead = std::mem::take(&mut seen[finished]);
        dead.retain(|&(born, _, died)| only_f(born, died));
        dead
    }

    fn release_all(self, arena: &mut Arena) -> Vec<Slot> {
        let mut slots = Vec::new();
        for (_, versions) in self.retained {
            slots.extend(versions.into_values().map(|v| v.slot));
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
                branches: BranchTable::new(),
                work: BranchWork::default(),
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
        let id = inner.branches.vacant_id();
        inner.next_id += 1;
        let f = inner.trunk.lineage.epoch;
        inner.trunk.lineage.epoch += 1;
        inner.trunk.lineage.children.insert(f, id);
        inner.branches.insert(
            id,
            BranchState::new(BranchId::TRUNK, f, schema, f, PageMap::default()),
        );
        self.trunk_children.fetch_add(1, Ordering::AcqRel);
        Ok(id)
    }

    /// Fork a child of a branch. Refused while the parent has a write transaction in progress, for
    /// the same reason a trunk fork takes the WAL write lock.
    pub(crate) fn fork_branch(&self, parent: BranchId) -> Result<BranchId> {
        let mut inner = self.inner.lock();
        let id = inner.branches.vacant_id();
        let st = inner.branches.get_mut(&parent).ok_or_else(|| gone(parent))?;
        if st.writer {
            return Err(LimboError::Busy);
        }
        let f = st.lineage.epoch;
        st.lineage.epoch += 1;
        st.lineage.children.insert(f, id);
        let schema = st.schema.clone();
        // Only the current versions born after `inherited` was taken (see `inherited_at`): a
        // splice's moved-in versions are in `inherited` already, and re-inserting them made the
        // first fork after each splice cost every page the dead levels above had moved in, which is
        // Theta(d^2) over a chain that writes a new page, forks and releases its parent at each of
        // d levels (r11-adversarial's chainw; r11-ever amendment 17).
        let (current, by_born, inherited, at) =
            (&st.current, &st.current_by_born, &st.inherited, st.inherited_at);
        let mut built = 0u64;
        let view = st
            .view
            .get_or_insert_with(|| {
                let mut view = inherited.clone();
                for &(_, page) in by_born.range((at.saturating_add(1), 0)..) {
                    view.insert(page, current[&page].slot);
                    built += 1;
                }
                view
            })
            .clone();
        let trunk_at = st.trunk_at;
        inner.work.view_build_entries += built;
        inner.next_id += 1;
        inner
            .branches
            .insert(id, BranchState::new(parent, f, schema, trunk_at, view));
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
        let (freed_pages, spliced) = self.collect(&mut inner, id);
        Reaped {
            freed_pages,
            // A spliced branch's versions live on in its child, freed when that child goes.
            deferred: spliced || inner.branches.contains_key(&id),
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
                st.set_current(page, Owned { slot, born: epoch });
                if let Some(view) = st.view.as_mut() {
                    view.insert(page, slot);
                }
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
                    st.set_current(page, Owned { slot, born: epoch });
                    if let Some(view) = st.view.as_mut() {
                        view.insert(page, slot);
                    }
                } else {
                    // No live child can see the current version: it is rewritten in its own slot,
                    // which is already the one `view` names.
                    st.set_current(
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
        let mut inner = self.inner.lock();
        let (mut levels, mut examined) = (0, 0);
        let resolved = inner.resolve(id, page, &mut levels, &mut examined);
        inner.work.resolve_calls += 1;
        inner.work.resolve_levels += levels;
        inner.work.resolve_retained_examined += examined;
        let Some(slot) = resolved? else {
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
            work: inner.work,
        }
    }

    /// `(states, capacity)` of the branch table: capacity is the slots allocated, which grows by a
    /// chunk at a time (F8, [`super::table`]; before F8 it was `HashMap::capacity`, which fell by one
    /// per tombstone and jumped back at a rehash) and, since r12-f9-shrink amendment 5, falls when
    /// the top chunks of slots are freed. O(1).
    pub(crate) fn table_shape(&self) -> (usize, usize) {
        let inner = self.inner.lock();
        (inner.branches.len(), inner.branches.capacity())
    }

    /// Every resident structure's size, by a full scan under the lock. Observation only.
    pub(crate) fn resident(&self) -> BranchResident {
        let inner = self.inner.lock();
        let mut r = BranchResident {
            states: inner.branches.len(),
            table_capacity: inner.branches.capacity(),
            next_id: inner.next_id,
            trunk_epoch: inner.trunk.lineage.epoch,
            trunk_children: inner.trunk.lineage.children.len(),
            trunk_retained_versions: inner.trunk.lineage.by_born.len(),
            trunk_retained_pages: inner.trunk.lineage.retained.len(),
            trunk_written_pages: inner.trunk.written.len(),
            ..Default::default()
        };
        let mut index_mismatch = inner.trunk.lineage.by_died.len() != r.trunk_retained_versions
            || inner.trunk.lineage.retained.values().map(|v| v.len()).sum::<usize>()
                != r.trunk_retained_versions;
        let mut nodes = std::collections::HashSet::new();
        for st in inner.branches.values() {
            r.zombies += usize::from(!st.handle);
            r.open += usize::from(st.open);
            r.branch_children += st.lineage.children.len();
            r.branch_retained_versions += st.lineage.by_born.len();
            index_mismatch |= st.lineage.by_died.len() != st.lineage.by_born.len()
                || st.lineage.retained.values().map(|v| v.len()).sum::<usize>()
                    != st.lineage.by_born.len();
            r.branch_current_pages += st.current.len();
            index_mismatch |= st.current_by_born.len() != st.current.len();
            st.inherited.count_nodes(&mut nodes);
            if let Some(view) = &st.view {
                r.views += 1;
                view.count_nodes(&mut nodes);
            }
        }
        r.page_map_nodes = nodes.len();
        // Slots some reader (a branch with a handle or an open connection) can read: its own
        // current versions, what its `inherited` map names for pages it has not written, and the
        // trunk's retained version at its `trunk_at` for pages neither holds.
        let mut visible = std::collections::HashSet::new();
        let mut unused = 0u64;
        for st in inner.branches.values().filter(|st| st.handle || st.open) {
            visible.extend(st.current.values().map(|o| o.slot));
            st.inherited.for_each(|page, slot| {
                if !st.current.contains_key(&page) {
                    visible.insert(slot);
                }
            });
            for &page in inner.trunk.lineage.retained.keys() {
                if st.current.contains_key(&page) || st.inherited.get(page).is_some() {
                    continue;
                }
                if let Some(slot) = inner.trunk.lineage.retained_at(page, st.trunk_at, &mut unused) {
                    visible.insert(slot);
                }
            }
        }
        r.visible_slots = visible.len();
        // Where the unreadable slots sit (observation only): a zombie's current versions born after
        // its newest kept child's fork (no child can ever read them), its other current versions,
        // retained versions of zombies and of live branches, and the trunk's retained versions.
        for st in inner.branches.values() {
            let zombie = !st.handle && !st.open;
            let newest = st.lineage.children.keys().next_back().copied();
            for o in st.current.values() {
                if visible.contains(&o.slot) {
                    continue;
                }
                if !zombie {
                    r.waste_live_current += 1;
                } else if newest.is_none_or(|f| o.born > f) {
                    r.waste_zombie_current_after_last_fork += 1;
                } else {
                    r.waste_zombie_current_shadowed += 1;
                }
            }
            for versions in st.lineage.retained.values() {
                for v in versions.values() {
                    if !visible.contains(&v.slot) {
                        if zombie {
                            r.waste_zombie_retained += 1;
                        } else {
                            r.waste_live_retained += 1;
                        }
                    }
                }
            }
        }
        for versions in inner.trunk.lineage.retained.values() {
            r.waste_trunk_retained += versions.values().filter(|v| !visible.contains(&v.slot)).count();
        }
        r.index_mismatch = index_mismatch;
        if let Some(arena) = &inner.arena {
            let (high_water, free_capacity, free_bits_words, chunks) = arena.shape();
            r.arena_high_water = high_water;
            r.arena_in_use = arena.in_use();
            r.arena_free_list_len = arena.free_count();
            r.arena_free_list_capacity = free_capacity;
            r.arena_free_bits_words = free_bits_words;
            r.arena_chunks = chunks;
            let b = arena.bytes();
            r.arena_chunks_mapped = b.chunks_mapped;
            r.arena_resident_bytes = b.resident;
            r.arena_meta_bytes = b.meta;
            r.arena_purges = b.purges;
            r.arena_reuses = b.reuses;
            r.arena_chunk_maps = b.chunk_maps;
            r.arena_chunk_unmaps = b.chunk_unmaps;
        }
        (
            r.table_value_bytes,
            r.table_index_bytes,
            r.table_entry_bytes,
            r.table_value_chunks,
        ) = inner.branches.bytes();
        r
    }

    pub(crate) fn owned_slots(&self, id: BranchId) -> Vec<u32> {
        let inner = self.inner.lock();
        let Some(st) = inner.branches.get(&id) else {
            return Vec::new();
        };
        let mut slots: Vec<u32> = st.current.values().map(|o| o.slot).collect();
        for versions in st.lineage.retained.values() {
            slots.extend(versions.values().map(|v| v.slot));
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
    /// reason to exist; a state left with no handle, no connection and exactly one kept child is
    /// spliced out instead (see "Splicing a zombie out"). Returns the number of arena pages released,
    /// and whether `id` itself was spliced.
    fn collect(&self, inner: &mut StoreInner, mut id: BranchId) -> (usize, bool) {
        let mut freed = 0;
        let first = id;
        loop {
            let Some(st) = inner.branches.get(&id) else {
                return (freed, false);
            };
            if st.handle || st.open {
                return (freed, false);
            }
            match st.lineage.children.len() {
                0 => {}
                1 => {
                    freed += inner.splice(id);
                    return (freed, id == first);
                }
                _ => return (freed, false),
            }
            let st = inner.branches.remove(&id).expect("just looked it up");
            let StoreInner {
                arena,
                trunk,
                branches,
                work,
                ..
            } = &mut *inner;
            let arena = arena.as_mut().expect("a branch existed, so the arena does");
            for owned in st.current.values() {
                arena.release(owned.slot);
                freed += 1;
            }
            freed += st.lineage.release_all(arena).len();
            if st.parent.is_trunk() {
                freed += trunk.lineage.child_gone(st.fork_epoch, arena, work);
                self.trunk_children.fetch_sub(1, Ordering::AcqRel);
                return (freed, false);
            }
            let parent = branches
                .get_mut(&st.parent)
                .expect("a live branch's parent is kept while the branch lives");
            freed += parent.lineage.child_gone(st.fork_epoch, arena, work);
            id = st.parent;
        }
    }
}

impl StoreInner {
    /// Splice the zombie `zid` (no handle, no connection, exactly one kept child) out of the tree:
    /// its child takes its place and inherits the versions it reads through it (see "Splicing a
    /// zombie out"). Returns the arena pages freed.
    fn splice(&mut self, zid: BranchId) -> usize {
        let StoreInner {
            arena,
            trunk,
            branches,
            work,
            ..
        } = self;
        let arena = arena.as_mut().expect("a branch exists, so the arena does");
        let mut z = branches.remove(&zid).expect("the zombie is kept");
        crate::turso_assert!(
            !z.handle && !z.open && z.lineage.children.len() == 1,
            "spliced a branch that is not a zombie with one child"
        );
        let (&f, &cid) = z.lineage.children.iter().next().expect("one child");
        let mut freed = 0;
        let mut visited = 0u64;
        // 1. Versions born after the child's fork: the child cannot see them.
        let later: Vec<(u64, u32)> = z
            .current_by_born
            .range((f + 1, 0)..)
            .copied()
            .collect();
        for (born, page) in later {
            let owned = z.current.remove(&page).expect("indexed current version");
            z.current_by_born.remove(&(born, page));
            arena.release(owned.slot);
            freed += 1;
            visited += 1;
        }
        // 2. Every retained version holds `f` (the child is the only one left), at most one per
        //    page: it is what the child reads of that page.
        let retained = std::mem::take(&mut z.lineage.retained);
        z.lineage.by_born.clear();
        z.lineage.by_died.clear();
        for (page, versions) in retained {
            crate::turso_assert!(versions.len() == 1, "two retained versions hold one fork epoch");
            let v = versions.into_values().next().expect("one version");
            crate::turso_assert!(
                v.born <= f && f < v.died && !z.current.contains_key(&page),
                "a zombie's retained version does not hold its only child's fork epoch"
            );
            z.set_current(
                page,
                Owned {
                    slot: v.slot,
                    born: v.born,
                },
            );
            visited += 1;
        }
        // 3. Merge what the child reads through the zombie into the child's own versions: iterate
        //    the smaller map, probe the larger, keep the merged map in the child.
        let c = branches.get_mut(&cid).expect("the zombie's child is kept");
        crate::turso_assert!(c.fork_epoch == f && c.parent == zid, "child link mismatch");
        let BranchState {
            current: c_current,
            current_by_born: c_by_born,
            lineage: c_lineage,
            ..
        } = c;
        // Keep a zombie version of a page the child has written for the child's children that
        // forked before the child's first own version of it, else free it.
        let mut shadowed = |page: u32, zo: Owned, c_first: u64, lineage: &mut Lineage| -> usize {
            if lineage.has_child_in(zo.born, c_first) {
                lineage.retain(
                    page,
                    Retained {
                        born: zo.born,
                        died: c_first,
                        slot: zo.slot,
                    },
                );
                0
            } else {
                arena.release(zo.slot);
                1
            }
        };
        let first_own = |lineage: &Lineage, page: u32, co: Owned| -> u64 {
            lineage
                .retained
                .get(&page)
                .and_then(|v| v.first_key_value().map(|(&b, _)| b))
                .map_or(co.born, |b| b.min(co.born))
        };
        let commit = z.current.len() > c_current.len();
        if !commit {
            // Stream: the zombie's versions into the child.
            for (page, zo) in std::mem::take(&mut z.current) {
                visited += 1;
                match c_current.get(&page).copied() {
                    None => {
                        c_current.insert(page, zo);
                        c_by_born.insert((zo.born, page));
                    }
                    Some(co) => {
                        let c_first = first_own(c_lineage, page, co);
                        freed += shadowed(page, zo, c_first, c_lineage);
                    }
                }
            }
            z.current_by_born.clear();
        } else {
            // Commit: the child's versions into the zombie's map, which then becomes the child's.
            for (&page, &co) in c_current.iter() {
                visited += 1;
                if let Some(zo) = z.current.remove(&page) {
                    z.current_by_born.remove(&(zo.born, page));
                    let c_first = first_own(c_lineage, page, co);
                    freed += shadowed(page, zo, c_first, c_lineage);
                }
            }
            for (page, co) in c_current.drain() {
                z.current.insert(page, co);
            }
            // One insert per child entry: `BTreeSet::append` rebuilds from both sides, O(larger),
            // which would make every commit-direction splice cost the zombie's whole map.
            for entry in std::mem::take(c_by_born) {
                z.current_by_born.insert(entry);
            }
            std::mem::swap(c_current, &mut z.current);
            std::mem::swap(c_by_born, &mut z.current_by_born);
        }
        // 4. The child takes the zombie's place.
        c.parent = z.parent;
        c.fork_epoch = z.fork_epoch;
        let siblings = if z.parent.is_trunk() {
            &mut trunk.lineage.children
        } else {
            &mut branches
                .get_mut(&z.parent)
                .expect("a kept branch's parent is kept")
                .lineage
                .children
        };
        let was = siblings.insert(z.fork_epoch, cid);
        crate::turso_assert!(was == Some(zid), "the zombie's parent did not list it");
        work.splices += 1;
        work.splice_commits += u64::from(commit);
        work.splice_entries += visited;
        freed
    }

    /// `levels` counts the nodes consulted — the branch (its own pages and its `inherited` map),
    /// then the trunk if neither holds the page — and `examined` the retained versions compared.
    fn resolve(
        &self,
        id: BranchId,
        page: u32,
        levels: &mut u64,
        examined: &mut u64,
    ) -> Result<Option<Slot>> {
        *levels += 1;
        let st = self.branches.get(&id).ok_or_else(|| gone(id))?;
        // A branch sees all of its own versions; its ancestors' as of its fork, which `inherited`
        // froze then.
        if let Some(owned) = st.current.get(&page) {
            return Ok(Some(owned.slot));
        }
        if let Some(slot) = st.inherited.get(page) {
            return Ok(Some(slot));
        }
        *levels += 1;
        let at = st.trunk_at;
        if let Some(slot) = self.trunk.lineage.retained_at(page, at, examined) {
            return Ok(Some(slot));
        }
        let born = self.trunk.written.get(&page).copied().unwrap_or(0);
        if born > at {
            // The trunk overwrote this page after the fork and nothing was retained: the ordinary
            // read path would return the NEW version. Refuse rather than serve it.
            return Err(LimboError::Corrupt(format!(
                "branch {} would read trunk page {page} written after its fork; the pre-image was \
                 not retained",
                id.0
            )));
        }
        Ok(None)
    }
}

impl BranchState {
    /// Set `page`'s current version, keeping `current_by_born` in step.
    fn set_current(&mut self, page: u32, owned: Owned) {
        if let Some(old) = self.current.insert(page, owned) {
            self.current_by_born.remove(&(old.born, page));
        }
        self.current_by_born.insert((owned.born, page));
    }

    fn new(
        parent: BranchId,
        fork_epoch: u64,
        schema: Arc<Schema>,
        trunk_at: u64,
        inherited: PageMap,
    ) -> Self {
        Self {
            parent,
            fork_epoch,
            // Epoch inheritance (see "The model"): the child's epochs start above its fork epoch, so
            // every version it reads through its parent is born before any epoch it uses. A splice
            // relies on that order; starting at 0 would break it.
            lineage: Lineage {
                epoch: fork_epoch + 1,
                ..Lineage::default()
            },
            current: HashMap::new(),
            current_by_born: BTreeSet::new(),
            schema,
            handle: true,
            open: false,
            writer: false,
            trunk_at,
            inherited,
            inherited_at: fork_epoch,
            view: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    const PAGE: usize = 64;
    const PAGES: u32 = 6;

    struct Rng(u64);
    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    fn image(generation: u64) -> Vec<u8> {
        generation.to_le_bytes().repeat(PAGE / 8)
    }

    /// The splice invariant and the bookkeeping behind it, read under the lock after a step: no
    /// kept zombie has fewer than two kept children, every index agrees with the map it indexes,
    /// every arena slot in use is owned by exactly one current or retained version, and every
    /// child link is mirrored by its parent.
    fn check_invariants(store: &BranchStore, what: &str) {
        let inner = store.inner.lock();
        let mut owned: Vec<Slot> = Vec::new();
        let lineage_slots = |l: &Lineage, owned: &mut Vec<Slot>| {
            assert_eq!(l.by_born.len(), l.by_died.len(), "{what}: born/died index sizes");
            let n: usize = l.retained.values().map(|v| v.len()).sum();
            assert_eq!(n, l.by_born.len(), "{what}: retained vs index");
            for v in l.retained.values() {
                owned.extend(v.values().map(|r| r.slot));
            }
        };
        lineage_slots(&inner.trunk.lineage, &mut owned);
        for (id, st) in inner.branches.iter() {
            if !st.handle && !st.open {
                assert!(
                    st.lineage.children.len() >= 2,
                    "{what}: zombie {} kept with {} children",
                    id.0,
                    st.lineage.children.len()
                );
            }
            assert_eq!(st.current.len(), st.current_by_born.len(), "{what}: current index");
            for (&page, o) in &st.current {
                assert!(st.current_by_born.contains(&(o.born, page)), "{what}: index entry");
                owned.push(o.slot);
            }
            lineage_slots(&st.lineage, &mut owned);
            for (&f, &child) in &st.lineage.children {
                let c = inner.branches.get(&child).expect("a listed child is kept");
                assert!(c.parent == id && c.fork_epoch == f, "{what}: child link");
            }
            let listed = if st.parent.is_trunk() {
                inner.trunk.lineage.children.get(&st.fork_epoch)
            } else {
                inner.branches[&st.parent].lineage.children.get(&st.fork_epoch)
            };
            assert_eq!(listed, Some(&id), "{what}: parent does not list branch {}", id.0);
        }
        let n = owned.len();
        owned.sort_unstable();
        owned.dedup();
        assert_eq!(owned.len(), n, "{what}: an arena slot has two owners");
        let in_use = inner.arena.as_ref().map_or(0, |a| a.in_use());
        assert_eq!(in_use, n, "{what}: arena slots in use that no version owns");
    }

    /// The trunk's retained-version index against a brute-force model, through the store's own
    /// entry points (`fork_trunk`, `first_write_trunk`, `release_handle`, `resolve_into`), and the
    /// garbage query's cost against its contract: a reap with no older live sibling, or no younger
    /// one, visits exactly the versions it frees, and one with both visits 2·|B| index entries if
    /// |B| <= |D| and 2·|D| + 1 otherwise, where B and D are the versions born in `(lo, f]` and
    /// dying in `(f, hi]`.
    ///
    /// The trunk rewrites a handful of pages, several per epoch, so versions share `born` and `died`
    /// across pages; children are reaped oldest-first, newest-first and at random, so the garbage
    /// query runs with no older sibling, with no younger one, and with both. After every step, every
    /// live child must read, for every page, the trunk's page as of its fork — from its retained
    /// version or, when there is none, from the trunk's current one — and the arena must hold
    /// exactly the versions whose `[born, died)` still contains a live child's fork epoch.
    #[test]
    fn retained_versions_match_a_model_under_forks_rewrites_and_reaps_in_every_order() {
        for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
            run(seed);
        }
    }

    fn run(seed: u64) {
        let store = BranchStore::new();
        let mut rng = Rng(seed);
        // The trunk's current generation of each page, and each live child's view at its fork.
        let mut current: HashMap<u32, u64> = (0..PAGES).map(|p| (p, 0)).collect();
        let mut live: Vec<(BranchId, u64, HashMap<u32, u64>)> = Vec::new();
        // Every version ever retained: (page, born, died), with `born`/`died` in fork epochs.
        let mut history: Vec<(u32, u64, u64)> = Vec::new();
        let mut written: HashMap<u32, u64> = HashMap::new();
        let mut epoch = 0u64;
        let mut generation = 0u64;
        let (mut freed_oldest, mut freed_newest, mut freed_middle) = (0, 0, 0);
        for step in 0..1500 {
            match rng.below(10) {
                0..=2 if live.len() < 40 => {
                    let id = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
                    live.push((id, epoch, current.clone()));
                    epoch += 1;
                }
                0..=5 => {
                    // One trunk transaction: first writes of up to three pages in this epoch.
                    for _ in 0..=rng.below(3) {
                        let page = rng.below(PAGES as u64) as u32;
                        if store.trunk_has_children() {
                            let born = written.get(&page).copied().unwrap_or(0);
                            if born < epoch {
                                if live.iter().any(|&(_, f, _)| born <= f && f < epoch) {
                                    history.push((page, born, epoch));
                                }
                                written.insert(page, epoch);
                            }
                            store.first_write_trunk(page, &image(current[&page]));
                        }
                        generation += 1;
                        current.insert(page, generation);
                    }
                }
                _ if !live.is_empty() => {
                    let at = match rng.below(3) {
                        0 => 0,
                        1 => live.len() - 1,
                        _ => rng.below(live.len() as u64) as usize,
                    };
                    let lo = at.checked_sub(1).map(|i| live[i].1);
                    let hi = live.get(at + 1).map(|c| c.1);
                    let (id, f, _) = live.remove(at);
                    let b = history
                        .iter()
                        .filter(|&&(_, born, _)| lo.is_none_or(|lo| born > lo) && born <= f)
                        .count() as u64;
                    let d = history
                        .iter()
                        .filter(|&&(_, _, died)| f < died && hi.is_none_or(|hi| died <= hi))
                        .count() as u64;
                    let before = store.stats();
                    let reaped = store.release_handle(id);
                    let after = store.stats();
                    assert!(!reaped.deferred, "seed {seed:#x} step {step}");
                    assert_eq!(
                        before.arena_slots_in_use - after.arena_slots_in_use,
                        reaped.freed_pages,
                        "seed {seed:#x} step {step}: the reap's report disagrees with the arena"
                    );
                    let visited = after.work.gc_range_entries - before.work.gc_range_entries;
                    let contract = match (lo, hi) {
                        (None, _) | (_, None) => reaped.freed_pages as u64,
                        _ if b <= d => 2 * b,
                        _ => 2 * d + 1,
                    };
                    assert_eq!(
                        visited, contract,
                        "seed {seed:#x} step {step}: reaping the child forked at {f} (lo {lo:?}, \
                         hi {hi:?}, |B| {b}, |D| {d}) visited {visited} index entries"
                    );
                    if reaped.freed_pages > 0 {
                        match at {
                            0 => freed_oldest += 1,
                            _ if at == live.len() => freed_newest += 1,
                            _ => freed_middle += 1,
                        }
                    }
                }
                _ => {}
            }
            let alive: HashSet<(u32, u64, u64)> = history
                .iter()
                .copied()
                .filter(|&(_, born, died)| live.iter().any(|&(_, f, _)| born <= f && f < died))
                .collect();
            assert_eq!(
                store.stats().arena_slots_in_use,
                alive.len(),
                "seed {seed:#x} step {step}: the arena holds a version no live child can see, or \
                 lost one a live child can"
            );
            // A version no live child can see never becomes visible again.
            history.retain(|v| alive.contains(v));
            let mut buf = vec![0u8; PAGE];
            for (id, f, view) in &live {
                for page in 0..PAGES {
                    let in_arena = store.resolve_into(*id, page, &mut buf).unwrap();
                    let got = if in_arena {
                        u64::from_le_bytes(buf[..8].try_into().unwrap())
                    } else {
                        current[&page]
                    };
                    assert_eq!(
                        got, view[&page],
                        "seed {seed:#x} step {step}: child forked at {f} read the wrong page {page}"
                    );
                }
            }
        }
        // The walk above must have freed versions through all three shapes of the garbage query;
        // otherwise a green run says nothing about the shape it skipped.
        assert!(
            freed_oldest > 0 && freed_newest > 0 && freed_middle > 0,
            "seed {seed:#x}: reaps that freed versions: oldest {freed_oldest}, newest \
             {freed_newest}, middle {freed_middle}"
        );
        for (id, _, _) in live {
            store.release_handle(id);
        }
        assert_eq!(store.stats().arena_slots_in_use, 0, "seed {seed:#x}: versions leaked");
    }

    /// A committed branch page holding `image(generation)`, as the pager hands it to
    /// `commit_pages`.
    fn page_with(page: u32, generation: u64) -> PageRef {
        let p = Arc::new(crate::storage::pager::Page::new(i64::from(page)));
        let buffer = Arc::new(crate::Buffer::new_temporary(PAGE));
        buffer.as_mut_slice().copy_from_slice(&image(generation));
        p.get().buffer = Some(buffer);
        p
    }

    /// Branch TREES — forks from the trunk and from branches, deep chains, writes on the trunk and
    /// on branches before and after they fork, deferred reaps of branches with live children —
    /// against a model in which each branch is a plain copy of its parent's pages at its fork.
    /// Every live branch must read, for every page, what the model says, through
    /// `resolve_into`, the path the pager uses.
    #[test]
    fn every_branch_of_a_random_tree_reads_its_parent_as_of_its_fork() {
        for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
            run_tree(seed);
        }
    }

    struct Node {
        id: BranchId,
        sees: HashMap<u32, u64>,
        handle: bool,
        depth: usize,
        /// This branch has forked a child, so its writes must reach the view its children inherit.
        forked: bool,
    }

    fn run_tree(seed: u64) {
        let store = BranchStore::new();
        let mut rng = Rng(seed);
        let mut trunk: HashMap<u32, u64> = (0..PAGES).map(|p| (p, 0)).collect();
        let mut nodes: Vec<Node> = Vec::new();
        let mut generation = 0u64;
        let (mut deferred, mut max_depth, mut wrote_after_fork) = (0, 0, 0);
        for step in 0..2500 {
            let live: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].handle).collect();
            match rng.below(12) {
                0 if live.len() < 60 => {
                    let id = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
                    nodes.push(Node {
                        id,
                        sees: trunk.clone(),
                        handle: true,
                        depth: 1,
                        forked: false,
                    });
                }
                1..=3 if !live.is_empty() && live.len() < 60 => {
                    // Half the time the newest live branch, so that chains grow deep.
                    let parent = if rng.below(2) == 0 {
                        *live.last().unwrap()
                    } else {
                        live[rng.below(live.len() as u64) as usize]
                    };
                    let id = store.fork_branch(nodes[parent].id).unwrap();
                    let (sees, depth) = (nodes[parent].sees.clone(), nodes[parent].depth + 1);
                    nodes[parent].forked = true;
                    max_depth = max_depth.max(depth);
                    nodes.push(Node {
                        id,
                        sees,
                        handle: true,
                        depth,
                        forked: false,
                    });
                }
                4..=5 => {
                    let page = rng.below(u64::from(PAGES)) as u32;
                    if store.trunk_has_children() {
                        store.first_write_trunk(page, &image(trunk[&page]));
                    }
                    generation += 1;
                    trunk.insert(page, generation);
                }
                6..=9 if !live.is_empty() => {
                    let v = live[rng.below(live.len() as u64) as usize];
                    if nodes[v].forked {
                        wrote_after_fork += 1;
                    }
                    let id = nodes[v].id;
                    store.begin_write(id).unwrap();
                    let mut committed = Vec::new();
                    for _ in 0..=rng.below(2) {
                        let page = rng.below(u64::from(PAGES)) as u32;
                        if committed.iter().any(|p: &PageRef| p.get().id == page as usize) {
                            continue;
                        }
                        store
                            .first_write_branch(id, page, &image(nodes[v].sees[&page]))
                            .unwrap();
                        generation += 1;
                        committed.push(page_with(page, generation));
                        nodes[v].sees.insert(page, generation);
                    }
                    store.commit_pages(id, &committed).unwrap();
                    store.end_write(id);
                }
                _ if !live.is_empty() => {
                    let v = live[rng.below(live.len() as u64) as usize];
                    nodes[v].handle = false;
                    if store.release_handle(nodes[v].id).deferred {
                        deferred += 1;
                    }
                }
                _ => {}
            }
            check_invariants(&store, &format!("seed {seed:#x} step {step}"));
            let mut buf = vec![0u8; PAGE];
            for n in nodes.iter().filter(|n| n.handle) {
                for page in 0..PAGES {
                    let got = if store.resolve_into(n.id, page, &mut buf).unwrap() {
                        u64::from_le_bytes(buf[..8].try_into().unwrap())
                    } else {
                        trunk[&page]
                    };
                    assert_eq!(
                        got,
                        n.sees[&page],
                        "seed {seed:#x} step {step}: branch {} at depth {} read the wrong page {page}",
                        n.id.0,
                        n.depth
                    );
                }
            }
        }
        // The shapes the page maps exist for must have occurred, or a green run says nothing.
        assert!(
            max_depth >= 10 && deferred > 0 && wrote_after_fork > 0,
            "seed {seed:#x}: max depth {max_depth}, deferred reaps {deferred}, writes by a branch \
             after its first fork {wrote_after_fork}"
        );
        let work = store.stats().work;
        assert!(
            work.splices > 0 && work.splice_commits > 0 && work.splice_commits < work.splices,
            "seed {seed:#x}: splices {} of which commits {}: both merge directions must run",
            work.splices,
            work.splice_commits
        );
        for n in nodes.iter().filter(|n| n.handle) {
            store.release_handle(n.id);
        }
        assert_eq!(store.stats().live_branches, 0, "seed {seed:#x}: branches leaked");
        assert_eq!(store.stats().arena_slots_in_use, 0, "seed {seed:#x}: slots leaked");
    }

    /// The two agent shapes that made the store keep every branch it ever created (lane r11-ever),
    /// against a model in which each branch is a plain copy of its parent's pages at its fork:
    /// `newest` forks every branch from the newest live one and reaps the oldest (without the
    /// splice, nothing is ever freed); `random` forks from a random live branch, which then keeps
    /// writing, and reaps a random one. Every live branch must read what the model says for every
    /// page after every step, the invariants must hold, the kept states must stay within twice the
    /// live ones, and teardown must free everything.
    #[test]
    fn churned_branch_trees_keep_at_most_twice_their_live_branches() {
        for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
            for newest in [true, false] {
                run_churn(seed, newest);
            }
        }
    }

    fn run_churn(seed: u64, newest: bool) {
        const LIVE: usize = 24;
        let store = BranchStore::new();
        let mut rng = Rng(seed);
        let mut trunk: HashMap<u32, u64> = (0..PAGES).map(|p| (p, 0)).collect();
        // (id, what it sees), oldest first.
        let mut live: Vec<(BranchId, HashMap<u32, u64>)> = Vec::new();
        let mut generation = 0u64;
        let write = |store: &BranchStore,
                     id: BranchId,
                     sees: &mut HashMap<u32, u64>,
                     rng: &mut Rng,
                     generation: &mut u64| {
            store.begin_write(id).unwrap();
            let mut committed = Vec::new();
            for _ in 0..=rng.below(2) {
                let page = rng.below(u64::from(PAGES)) as u32;
                if committed.iter().any(|p: &PageRef| p.get().id == page as usize) {
                    continue;
                }
                store.first_write_branch(id, page, &image(sees[&page])).unwrap();
                *generation += 1;
                committed.push(page_with(page, *generation));
                sees.insert(page, *generation);
            }
            store.commit_pages(id, &committed).unwrap();
            store.end_write(id);
        };
        for step in 0..3000 {
            let parent = match live.len() {
                0 => None,
                n if newest => Some(n - 1),
                n => Some(rng.below(n as u64) as usize),
            };
            let (id, mut sees) = match parent {
                None => (
                    store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap(),
                    trunk.clone(),
                ),
                Some(i) => (store.fork_branch(live[i].0).unwrap(), live[i].1.clone()),
            };
            write(&store, id, &mut sees, &mut rng, &mut generation);
            if let Some(i) = parent.filter(|_| !newest || rng.below(2) == 0) {
                // The parent keeps working after the fork.
                let (pid, mut psees) = (live[i].0, std::mem::take(&mut live[i].1));
                write(&store, pid, &mut psees, &mut rng, &mut generation);
                live[i].1 = psees;
            }
            if rng.below(4) == 0 {
                let page = rng.below(u64::from(PAGES)) as u32;
                store.first_write_trunk(page, &image(trunk[&page]));
                generation += 1;
                trunk.insert(page, generation);
            }
            live.push((id, sees));
            if live.len() > LIVE {
                let at = if newest { 0 } else { rng.below(live.len() as u64 - 1) as usize };
                let (victim, _) = live.remove(at);
                store.release_handle(victim);
            }
            let what = format!("seed {seed:#x} newest {newest} step {step}");
            check_invariants(&store, &what);
            let states = store.stats().live_branches;
            assert!(states < 2 * LIVE, "{what}: {states} states kept for {} live", live.len());
            if newest {
                assert_eq!(states, live.len(), "{what}: a chain kept a zombie");
            }
            let mut buf = vec![0u8; PAGE];
            for (id, sees) in &live {
                for page in 0..PAGES {
                    let got = if store.resolve_into(*id, page, &mut buf).unwrap() {
                        u64::from_le_bytes(buf[..8].try_into().unwrap())
                    } else {
                        trunk[&page]
                    };
                    assert_eq!(got, sees[&page], "{what}: branch {} read page {page}", id.0);
                }
            }
        }
        let work = store.stats().work;
        assert!(work.splices > 1000, "seed {seed:#x} newest {newest}: {} splices", work.splices);
        for (id, _) in live {
            store.release_handle(id);
        }
        assert_eq!(store.stats().live_branches, 0, "seed {seed:#x}: branches leaked");
        assert_eq!(store.stats().arena_slots_in_use, 0, "seed {seed:#x}: slots leaked");
    }

    /// A zombie kept only by an open connection is spliced the moment the connection closes, while
    /// its only child is in the middle of a write transaction; the child's in-flight decisions
    /// survive the merge (its commit lands where they point), a page the zombie wrote and the child
    /// had overwritten is freed, and a page the child reads through the zombie is kept for the
    /// child's own later children.
    #[test]
    fn a_zombie_closed_while_its_child_is_mid_write_is_spliced_without_disturbing_the_write() {
        let store = BranchStore::new();
        let z = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
        let mut generation = 100u64;
        // z writes pages 0, 1 and 2.
        store.begin_write(z).unwrap();
        let mut committed = Vec::new();
        for page in 0..3u32 {
            store.first_write_branch(z, page, &image(0)).unwrap();
            generation += 1;
            committed.push(page_with(page, generation));
        }
        store.commit_pages(z, &committed).unwrap();
        store.end_write(z);
        let z_gen: Vec<u64> = (0..3).map(|p| 101 + p).collect();
        let c = store.fork_branch(z).unwrap();
        // c overwrites page 1 and commits; then opens a transaction on page 2 and leaves it open.
        store.begin_write(c).unwrap();
        store.first_write_branch(c, 1, &image(z_gen[1])).unwrap();
        store.commit_pages(c, &[page_with(1, 201)]).unwrap();
        store.end_write(c);
        store.open(z).unwrap();
        let reaped = store.release_handle(z);
        assert!(reaped.deferred && reaped.freed_pages == 0, "an open zombie was freed: {reaped:?}");
        assert_eq!(store.stats().live_branches, 2);
        // Reads through the zombie while its connection holds it (r11-ever-refute coverage caveat
        // iii): its child reads the page it inherits, the zombie its own page the child overwrote.
        let mut buf = vec![0u8; PAGE];
        assert!(store.resolve_into(c, 0, &mut buf).unwrap());
        assert_eq!(u64::from_le_bytes(buf[..8].try_into().unwrap()), z_gen[0], "c misread through the held zombie");
        assert!(store.resolve_into(z, 1, &mut buf).unwrap());
        assert_eq!(u64::from_le_bytes(buf[..8].try_into().unwrap()), z_gen[1], "the held zombie lost its own page");
        store.begin_write(c).unwrap();
        store.first_write_branch(c, 2, &image(z_gen[2])).unwrap();
        let before = store.stats();
        store.close(z);
        let after = store.stats();
        check_invariants(&store, "after the splice");
        assert_eq!(after.live_branches, 1, "the closed zombie was not spliced");
        assert_eq!(after.work.splices, before.work.splices + 1);
        assert_eq!(
            before.arena_slots_in_use - after.arena_slots_in_use,
            2,
            "z's versions of pages 1 and 2 are freed: c overwrote 1 and holds its own copy of 2 \
             (the in-flight write's pre-image), and c has no child to read either"
        );
        store.commit_pages(c, &[page_with(2, 202)]).unwrap();
        store.end_write(c);
        let read = |id: BranchId, page: u32| -> Option<u64> {
            let mut buf = vec![0u8; PAGE];
            store
                .resolve_into(id, page, &mut buf)
                .unwrap()
                .then(|| u64::from_le_bytes(buf[..8].try_into().unwrap()))
        };
        assert_eq!(read(c, 0), Some(z_gen[0]), "c lost the page it read through z");
        assert_eq!(read(c, 1), Some(201));
        assert_eq!(read(c, 2), Some(202), "the in-flight write did not land");
        // A child of c forked now, then c rewrites page 0: the child keeps z's version.
        let d = store.fork_branch(c).unwrap();
        store.begin_write(c).unwrap();
        store.first_write_branch(c, 0, &image(z_gen[0])).unwrap();
        store.commit_pages(c, &[page_with(0, 300)]).unwrap();
        store.end_write(c);
        check_invariants(&store, "after c rewrote an absorbed page");
        assert_eq!(read(d, 0), Some(z_gen[0]), "d lost z's version of page 0");
        assert_eq!(read(c, 0), Some(300));
        store.release_handle(c);
        store.release_handle(d);
        assert_eq!(store.stats().live_branches, 0);
        assert_eq!(store.stats().arena_slots_in_use, 0, "slots leaked");
    }

    /// r11-adversarial's chainw (r11-ever amendment 17): at each of D levels the newest branch
    /// writes a page no level wrote before, forks, and is released, so it is spliced into the new
    /// child, which inherits every page the dead levels above had moved into it. Each level's first
    /// fork builds its view: from all of `current`, j entries at level j and D(D+1)/2 in all; from
    /// the versions born after `inherited_at`, the level's own page, D in all. The tip and a child
    /// forked from it read every level's page, and teardown frees everything.
    #[test]
    fn a_chain_that_writes_forks_and_releases_builds_each_view_from_its_own_pages() {
        const D: u32 = 64;
        let store = BranchStore::new();
        let mut prev = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
        let before = store.stats().work;
        for j in 0..D {
            store.begin_write(prev).unwrap();
            store.first_write_branch(prev, j, &image(0)).unwrap();
            store.commit_pages(prev, &[page_with(j, 1000 + u64::from(j))]).unwrap();
            store.end_write(prev);
            let next = store.fork_branch(prev).unwrap();
            let reaped = store.release_handle(prev);
            assert!(reaped.deferred, "level {j}: the released level was not kept for its child");
            prev = next;
        }
        check_invariants(&store, "after the chain");
        let work = store.stats().work;
        assert_eq!(work.splices - before.splices, u64::from(D), "premise: every level was spliced");
        assert_eq!(
            work.view_build_entries - before.view_build_entries,
            u64::from(D),
            "a level's first fork built its view from more than its own page"
        );
        let child = store.fork_branch(prev).unwrap();
        let mut buf = vec![0u8; PAGE];
        for j in 0..D {
            for id in [prev, child] {
                assert!(store.resolve_into(id, j, &mut buf).unwrap(), "branch {} lost page {j}", id.0);
                let got = u64::from_le_bytes(buf[..8].try_into().unwrap());
                assert_eq!(got, 1000 + u64::from(j), "branch {} misread page {j}", id.0);
            }
        }
        store.release_handle(child);
        store.release_handle(prev);
        assert_eq!(store.stats().live_branches, 0);
        assert_eq!(store.stats().arena_slots_in_use, 0, "slots leaked");
    }
}
