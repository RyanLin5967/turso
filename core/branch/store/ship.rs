//! Moving a branch store to another place: a full send, an incremental send between two points
//! in time, a receiver that applies either to a replica, and a single-branch export.
//!
//! # What a receiver needs, and what it can derive
//!
//! A receiver needs every live arena slot's content, every state's `parent`, `fork_epoch`,
//! `lineage.epoch`, `handle`, `current` and `retained`, and the trunk's `lineage` and `written`.
//! Everything else is derived on arrival: `children` from each state's parent and fork epoch,
//! `trunk_at` from the parent chain, the two retained-version indexes from `retained`, and
//! `inherited` — the persistent page map — from the parent's versions: a child forked at `f` sees,
//! per page its parent owned at `f`, the version whose `[born, died)` holds `f`, and that version is
//! still held because the child pins it. So the stream carries no page map at all.
//!
//! # The ship view: snapshots in O(1), changes by birth (ZFS/Btrfs)
//!
//! A tracked store keeps, beside its working structures, a SHIP VIEW made of persistent
//! [`BirthTree`]s: `states` (a state per id, each with its own `current` tree by page and
//! `retained` tree by (page, born); a freed state becomes a hole that keeps its birth), `slots` (each
//! live slot's page, owner, births and bytes), `trunk_retained` and `written`. Every mutation updates
//! the view under the store mutex, path-copying O(height) nodes and raising their births to its
//! sequence number. Page bytes are immutable `Arc`s (see `arena`), so the view shares them.
//!
//! * **F-S1, the send.** [`BranchStore::send`] takes the mutex only to clone the view's roots —
//!   one operation, whatever the store holds — and serialises from the clone with no lock.
//! * **F-S2, the incremental.** "What changed after `base`" is a walk of each tree that skips every
//!   node whose birth is `<= base`, as ZFS's send skips block pointers born before the from-txg.
//!
//! # The incremental stream (ZFS `send -i`, with this store's twist)
//!
//! A receiver at sequence number `base` holds every slot and trunk page as of `base`. The sender
//! ships, in the order the receiver applies them:
//!
//! 1. `DEAD`: every state that existed at `base` and is now a hole born after it, children before
//!    parents (descending id). The receiver runs the same garbage query the sender ran and frees the
//!    same versions. A version retained before `base` can never be pinned by a child forked after it
//!    (the child's fork epoch is past the version's `died`), so applying deaths to the `base` state
//!    before anything new arrives frees exactly what the sender freed, in any order.
//! 2. `STATE`: every state whose view changed since `base`, in id order so a parent precedes its
//!    children — a new one whole, an old one as its changed `current` entries and its new retained
//!    versions; then `TRUNK_META`. A changed entry can drop a slot the receiver holds: the sender
//!    retained that version for a child and freed it when the child died, both inside the window,
//!    so no hole names it. The receiver releases every slot an entry stopped naming here, before any
//!    page record can claim that slot number again.
//! 3. `REF`: a retained TRUNK pre-image whose content the receiver already holds as its own trunk
//!    page. The trunk's pre-image is COPIED into a slot handed out when the trunk overwrites the
//!    page (`first_write_trunk`), so choosing pages by when the slot was handed out would ship old
//!    content again; choosing by when the content was born — ZFS's block birth txg — ships a
//!    reference, whose content hash the receiver checks.
//! 4. `TRUNK_PAGE`: trunk pages written since `base`.
//! 5. `SLOT`: every live slot whose content was born after `base`. With deltas on, each is a run-diff
//!    against its FORK BASE — the version its owner saw before writing it (a branch page: what the
//!    branch reads through its ancestry; a retained trunk version: the page's next version) — which
//!    the receiver holds or has just received: trunk versions go first, newest per page first, then
//!    states in id order, and a fork base always belongs to the trunk or to an ancestor.
//! 6. `END`.
//!
//! The other modes exist to be COUNTED against this one: a per-dataset stream that re-describes every
//! state and lists every live id for deletion (`zfs send -R -I` / `recv -F`), one that picks pages by
//! slot allocation, and full sends with and without flattened page maps.

use std::cell::Cell;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{Read, Write};
use std::time::Instant;

use super::{
    gone, Arena, BranchId, BranchState, BranchStore, Lineage, Owned, PageMap, Retained, Schema, Slot,
    StoreInner,
};
use crate::branch::birth_tree::{BirthTree, Item, TreeWork};
use crate::sync::atomic::Ordering;
use crate::sync::Arc;
use crate::{LimboError, Result};

/// A state as a receiver needs it, in the ship view.
#[derive(Clone)]
pub(crate) struct StateView {
    parent: u64,
    fork_epoch: u64,
    epoch: u64,
    handle: bool,
    schema_version: u32,
    born_seq: u64,
    trunk_at: u64,
    inherited: PageMap,
    /// page → current version.
    current: BirthTree<CurView>,
    /// `ret_key(page, born)` → retained version.
    retained: BirthTree<RetView>,
}

pub(crate) struct CurView {
    page: u32,
    slot: Slot,
    born: u64,
}

pub(crate) struct RetView {
    page: u32,
    born: u64,
    died: u64,
    slot: Slot,
}

#[derive(Clone, Copy)]
pub(crate) enum SlotOwner {
    Branch(u64),
    Trunk { born: u64, died: u64 },
}

pub(crate) struct SlotView {
    page: u32,
    owner: SlotOwner,
    alloc_seq: u64,
    content_seq: u64,
    data: std::sync::Arc<[u8]>,
}

pub(crate) struct TrunkVersion {
    page: u32,
    born: u64,
    died: u64,
    slot: Slot,
}

/// A trunk page's last write: the `written` epoch it left (if any) and when.
pub(crate) struct WrittenView {
    page: u32,
    epoch: Option<u64>,
}

/// The ship view. Cloning it is the O(1) snapshot a send serialises from.
#[derive(Clone, Default)]
pub(crate) struct ShipView {
    states: BirthTree<StateView>,
    slots: BirthTree<SlotView>,
    trunk_retained: BirthTree<TrunkVersion>,
    written: BirthTree<WrittenView>,
    trunk_epoch: u64,
    next_id: u64,
    seq: u64,
}

/// A retained version's key: its page, then its birth epoch.
fn ret_key(page: u32, born: u64) -> u64 {
    crate::turso_assert!(born < 1 << 32, "a node forked more than 2^32 times");
    (u64::from(page) << 32) | born
}

/// Shipping bookkeeping kept by a tracked store.
#[derive(Default)]
pub(crate) struct Track {
    /// One per store operation; a receiver's position is a value of it.
    pub(super) seq: u64,
    /// What shipping every operation's log record would cost (WAL shipping); observation only.
    pub(super) log_bytes: u64,
    view: ShipView,
    /// Work the view's upkeep did under the mutex, and the operations that did it.
    view_work: TreeWork,
    view_ops: u64,
    /// `(birth, id)` of every hole, so that holes every receiver has passed can be dropped.
    holes: BTreeSet<(u64, u64)>,
}

impl Track {
    /// The next store operation's sequence number, which the view's snapshot will carry.
    pub(super) fn advance(&mut self) -> u64 {
        self.seq += 1;
        self.view.seq = self.seq;
        self.seq
    }
}

/// What a snapshot for a send is: the view, and the page size its bytes have.
#[derive(Clone)]
pub struct ShipSnap {
    view: ShipView,
    page_size: usize,
}

impl ShipSnap {
    pub fn seq(&self) -> u64 {
        self.view.seq
    }
}

/// Upkeep of the ship view, called under the mutex after each mutation (no-ops when untracked).
impl StoreInner {
    /// Re-put state `id`'s scalars, keeping its current and retained trees; `bump` makes the change
    /// visible to incrementals (None: a derivable change, which keeps the state's birth).
    pub(super) fn ship_state(&mut self, id: BranchId, bump: Option<u64>) {
        let Some(st) = self.branches.get(&id) else {
            return;
        };
        let Some(t) = self.track.as_mut() else {
            return;
        };
        let old = t.view.states.get(id.0).and_then(|i| i.live().cloned().map(|v| (v, i.birth())));
        let (current, retained, birth) = match &old {
            Some((v, b)) => (v.current.clone(), v.retained.clone(), *b),
            None => (BirthTree::default(), BirthTree::default(), 0),
        };
        let view = StateView {
            parent: st.parent.0,
            fork_epoch: st.fork_epoch,
            epoch: st.lineage.epoch,
            handle: st.handle,
            schema_version: st.schema.schema_version,
            born_seq: st.born_seq,
            trunk_at: st.trunk_at,
            inherited: st.inherited.clone(),
            current,
            retained,
        };
        let item_birth = bump.unwrap_or(birth);
        let mut w = TreeWork::default();
        t.view
            .states
            .put(id.0, Item::Live { birth: item_birth, val: Arc::new(view) }, bump, &mut w);
        t.view_work.add(w);
        t.view_ops += 1;
    }

    /// State `id`'s current version of `page` changed at `seq`.
    pub(super) fn ship_current(&mut self, id: BranchId, page: u32, seq: u64) {
        let Some(owned) = self.branches.get(&id).and_then(|st| st.current.get(&page).copied()) else {
            return;
        };
        self.ship_edit(id, Some(seq), |v, w| {
            v.current.put(
                u64::from(page),
                Item::Live {
                    birth: seq,
                    val: Arc::new(CurView { page, slot: owned.slot, born: owned.born }),
                },
                Some(seq),
                w,
            )
        });
    }

    /// State `id` retained version `v` of `page` at `seq`.
    pub(super) fn ship_retained(&mut self, id: BranchId, page: u32, v: Retained, seq: u64) {
        self.ship_edit(id, Some(seq), |view, w| {
            view.retained.put(
                ret_key(page, v.born),
                Item::Live {
                    birth: seq,
                    val: Arc::new(RetView { page, born: v.born, died: v.died, slot: v.slot }),
                },
                Some(seq),
                w,
            )
        });
    }

    /// State `id`'s garbage query released its version of `page` born at `born` (derivable).
    fn ship_retained_gone(&mut self, id: BranchId, page: u32, born: u64) {
        self.ship_edit(id, None, |view, w| {
            view.retained.remove(ret_key(page, born), w);
        });
    }

    /// Edit state `id`'s view in place of the stored one: `bump` raises its birth, None keeps it.
    fn ship_edit(&mut self, id: BranchId, bump: Option<u64>, edit: impl FnOnce(&mut StateView, &mut TreeWork)) {
        let Some(t) = self.track.as_mut() else {
            return;
        };
        let Some((old, birth)) = t.view.states.get(id.0).and_then(|i| i.live().cloned().map(|v| (v, i.birth())))
        else {
            return;
        };
        let mut view = (*old).clone();
        let mut w = TreeWork::default();
        edit(&mut view, &mut w);
        t.view.states.put(
            id.0,
            Item::Live { birth: bump.unwrap_or(birth), val: Arc::new(view) },
            bump,
            &mut w,
        );
        t.view_work.add(w);
        t.view_ops += 1;
    }

    /// Slot `slot`'s bytes, owner or births changed at `seq` (its item birth is `seq`).
    pub(super) fn ship_slot(&mut self, slot: Slot, owner: SlotOwner, seq: u64) {
        let (Some(t), Some(arena)) = (self.track.as_mut(), self.arena.as_ref()) else {
            return;
        };
        let (alloc_seq, content_seq, page) = arena.stamps_of(slot).expect("a tracked arena stamps");
        let view = SlotView { page, owner, alloc_seq, content_seq, data: arena.page_arc(slot).clone() };
        let mut w = TreeWork::default();
        t.view
            .slots
            .put(u64::from(slot), Item::Live { birth: seq, val: Arc::new(view) }, Some(seq), &mut w);
        t.view_work.add(w);
        t.view_ops += 1;
    }

    /// Slot `slot` was released (derivable by the receiver).
    pub(super) fn ship_slot_gone(&mut self, slot: Slot) {
        if let Some(t) = self.track.as_mut() {
            let mut w = TreeWork::default();
            t.view.slots.remove(u64::from(slot), &mut w);
            t.view_work.add(w);
        }
    }

    /// State `id` (forked at `born_seq`) was freed at `seq`: a hole.
    pub(super) fn ship_state_gone(&mut self, id: BranchId, born_seq: u64, seq: u64) {
        if let Some(t) = self.track.as_mut() {
            let mut w = TreeWork::default();
            t.view.states.put(id.0, Item::Hole { birth: seq, born: born_seq }, Some(seq), &mut w);
            t.holes.insert((seq, id.0));
            t.view_work.add(w);
            t.view_ops += 1;
        }
    }

    /// A trunk page was written at `seq`: returns the sequence number of its previous write (the
    /// content birth of the pre-image being overwritten; 0 before tracking began).
    pub(super) fn ship_trunk_prev(&self, page: u32) -> u64 {
        self.track
            .as_ref()
            .and_then(|t| t.view.written.get(u64::from(page)).map(Item::birth))
            .unwrap_or(0)
    }

    pub(super) fn ship_written(&mut self, page: u32, seq: u64, page_size: usize) {
        let epoch = self.trunk.written.get(&page).copied();
        if let Some(t) = self.track.as_mut() {
            let mut w = TreeWork::default();
            t.view.written.put(
                u64::from(page),
                Item::Live { birth: seq, val: Arc::new(WrittenView { page, epoch }) },
                Some(seq),
                &mut w,
            );
            t.log_bytes += 24 + page_size as u64;
            t.view_work.add(w);
            t.view_ops += 1;
        }
    }

    pub(super) fn ship_trunk_retained(&mut self, page: u32, v: Retained, seq: u64) {
        if let Some(t) = self.track.as_mut() {
            let mut w = TreeWork::default();
            t.view.trunk_retained.put(
                ret_key(page, v.born),
                Item::Live {
                    birth: seq,
                    val: Arc::new(TrunkVersion { page, born: v.born, died: v.died, slot: v.slot }),
                },
                Some(seq),
                &mut w,
            );
            t.view_work.add(w);
            t.view_ops += 1;
        }
    }

    fn ship_trunk_retained_gone(&mut self, page: u32, born: u64) {
        if let Some(t) = self.track.as_mut() {
            let mut w = TreeWork::default();
            t.view.trunk_retained.remove(ret_key(page, born), &mut w);
            t.view_work.add(w);
        }
    }

    /// Trunk scalars (epoch, next id) changed.
    pub(super) fn ship_trunk_scalars(&mut self) {
        let (epoch, next_id) = (self.trunk.lineage.epoch, self.next_id);
        if let Some(t) = self.track.as_mut() {
            t.view.trunk_epoch = epoch;
            t.view.next_id = next_id;
        }
    }

    /// A released set of versions of state `owner` (or of the trunk, `owner == TRUNK`) and the
    /// slots of a freed state: all derivable by the receiver, so no birth moves.
    pub(super) fn ship_released(&mut self, owner: BranchId, released: &[(u32, Retained)]) {
        if self.track.is_none() {
            return;
        }
        for (page, v) in released {
            if owner.is_trunk() {
                self.ship_trunk_retained_gone(*page, v.born);
            } else {
                self.ship_retained_gone(owner, *page, v.born);
            }
            self.ship_slot_gone(v.slot);
        }
    }
}

/// A trunk database image: `page_size`-byte pages, page 1 first.
#[derive(Clone)]
pub struct TrunkImage {
    page_size: usize,
    bytes: Vec<u8>,
}

impl TrunkImage {
    pub fn new(page_size: usize, bytes: Vec<u8>) -> Self {
        assert!(bytes.len() % page_size == 0, "a trunk image is whole pages");
        Self { page_size, bytes }
    }

    pub fn empty(page_size: usize) -> Self {
        Self::new(page_size, Vec::new())
    }

    pub fn page_size(&self) -> usize {
        self.page_size
    }

    pub fn pages(&self) -> u32 {
        (self.bytes.len() / self.page_size) as u32
    }

    /// Page `pgno` (1-based).
    pub fn page(&self, pgno: u32) -> Option<&[u8]> {
        if pgno == 0 || pgno > self.pages() {
            return None;
        }
        let at = (pgno as usize - 1) * self.page_size;
        Some(&self.bytes[at..at + self.page_size])
    }

    fn resize(&mut self, pages: u32) {
        self.bytes.resize(pages as usize * self.page_size, 0);
    }

    fn set_page(&mut self, pgno: u32, data: &[u8]) {
        if pgno > self.pages() {
            self.resize(pgno);
        }
        let at = (pgno as usize - 1) * self.page_size;
        self.bytes[at..at + self.page_size].copy_from_slice(data);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendMode {
    /// Every slot once and the non-derivable metadata; the receiver derives the page maps.
    FullFix,
    /// As `FullFix`, plus every state's page map flattened (counted, never received).
    FullMaps,
    /// A walk of the ship view's trees from the base, pruned by birth (F-S2): holes as deaths,
    /// changed entries, content-birth pages and trunk pre-image references.
    IncrFix,
    /// A record for EVERY live state, the full live-id list for deletion, and the trunk's whole
    /// retained set: `zfs send -R -I` + `recv -F` (counted, never received).
    IncrRoot,
    /// As `IncrFix`, but pages chosen by slot allocation, so trunk pre-images ship as data
    /// (counted, never received).
    IncrAlloc,
}

impl SendMode {
    fn incremental(self) -> bool {
        matches!(self, SendMode::IncrFix | SendMode::IncrRoot | SendMode::IncrAlloc)
    }
}

/// A defect a fire-check plants in a stream, to prove the receiver's checks can fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Plant {
    None,
    /// Flip one byte of the first shipped slot payload.
    FlipPayload,
    /// Leave out the first tombstone.
    DropTombstone,
    /// Corrupt the content hash of the first trunk pre-image reference.
    BadRefHash,
}

/// What a send shipped, by record kind, and the work the sender did.
#[derive(Clone, Debug, Default)]
pub struct SendReport {
    pub base: u64,
    pub to: u64,
    pub total_bytes: u64,
    /// Bytes of page records (headers and payloads), of references, and of everything else.
    pub page_bytes: u64,
    pub ref_bytes: u64,
    pub meta_bytes: u64,
    pub slot_records: u64,
    pub trunk_page_records: u64,
    pub ref_records: u64,
    pub state_records: u64,
    pub dead_records: u64,
    pub current_entries: u64,
    pub retained_entries: u64,
    pub trunk_retained_entries: u64,
    pub written_entries: u64,
    pub live_list_bytes: u64,
    /// Flattened page-map bytes (`FullMaps` only): 8 per entry of `inherited` ∪ `current`.
    pub maps_bytes: u64,
    /// Page payload bytes shipped raw; the same payloads as run-diffs against the receiver's trunk
    /// page of the same number (raw where that is not smaller); and raw with in-stream duplicates
    /// replaced by 13-byte references.
    pub payload_raw: u64,
    pub payload_delta: u64,
    /// Payload bytes as this stream carries them (raw, or delta with its 2-byte length).
    pub payload_shipped: u64,
    /// Payloads as run-diffs against each slot's FORK BASE (the version its owner saw before
    /// writing it; a retained trunk version's next version), counted, not shipped; trunk pages
    /// count raw.
    pub payload_delta_fork: u64,
    pub payload_dedup: u64,
    pub dup_payloads: u64,
    /// Sender work: states, map/version entries, slots, and change-index entries visited.
    pub states_visited: u64,
    pub entries_visited: u64,
    pub slots_visited: u64,
    pub index_visited: u64,
    /// F-S2: tree nodes the send entered (a walk skips every node born at or before the base), and
    /// the items it looked at inside them.
    pub nodes_visited: u64,
    pub items_checked: u64,
    /// F-S1: operations the send performed while holding the store mutex (cloning the view's roots
    /// is one), and how long it held it.
    pub locked_ops: u64,
    pub locked_ns: u64,
}

/// Receiver work, counted in the receive code.
#[derive(Clone, Copy, Debug, Default)]
pub struct RecvWork {
    pub records: u64,
    pub slots_written: u64,
    pub slots_claimed: u64,
    pub refs: u64,
    pub trunk_pages: u64,
    pub deaths: u64,
    pub gc_freed: u64,
    pub states_new: u64,
    pub states_updated: u64,
    pub entries: u64,
    pub retained_inserted: u64,
    pub trie_inserts: u64,
    /// Slots a changed state's old `current` named and its new record does not.
    pub slots_released: u64,
    /// Slot payloads decoded against a fork base (another slot or a trunk page).
    pub fork_deltas: u64,
}

/// A canonical digest of a store's whole observable state (see [`BranchStore::digest`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Digest {
    pub hash: (u64, u64),
    pub states: u64,
    pub slots: u64,
    pub entries: u64,
}

/// Every content hash and every metadata entry of a store, for the incremental-minimum instrument.
#[derive(Default)]
pub struct ShipDump {
    /// Content hashes of every live slot and every trunk page.
    pub content: HashSet<u64>,
    pub state_headers: HashSet<u64>,
    pub current: HashSet<u64>,
    pub retained: HashSet<u64>,
    pub trunk_written: HashSet<u64>,
    pub trunk_retained: HashSet<u64>,
    pub states: HashSet<u64>,
}

/// Fixed-width metadata sizes: the minimum's unit costs.
pub const STATE_HEADER_BYTES: u64 = 33;
pub const CURRENT_ENTRY_BYTES: u64 = 16;
pub const RETAINED_ENTRY_BYTES: u64 = 24;
pub const WRITTEN_ENTRY_BYTES: u64 = 12;

pub fn content_hash(bytes: &[u8]) -> u64 {
    let mut h = DefaultHasher::new();
    bytes.hash(&mut h);
    h.finish()
}

fn hash_of<T: Hash>(v: T) -> u64 {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

const MAGIC: u32 = 0x5348_4950;
const T_BEGIN: u8 = 1;
const T_DEAD: u8 = 2;
const T_REF: u8 = 3;
const T_TRUNK_PAGE: u8 = 4;
const T_SLOT: u8 = 5;
const T_STATE: u8 = 6;
const T_TRUNK_META: u8 = 7;
const T_LIVE_LIST: u8 = 8;
const T_END: u8 = 9;
const ENC_RAW: u8 = 0;
const ENC_DELTA: u8 = 1;
/// A run-diff against another slot the receiver holds (the page's fork base).
const ENC_DELTA_SLOT: u8 = 2;
/// A run-diff against a trunk page of the receiver's image (the page's fork base).
const ENC_DELTA_TRUNK: u8 = 3;
/// Two differing runs closer than this are shipped as one.
const DELTA_GAP: usize = 8;

/// The differing byte runs of `page` against `base`, merged across gaps of at most `DELTA_GAP`
/// equal bytes, encoded as `u16 count, (u16 offset, u16 len, bytes)*`. `None` when not smaller than
/// the raw page (or the page is too large for 16-bit offsets).
fn delta_encode(base: &[u8], page: &[u8]) -> Option<Vec<u8>> {
    if page.len() > 32768 || base.len() != page.len() {
        return None;
    }
    let mut runs: Vec<(usize, usize)> = Vec::new();
    let mut i = 0;
    while i < page.len() {
        if base[i] == page[i] {
            i += 1;
            continue;
        }
        let start = i;
        while i < page.len() && base[i] != page[i] {
            i += 1;
        }
        match runs.last_mut() {
            Some((s, e)) if start - *e <= DELTA_GAP => {
                let _ = s;
                *e = i;
            }
            _ => runs.push((start, i)),
        }
    }
    let size = 2 + runs.iter().map(|(s, e)| 4 + (e - s)).sum::<usize>();
    if size >= page.len() || runs.len() > u16::MAX as usize {
        return None;
    }
    let mut out = Vec::with_capacity(size);
    out.extend_from_slice(&(runs.len() as u16).to_le_bytes());
    for (s, e) in runs {
        out.extend_from_slice(&(s as u16).to_le_bytes());
        out.extend_from_slice(&((e - s) as u16).to_le_bytes());
        out.extend_from_slice(&page[s..e]);
    }
    Some(out)
}

fn delta_decode(base: &[u8], delta: &[u8], out: &mut [u8]) -> Result<()> {
    let bad = || LimboError::Corrupt("malformed page delta in a branch stream".to_string());
    out.copy_from_slice(base);
    let mut at = 0usize;
    let mut take = |n: usize| -> Result<&[u8]> {
        let s = delta.get(at..at + n).ok_or_else(bad)?;
        at += n;
        Ok(s)
    };
    let count = u16::from_le_bytes(take(2)?.try_into().unwrap());
    for _ in 0..count {
        let off = u16::from_le_bytes(take(2)?.try_into().unwrap()) as usize;
        let len = u16::from_le_bytes(take(2)?.try_into().unwrap()) as usize;
        let bytes = take(len)?;
        out.get_mut(off..off + len).ok_or_else(bad)?.copy_from_slice(bytes);
    }
    if at != delta.len() {
        return Err(bad());
    }
    Ok(())
}

/// The byte sink of a send: counts every byte and forwards it when a writer is attached.
struct Sink<'a> {
    out: Option<&'a mut dyn Write>,
    buf: Vec<u8>,
    bytes: u64,
}

impl Sink<'_> {
    fn put(&mut self, data: &[u8]) -> Result<()> {
        self.bytes += data.len() as u64;
        if self.out.is_some() {
            self.buf.extend_from_slice(data);
            if self.buf.len() >= 1 << 20 {
                self.flush()?;
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if let Some(out) = self.out.as_mut() {
            out.write_all(&self.buf)
                .map_err(|e| LimboError::InternalError(format!("branch stream write: {e}")))?;
            self.buf.clear();
        }
        Ok(())
    }
}

/// Little-endian record builder.
#[derive(Default)]
struct Rec(Vec<u8>);

impl Rec {
    fn new(tag: u8) -> Self {
        Rec(vec![tag])
    }
    fn u8(mut self, v: u8) -> Self {
        self.0.push(v);
        self
    }
    fn u16(mut self, v: u16) -> Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    fn u32(mut self, v: u32) -> Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    fn u64(mut self, v: u64) -> Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    fn bytes(mut self, v: &[u8]) -> Self {
        self.0.extend_from_slice(v);
        self
    }
}

/// Little-endian record reader over the stream.
struct Src<'a> {
    r: &'a mut dyn Read,
}

impl Src<'_> {
    fn fill(&mut self, buf: &mut [u8]) -> Result<()> {
        self.r
            .read_exact(buf)
            .map_err(|e| LimboError::Corrupt(format!("branch stream truncated: {e}")))
    }
    fn u8(&mut self) -> Result<u8> {
        let mut b = [0u8; 1];
        self.fill(&mut b)?;
        Ok(b[0])
    }
    fn u16(&mut self) -> Result<u16> {
        let mut b = [0u8; 2];
        self.fill(&mut b)?;
        Ok(u16::from_le_bytes(b))
    }
    fn u32(&mut self) -> Result<u32> {
        let mut b = [0u8; 4];
        self.fill(&mut b)?;
        Ok(u32::from_le_bytes(b))
    }
    fn u64(&mut self) -> Result<u64> {
        let mut b = [0u8; 8];
        self.fill(&mut b)?;
        Ok(u64::from_le_bytes(b))
    }
    fn vec(&mut self, n: usize) -> Result<Vec<u8>> {
        let mut v = vec![0u8; n];
        self.fill(&mut v)?;
        Ok(v)
    }
}

fn corrupt(msg: impl Into<String>) -> LimboError {
    LimboError::Corrupt(msg.into())
}

/// A state's retained versions as `(page, Retained)`, sorted by page then birth.
fn retained_sorted(lineage: &Lineage) -> Vec<(u32, Retained)> {
    let mut out: Vec<(u32, Retained)> = lineage
        .retained
        .iter()
        .flat_map(|(&page, versions)| versions.values().map(move |&v| (page, v)))
        .collect();
    out.sort_unstable_by_key(|&(page, v)| (page, v.born));
    out
}

impl BranchStore {
    /// Start tracking what a receiver needs. Refused once the arena exists: slots handed out before
    /// tracking have no birth stamps, and a send could not tell what a receiver already holds.
    pub(crate) fn enable_shipping(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        if inner.arena.is_some() || !inner.branches.is_empty() {
            return Err(LimboError::InvalidArgument(
                "enable branch shipping before the first fork: slots handed out untracked carry \
                 no birth stamp, so no incremental send could tell what a receiver holds"
                    .to_string(),
            ));
        }
        if inner.track.is_none() {
            let mut track = Track::default();
            track.view.next_id = inner.next_id;
            inner.track = Some(track);
        }
        self.tracking.store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) fn ship_seq(&self) -> u64 {
        self.inner.lock().track.as_ref().map_or(0, |t| t.seq)
    }

    pub(crate) fn log_bytes(&self) -> u64 {
        self.inner.lock().track.as_ref().map_or(0, |t| t.log_bytes)
    }

    /// The view's upkeep so far: (operations, nodes touched, nodes copied because a snapshot
    /// shared them).
    pub(crate) fn view_work(&self) -> (u64, u64, u64) {
        self.inner.lock().track.as_ref().map_or((0, 0, 0), |t| {
            (t.view_ops, t.view_work.nodes_touched, t.view_work.nodes_copied)
        })
    }

    /// Every receiver has acknowledged `upto`: holes born at or before it can go.
    pub(crate) fn forget_tombstones(&self, upto: u64) -> usize {
        let mut inner = self.inner.lock();
        let Some(t) = inner.track.as_mut() else {
            return 0;
        };
        let keep = t.holes.split_off(&(upto + 1, 0));
        let gone = std::mem::replace(&mut t.holes, keep);
        let mut w = TreeWork::default();
        for &(_, id) in &gone {
            t.view.states.remove(id, &mut w);
        }
        t.view_work.add(w);
        gone.len()
    }

    pub(crate) fn tombstone_count(&self) -> usize {
        self.inner.lock().track.as_ref().map_or(0, |t| t.holes.len())
    }

    /// F-S1: the O(1) snapshot a send serialises from — the view's roots, cloned under the mutex.
    /// Refused while a branch write transaction is open: its copy decisions are taken but its pages
    /// not yet committed (a snapshot is taken at a transaction boundary, like a ZFS txg).
    pub(crate) fn snapshot(&self) -> Result<(ShipSnap, u64)> {
        let t0 = Instant::now();
        let inner = self.inner.lock();
        let snap = Self::snapshot_locked(&inner)?;
        drop(inner);
        Ok((snap, t0.elapsed().as_nanos() as u64))
    }

    fn snapshot_locked(inner: &StoreInner) -> Result<ShipSnap> {
        let t = inner.track.as_ref().ok_or_else(|| {
            LimboError::InvalidArgument("branch shipping is not tracked".to_string())
        })?;
        if inner.writers > 0 {
            return Err(LimboError::Busy);
        }
        Ok(ShipSnap {
            view: t.view.clone(),
            page_size: inner.arena.as_ref().map_or(0, |a| a.page_size()),
        })
    }

    /// Write a stream of `mode` to `out` (or only count it): an O(1) snapshot under the mutex,
    /// then [`send_snapshot`] with no lock. `base` is the receiver's position for an incremental
    /// mode and `None` for a full one; `trunk` is the trunk's image at the same moment, which the
    /// caller must have checkpointed so that it holds every committed trunk write.
    pub(crate) fn send(
        &self,
        mode: SendMode,
        base: Option<u64>,
        trunk: &TrunkImage,
        use_delta: bool,
        plant: Plant,
        count: bool,
        out: Option<&mut dyn Write>,
    ) -> Result<SendReport> {
        let t0 = Instant::now();
        let inner = self.inner.lock();
        let snap = Self::snapshot_locked(&inner)?;
        // Mutant S1 (PREREG A5): the send keeps the mutex through serialisation.
        let held = if super::super::ship_mutant() == "S1" {
            Some(inner)
        } else {
            drop(inner);
            None
        };
        let locked_ns = t0.elapsed().as_nanos() as u64;
        let mut rep = send_snapshot(&snap, mode, base, trunk, use_delta, plant, count, out)?;
        rep.locked_ops = 1;
        rep.locked_ns = match held {
            Some(guard) => {
                drop(guard);
                t0.elapsed().as_nanos() as u64
            }
            None => locked_ns,
        };
        Ok(rep)
    }

    /// Apply a stream from [`BranchStore::send`] to this store, a replica whose trunk image is
    /// `trunk`. `replica_at` is the replica's position (`None` before its full stream) and is
    /// advanced on success. A refused stream leaves the replica unusable; the caller re-seeds it.
    pub(crate) fn receive(
        &self,
        r: &mut dyn Read,
        trunk: &mut TrunkImage,
        replica_at: &mut Option<u64>,
        skip_inherit: bool,
    ) -> Result<RecvWork> {
        let mut inner = self.inner.lock();
        let mut src = Src { r };
        let mut work = RecvWork::default();
        if src.u8()? != T_BEGIN || src.u32()? != MAGIC || src.u8()? != 1 {
            return Err(corrupt("not a branch stream"));
        }
        let incremental = src.u8()? == 1;
        let base = src.u64()?;
        let _to = src.u64()?;
        let page_size = src.u32()? as usize;
        let trunk_pages = src.u32()?;
        let next_id = src.u64()?;
        match (incremental, *replica_at) {
            (false, None) => {
                if inner.arena.is_some() || !inner.branches.is_empty() {
                    return Err(corrupt("a full stream needs an empty replica"));
                }
                *trunk = TrunkImage::empty(page_size);
            }
            (true, Some(at)) if at == base => {}
            (true, at) => {
                return Err(corrupt(format!(
                    "incremental stream from {base} but the replica is at {at:?}"
                )))
            }
            (false, Some(_)) => return Err(corrupt("a full stream needs an empty replica")),
        }
        if trunk.page_size() != page_size {
            return Err(corrupt("page size differs from the replica's"));
        }
        if inner.arena.is_none() {
            inner.arena = Some(Arena::new(page_size));
        }
        let mut schemas: HashMap<u32, Arc<Schema>> = HashMap::new();
        let mut schema_for = |version: u32| -> Arc<Schema> {
            schemas
                .entry(version)
                .or_insert_with(|| {
                    let mut s = Schema::default();
                    s.schema_version = version;
                    Arc::new(s)
                })
                .clone()
        };
        let mut new_states: Vec<BranchId> = Vec::new();
        let mut page = vec![0u8; page_size];
        let to = loop {
            let tag = src.u8()?;
            work.records += 1;
            match tag {
                T_DEAD => {
                    let id = BranchId(src.u64()?);
                    work.deaths += 1;
                    work.gc_freed += inner.remove_state(id)? as u64;
                }
                T_REF => {
                    let slot = src.u32()?;
                    let pgno = src.u32()?;
                    let hash = src.u64()?;
                    let held = trunk
                        .page(pgno)
                        .ok_or_else(|| corrupt(format!("reference to trunk page {pgno} the replica lacks")))?;
                    if content_hash(held) != hash {
                        return Err(corrupt(format!(
                            "reference to trunk page {pgno}: the replica's copy has a different \
                             content hash, so it is not the version the sender retained"
                        )));
                    }
                    let arena = inner.arena.as_mut().expect("created above");
                    if arena.is_live(slot) {
                        return Err(corrupt(format!("reference claims live slot {slot}")));
                    }
                    arena.claim(slot);
                    arena.set_page(slot, held);
                    work.refs += 1;
                    work.slots_claimed += 1;
                }
                T_TRUNK_PAGE => {
                    let pgno = src.u32()?;
                    let enc = src.u8()?;
                    if enc != ENC_RAW || pgno == 0 {
                        return Err(corrupt("trunk pages ship raw, 1-based"));
                    }
                    src.fill(&mut page)?;
                    trunk.set_page(pgno, &page);
                    work.trunk_pages += 1;
                }
                T_SLOT => {
                    let slot = src.u32()?;
                    let pgno = src.u32()?;
                    let fresh = src.u8()? == 1;
                    match src.u8()? {
                        ENC_RAW => src.fill(&mut page)?,
                        ENC_DELTA => {
                            let n = src.u16()? as usize;
                            let d = src.vec(n)?;
                            let base_page = trunk
                                .page(pgno)
                                .ok_or_else(|| corrupt(format!("delta against trunk page {pgno} the replica lacks")))?;
                            delta_decode(base_page, &d, &mut page)?;
                        }
                        ENC_DELTA_SLOT => {
                            let base_slot = src.u32()?;
                            let n = src.u16()? as usize;
                            let d = src.vec(n)?;
                            // Mutant D (PREREG A5): decode against the page's trunk page instead.
                            let base_page: std::sync::Arc<[u8]> = if super::super::ship_mutant() == "D" {
                                std::sync::Arc::from(trunk.page(pgno).ok_or_else(|| corrupt("no trunk page"))?)
                            } else {
                                let arena = inner.arena.as_ref().expect("created above");
                                if !arena.is_live(base_slot) {
                                    return Err(corrupt(format!(
                                        "slot {slot}'s delta names base slot {base_slot}, which the replica does not hold"
                                    )));
                                }
                                arena.page_arc(base_slot).clone()
                            };
                            delta_decode(&base_page, &d, &mut page)?;
                            work.fork_deltas += 1;
                        }
                        ENC_DELTA_TRUNK => {
                            let base_pgno = src.u32()?;
                            let n = src.u16()? as usize;
                            let d = src.vec(n)?;
                            let base_page = trunk.page(base_pgno).ok_or_else(|| {
                                corrupt(format!("delta against trunk page {base_pgno} the replica lacks"))
                            })?;
                            delta_decode(base_page, &d, &mut page)?;
                            work.fork_deltas += 1;
                        }
                        _ => return Err(corrupt("unknown page encoding")),
                    }
                    let arena = inner.arena.as_mut().expect("created above");
                    if fresh {
                        if arena.is_live(slot) {
                            return Err(corrupt(format!(
                                "slot {slot} arrives as new but the replica still holds it"
                            )));
                        }
                        arena.claim(slot);
                        work.slots_claimed += 1;
                    } else if !arena.is_live(slot) {
                        return Err(corrupt(format!(
                            "slot {slot} arrives as rewritten but the replica does not hold it"
                        )));
                    }
                    arena.set_page(slot, &page);
                    work.slots_written += 1;
                }
                T_STATE => {
                    let id = BranchId(src.u64()?);
                    let parent = BranchId(src.u64()?);
                    let fork_epoch = src.u64()?;
                    let epoch = src.u64()?;
                    let flags = src.u8()?;
                    let schema_version = src.u32()?;
                    let n_current = src.u32()?;
                    let n_retained = src.u32()?;
                    let mut current = Vec::with_capacity(n_current as usize);
                    for _ in 0..n_current {
                        let page_no = src.u32()?;
                        let slot = src.u32()?;
                        let born = src.u64()?;
                        current.push((page_no, Owned { slot, born }));
                    }
                    let mut retained = Vec::with_capacity(n_retained as usize);
                    for _ in 0..n_retained {
                        let page_no = src.u32()?;
                        let born = src.u64()?;
                        let died = src.u64()?;
                        let slot = src.u32()?;
                        retained.push((page_no, Retained { born, died, slot }));
                    }
                    work.entries += u64::from(n_current) + u64::from(n_retained);
                    let schema = schema_for(schema_version);
                    let handle = flags & 1 != 0;
                    if flags & 2 != 0 {
                        inner.insert_state(id, parent, fork_epoch, schema.clone())?;
                        new_states.push(id);
                        work.states_new += 1;
                    } else {
                        work.states_updated += 1;
                    }
                    let StoreInner {
                        arena, branches, ..
                    } = &mut *inner;
                    let st = branches.get_mut(&id).ok_or_else(|| gone(id))?;
                    st.lineage.epoch = epoch;
                    st.handle = handle;
                    if st.schema.schema_version != schema_version {
                        st.schema = schema;
                    }
                    // A slot an old `current` entry named and nothing in this record names any more
                    // was retained and freed on the sender inside the window (see the module doc).
                    // A partial record carries only the changed entries; a whole one replaces them.
                    let named: HashSet<Slot> = current
                        .iter()
                        .map(|(_, o)| o.slot)
                        .chain(retained.iter().map(|(_, v)| v.slot))
                        .collect();
                    let arena = arena.as_mut().expect("created above");
                    let replaced: Vec<Owned> = if flags & 4 != 0 {
                        current
                            .iter()
                            .filter_map(|&(page_no, o)| st.current.insert(page_no, o))
                            .collect()
                    } else {
                        let old: Vec<Owned> = st.current.values().copied().collect();
                        st.current = current.iter().copied().collect();
                        old
                    };
                    for o in replaced {
                        if !named.contains(&o.slot) {
                            arena.release(o.slot);
                            work.slots_released += 1;
                        }
                    }
                    st.view = None;
                    for (page_no, v) in retained {
                        st.lineage.retain(page_no, v);
                        work.retained_inserted += 1;
                    }
                }
                T_TRUNK_META => {
                    let epoch = src.u64()?;
                    let n_written = src.u32()?;
                    let n_retained = src.u32()?;
                    inner.trunk.lineage.epoch = epoch;
                    for _ in 0..n_written {
                        let page_no = src.u32()?;
                        let e = src.u64()?;
                        inner.trunk.written.insert(page_no, e);
                    }
                    for _ in 0..n_retained {
                        let page_no = src.u32()?;
                        let born = src.u64()?;
                        let died = src.u64()?;
                        let slot = src.u32()?;
                        inner
                            .trunk
                            .lineage
                            .retain(page_no, Retained { born, died, slot });
                        work.retained_inserted += 1;
                    }
                    work.entries += u64::from(n_written) + u64::from(n_retained);
                }
                T_END => break src.u64()?,
                T_LIVE_LIST => {
                    return Err(corrupt(
                        "a live-list stream is counted, never received (IncrRoot)",
                    ))
                }
                other => return Err(corrupt(format!("unknown branch stream record {other}"))),
            }
        };
        trunk.resize(trunk_pages);
        inner.next_id = next_id;
        if !skip_inherit {
            work.trie_inserts += inner.derive_inherited(&new_states);
        }
        let trunk_children = inner.trunk.lineage.children.len();
        self.trunk_children.store(trunk_children, Ordering::Release);
        *replica_at = Some(to);
        Ok(work)
    }

    /// A canonical digest of everything a reader of any branch can observe, and everything the
    /// store's next decision depends on: every state's parent, fork epoch, epoch, trunk_at,
    /// handle, schema version, children, current, retained and inherited entries; the trunk's
    /// lineage and written map; every live slot's content hash; every trunk page's hash; next_id.
    /// Excluded: `view` (a cache of `inherited` + `current`), connection flags, shipping stamps.
    pub(crate) fn digest(&self, trunk: &TrunkImage) -> Digest {
        let inner = self.inner.lock();
        let mut a = DefaultHasher::new();
        let mut b = DefaultHasher::new();
        0xA5A5_5A5Au32.hash(&mut b);
        let mut entries = 0u64;
        let mut feed = |v: &dyn Fn(&mut DefaultHasher)| {
            v(&mut a);
            v(&mut b);
            entries += 1;
        };
        feed(&|h| inner.next_id.hash(h));
        let mut ids: Vec<BranchId> = inner.branches.keys().copied().collect();
        ids.sort_unstable();
        for id in &ids {
            let st = &inner.branches[id];
            feed(&|h| {
                (
                    id.0,
                    st.parent.0,
                    st.fork_epoch,
                    st.lineage.epoch,
                    st.trunk_at,
                    st.handle,
                    st.schema.schema_version,
                )
                    .hash(h)
            });
            for (&f, c) in &st.lineage.children {
                feed(&|h| (1u8, f, c.0).hash(h));
            }
            let mut current: Vec<(u32, Owned)> = st.current.iter().map(|(&p, &o)| (p, o)).collect();
            current.sort_unstable_by_key(|&(p, _)| p);
            for (p, o) in current {
                feed(&|h| (2u8, p, o.slot, o.born).hash(h));
            }
            for (p, v) in retained_sorted(&st.lineage) {
                feed(&|h| (3u8, p, v.born, v.died, v.slot).hash(h));
            }
            st.inherited.for_each(|p, s| feed(&|h| (4u8, p, s).hash(h)));
        }
        feed(&|h| inner.trunk.lineage.epoch.hash(h));
        for (&f, c) in &inner.trunk.lineage.children {
            feed(&|h| (5u8, f, c.0).hash(h));
        }
        for (p, v) in retained_sorted(&inner.trunk.lineage) {
            feed(&|h| (6u8, p, v.born, v.died, v.slot).hash(h));
        }
        let mut written: Vec<(u32, u64)> = inner.trunk.written.iter().map(|(&p, &e)| (p, e)).collect();
        written.sort_unstable();
        for (p, e) in written {
            feed(&|h| (7u8, p, e).hash(h));
        }
        let mut slots = 0u64;
        if let Some(arena) = inner.arena.as_ref() {
            for slot in arena.slots_in_use() {
                let c = content_hash(arena.page(slot));
                feed(&|h| (8u8, slot, c).hash(h));
                slots += 1;
            }
        }
        for pgno in 1..=trunk.pages() {
            let c = content_hash(trunk.page(pgno).unwrap());
            feed(&|h| (9u8, pgno, c).hash(h));
        }
        Digest {
            hash: (a.finish(), b.finish()),
            states: ids.len() as u64,
            slots,
            entries,
        }
    }

    /// Every content hash and metadata entry, for the incremental-minimum instrument.
    pub(crate) fn dump(&self, trunk: &TrunkImage) -> ShipDump {
        let inner = self.inner.lock();
        let mut d = ShipDump::default();
        if let Some(arena) = inner.arena.as_ref() {
            for slot in arena.slots_in_use() {
                d.content.insert(content_hash(arena.page(slot)));
            }
        }
        for pgno in 1..=trunk.pages() {
            d.content.insert(content_hash(trunk.page(pgno).unwrap()));
        }
        for (id, st) in &inner.branches {
            d.states.insert(id.0);
            d.state_headers.insert(hash_of((
                id.0,
                st.parent.0,
                st.fork_epoch,
                st.lineage.epoch,
                st.handle,
                st.schema.schema_version,
            )));
            for (&p, o) in &st.current {
                d.current.insert(hash_of((id.0, p, o.slot, o.born)));
            }
            for (&p, versions) in &st.lineage.retained {
                for v in versions.values() {
                    d.retained.insert(hash_of((id.0, p, v.born, v.died, v.slot)));
                }
            }
        }
        for (&p, &e) in &inner.trunk.written {
            d.trunk_written.insert(hash_of((p, e)));
        }
        for (&p, versions) in &inner.trunk.lineage.retained {
            for v in versions.values() {
                d.trunk_retained.insert(hash_of((p, v.born, v.died, v.slot)));
            }
        }
        d
    }

    /// Write branch `id`'s whole database image, as a connection on it reads it, to `out`: page 1's
    /// header gives the page count, and each page is the branch's arena version or the trunk's page
    /// from `trunk`. Returns (pages written, pages that differ from `trunk`'s page of that number).
    pub(crate) fn export(
        &self,
        id: BranchId,
        trunk: &TrunkImage,
        out: &mut dyn Write,
    ) -> Result<(u32, u32)> {
        let inner = self.inner.lock();
        let read = |pgno: u32| -> Result<Vec<u8>> {
            let (mut levels, mut examined) = (0, 0);
            match inner.resolve(id, pgno, &mut levels, &mut examined)? {
                Some(slot) => Ok(inner
                    .arena
                    .as_ref()
                    .expect("a slot resolved, so the arena exists")
                    .page(slot)
                    .to_vec()),
                None => trunk.page(pgno).map(<[u8]>::to_vec).ok_or_else(|| {
                    corrupt(format!("branch {} reads trunk page {pgno}, beyond the image", id.0))
                }),
            }
        };
        let first = read(1)?;
        let in_header = u32::from_be_bytes(first[28..32].try_into().unwrap());
        let pages = if in_header == 0 { trunk.pages() } else { in_header };
        let mut differ = 0u32;
        for pgno in 1..=pages {
            let p = if pgno == 1 { first.clone() } else { read(pgno)? };
            if trunk.page(pgno) != Some(&p[..]) {
                differ += 1;
            }
            out.write_all(&p)
                .map_err(|e| LimboError::InternalError(format!("export write: {e}")))?;
        }
        Ok((pages, differ))
    }
}

/// The fork base of a slot the view holds: the version its owner saw before writing it. A branch
/// page's is what the branch reads through its ancestry (`inherited`, else the trunk's version as of
/// `trunk_at`, else the trunk page); a retained trunk version's is the page's next version, retained
/// or current. Returns the base as a slot of the view or as a trunk page number.
fn fork_base(v: &ShipView, sv: &SlotView) -> Result<(Option<Slot>, u32)> {
    let page = sv.page;
    match sv.owner {
        SlotOwner::Trunk { died, .. } => Ok((
            v.trunk_retained.get_live(ret_key(page, died)).map(|n| n.slot),
            page,
        )),
        SlotOwner::Branch(owner) => {
            let st = v
                .states
                .get_live(owner)
                .ok_or_else(|| corrupt(format!("slot of page {page} owned by state {owner}, which the view lacks")))?;
            if let Some(slot) = st.inherited.get(page) {
                return Ok((Some(slot), page));
            }
            let at = st.trunk_at;
            let covering = v
                .trunk_retained
                .pred(ret_key(page, 0), ret_key(page, at))
                .filter(|(_, ver)| at < ver.died)
                .map(|(_, ver)| ver.slot);
            Ok((covering, page))
        }
    }
}

/// Serialise `snap` with no lock held (F-S1): everything below reads only the snapshot's trees and
/// the bytes they share. Incremental modes walk the trees from `base` (F-S2).
#[allow(clippy::too_many_arguments)]
pub(crate) fn send_snapshot(
    snap: &ShipSnap,
    mode: SendMode,
    base: Option<u64>,
    trunk: &TrunkImage,
    use_delta: bool,
    plant: Plant,
    count: bool,
    out: Option<&mut dyn Write>,
) -> Result<SendReport> {
    let v = &snap.view;
    if mode.incremental() != base.is_some() {
        return Err(LimboError::InvalidArgument(format!(
            "{mode:?} needs {} base",
            if mode.incremental() { "a" } else { "no" }
        )));
    }
    if snap.page_size != 0 && snap.page_size != trunk.page_size() {
        return Err(LimboError::InvalidArgument(
            "trunk image page size differs from the arena's".to_string(),
        ));
    }
    let base_seq = base.unwrap_or(0);
    if base_seq > v.seq {
        return Err(LimboError::InvalidArgument(format!(
            "base {base_seq} is ahead of the snapshot ({})",
            v.seq
        )));
    }
    let incremental = mode.incremental();
    // Modes that find changes by walking from the base (the others re-describe everything).
    let pruned = matches!(mode, SendMode::IncrFix | SendMode::IncrAlloc);
    let mut rep = SendReport {
        base: base_seq,
        to: v.seq,
        ..Default::default()
    };
    let mut sink = Sink {
        out,
        buf: Vec::new(),
        bytes: 0,
    };
    let page_size = trunk.page_size();
    if sink.out.is_some() && !matches!(mode, SendMode::FullFix | SendMode::IncrFix) {
        return Err(LimboError::InvalidArgument(format!(
            "{mode:?} is counted, never shipped"
        )));
    }
    let mut tw = TreeWork::default();
    let mut seen: HashSet<u64> = HashSet::new();
    let planted = Cell::new(false);

    let begin = Rec::new(T_BEGIN)
        .u32(MAGIC)
        .u8(1)
        .u8(u8::from(incremental))
        .u64(base_seq)
        .u64(v.seq)
        .u32(page_size as u32)
        .u32(trunk.pages())
        .u64(v.next_id);
    rep.meta_bytes += begin.0.len() as u64;
    sink.put(&begin.0)?;

    // The states to describe, and the holes a receiver at `base` must hear about.
    let mut states: Vec<(u64, std::sync::Arc<StateView>)> = Vec::new();
    let mut dead: Vec<u64> = Vec::new();
    if pruned {
        v.states.walk_changed(base_seq, &mut tw, |id, item| match item {
            Item::Live { val, .. } => states.push((id, val.clone())),
            Item::Hole { born, .. } => {
                if *born <= base_seq {
                    dead.push(id);
                }
            }
        });
    } else {
        v.states.walk_all(&mut tw, |id, item| {
            if let Some(val) = item.live() {
                states.push((id, val.clone()));
            }
        });
    }

    // 1. Deaths, children before parents.
    if incremental {
        if mode == SendMode::IncrRoot {
            // `recv -F`: the full live list; the receiver drops what it holds and this lacks.
            rep.states_visited += states.len() as u64;
            let mut rec = Rec::new(T_LIVE_LIST).u64(states.len() as u64);
            for (id, _) in &states {
                rec = rec.u64(*id);
            }
            rep.live_list_bytes += rec.0.len() as u64;
            rep.meta_bytes += rec.0.len() as u64;
            sink.put(&rec.0)?;
        } else {
            dead.sort_unstable_by(|a, b| b.cmp(a));
            for id in dead {
                if plant == Plant::DropTombstone && !planted.get() {
                    planted.set(true);
                    continue;
                }
                let rec = Rec::new(T_DEAD).u64(id);
                rep.dead_records += 1;
                rep.meta_bytes += rec.0.len() as u64;
                sink.put(&rec.0)?;
            }
        }
    }

    // 2. States, in id order: whole when new (or re-described), else their changes since `base`.
    for (id, sv) in &states {
        rep.states_visited += 1;
        let new = !incremental || sv.born_seq > base_seq;
        let partial = pruned && !new;
        let mut current: Vec<std::sync::Arc<CurView>> = Vec::new();
        let mut retained: Vec<std::sync::Arc<RetView>> = Vec::new();
        let mut take_cur = |_: u64, it: &Item<CurView>| {
            if let Some(c) = it.live() {
                current.push(c.clone());
            }
        };
        if partial {
            sv.current.walk_changed(base_seq, &mut tw, &mut take_cur);
        } else {
            sv.current.walk_all(&mut tw, &mut take_cur);
        }
        let mut take_ret = |_: u64, it: &Item<RetView>| {
            if let Some(r) = it.live() {
                retained.push(r.clone());
            }
        };
        if partial {
            sv.retained.walk_changed(base_seq, &mut tw, &mut take_ret);
        } else {
            sv.retained.walk_all(&mut tw, &mut take_ret);
        }
        rep.entries_visited += (current.len() + retained.len()) as u64;
        let mut rec = Rec::new(T_STATE)
            .u64(*id)
            .u64(sv.parent)
            .u64(sv.fork_epoch)
            .u64(sv.epoch)
            .u8(u8::from(sv.handle) | (u8::from(new) << 1) | (u8::from(partial) << 2))
            .u32(sv.schema_version)
            .u32(current.len() as u32)
            .u32(retained.len() as u32);
        for c in &current {
            rec = rec.u32(c.page).u32(c.slot).u64(c.born);
        }
        for r in &retained {
            rec = rec.u32(r.page).u64(r.born).u64(r.died).u32(r.slot);
        }
        rep.state_records += 1;
        rep.current_entries += current.len() as u64;
        rep.retained_entries += retained.len() as u64;
        rep.meta_bytes += rec.0.len() as u64;
        sink.put(&rec.0)?;
        if mode == SendMode::FullMaps {
            // A per-dataset stream describes each clone's whole map: `inherited` ∪ `current`.
            let own_new = current
                .iter()
                .filter(|c| sv.inherited.get(c.page).is_none())
                .count();
            let entries = (sv.inherited.len() + own_new) as u64;
            rep.entries_visited += entries;
            rep.maps_bytes += 13 + 8 * entries;
            rep.meta_bytes += 13 + 8 * entries;
            sink.bytes += 13 + 8 * entries;
        }
    }

    // 3. The trunk's own metadata.
    let mut written: Vec<std::sync::Arc<WrittenView>> = Vec::new();
    let mut take_written = |_: u64, it: &Item<WrittenView>| {
        if let Some(w) = it.live() {
            written.push(w.clone());
        }
    };
    if incremental {
        v.written.walk_changed(base_seq, &mut tw, &mut take_written);
    } else {
        v.written.walk_all(&mut tw, &mut take_written);
    }
    let mut versions: Vec<std::sync::Arc<TrunkVersion>> = Vec::new();
    let mut take_version = |_: u64, it: &Item<TrunkVersion>| {
        if let Some(ver) = it.live() {
            versions.push(ver.clone());
        }
    };
    if incremental {
        v.trunk_retained.walk_changed(base_seq, &mut tw, &mut take_version);
    } else {
        v.trunk_retained.walk_all(&mut tw, &mut take_version);
    }
    let meta_written: Vec<(u32, u64)> = if pruned || !incremental {
        written.iter().filter_map(|w| w.epoch.map(|e| (w.page, e))).collect()
    } else {
        // IncrRoot re-describes the trunk's whole written map.
        let mut all = Vec::new();
        v.written.walk_all(&mut tw, |_, it| {
            if let Some(w) = it.live() {
                if let Some(e) = w.epoch {
                    all.push((w.page, e));
                }
            }
        });
        all
    };
    let meta_versions: Vec<std::sync::Arc<TrunkVersion>> = if pruned || !incremental {
        versions.clone()
    } else {
        let mut all = Vec::new();
        v.trunk_retained.walk_all(&mut tw, |_, it| {
            if let Some(ver) = it.live() {
                all.push(ver.clone());
            }
        });
        all
    };
    rep.entries_visited += (meta_written.len() + meta_versions.len()) as u64;
    let mut rec = Rec::new(T_TRUNK_META)
        .u64(v.trunk_epoch)
        .u32(meta_written.len() as u32)
        .u32(meta_versions.len() as u32);
    for &(page, e) in &meta_written {
        rec = rec.u32(page).u64(e);
    }
    for ver in &meta_versions {
        rec = rec.u32(ver.page).u64(ver.born).u64(ver.died).u32(ver.slot);
    }
    rep.written_entries += meta_written.len() as u64;
    rep.trunk_retained_entries += meta_versions.len() as u64;
    rep.meta_bytes += rec.0.len() as u64;
    sink.put(&rec.0)?;

    let mut ship_page = |sink: &mut Sink,
                         rep: &mut SendReport,
                         tag: u8,
                         id: u32,
                         pgno: u32,
                         fresh: bool,
                         content: &[u8],
                         same_path: Option<&[u8]>,
                         fork: Option<(Option<Slot>, &[u8])>|
     -> Result<()> {
        let owned;
        let mut payload = content;
        if plant == Plant::FlipPayload && tag == T_SLOT && !planted.get() {
            planted.set(true);
            let mut flipped = content.to_vec();
            flipped[page_size / 2] ^= 0x5A;
            owned = flipped;
            payload = &owned[..];
        }
        let fork_delta = fork.and_then(|(_, b)| {
            if count || use_delta {
                delta_encode(b, payload)
            } else {
                None
            }
        });
        if count {
            rep.payload_delta += same_path
                .and_then(|b| delta_encode(b, payload))
                .map_or(payload.len(), |d| d.len() + 2) as u64;
            rep.payload_delta_fork += fork_delta.as_ref().map_or(payload.len(), |d| d.len() + 6) as u64;
            if seen.insert(content_hash(payload)) {
                rep.payload_dedup += payload.len() as u64;
            } else {
                rep.dup_payloads += 1;
                rep.payload_dedup += 13;
            }
        }
        rep.payload_raw += payload.len() as u64;
        let mut rec = Rec::new(tag).u32(id);
        if tag == T_SLOT {
            rec = rec.u32(pgno).u8(u8::from(fresh));
        }
        rec = match (use_delta, fork_delta, fork) {
            (true, Some(d), Some((base_slot, _))) => {
                rep.payload_shipped += d.len() as u64 + 6;
                match base_slot {
                    Some(b) => rec.u8(ENC_DELTA_SLOT).u32(b),
                    None => rec.u8(ENC_DELTA_TRUNK).u32(pgno),
                }
                .u16(d.len() as u16)
                .bytes(&d)
            }
            _ => {
                rep.payload_shipped += payload.len() as u64;
                rec.u8(ENC_RAW).bytes(payload)
            }
        };
        rep.page_bytes += rec.0.len() as u64;
        sink.put(&rec.0)
    };
    // The bytes of a fork base: a slot of the snapshot, or a trunk page of the image.
    let base_bytes = |base: (Option<Slot>, u32)| -> Option<(Option<Slot>, &[u8])> {
        match base {
            (Some(slot), _) => v
                .slots
                .get_live(u64::from(slot))
                .map(|b| (Some(slot), &b.data[..])),
            (None, pgno) => trunk.page(pgno).map(|p| (None, p)),
        }
    };

    // 4. References to trunk pre-images the receiver holds (IncrAlloc ships them as data).
    if incremental {
        let changed: Vec<std::sync::Arc<TrunkVersion>> = if pruned {
            versions.clone()
        } else {
            let mut c = Vec::new();
            v.trunk_retained.walk_changed(base_seq, &mut tw, |_, it| {
                if let Some(ver) = it.live() {
                    c.push(ver.clone());
                }
            });
            c
        };
        for ver in changed {
            let sv = v
                .slots
                .get_live(u64::from(ver.slot))
                .ok_or_else(|| corrupt("a retained trunk version's slot is missing from the view"))?;
            if sv.content_seq > base_seq || sv.alloc_seq <= base_seq {
                continue; // new content (a SLOT below), or a slot the receiver holds already
            }
            if mode == SendMode::IncrAlloc {
                let fb = fork_base(v, sv)?;
                ship_page(&mut sink, &mut rep, T_SLOT, ver.slot, ver.page, true, &sv.data[..], trunk.page(ver.page), base_bytes(fb))?;
                rep.slot_records += 1;
                continue;
            }
            let mut hash = content_hash(&sv.data);
            if plant == Plant::BadRefHash && !planted.get() {
                planted.set(true);
                hash ^= 1;
            }
            let rec = Rec::new(T_REF).u32(ver.slot).u32(ver.page).u64(hash);
            rep.ref_records += 1;
            rep.ref_bytes += rec.0.len() as u64;
            sink.put(&rec.0)?;
        }
    }

    // 5. Trunk pages.
    let trunk_pages: Vec<u32> = if incremental {
        let mut pages: Vec<u32> = if pruned {
            written.iter().map(|w| w.page).collect()
        } else {
            let mut c = Vec::new();
            v.written.walk_changed(base_seq, &mut tw, |_, it| {
                if let Some(w) = it.live() {
                    c.push(w.page);
                }
            });
            c
        };
        pages.retain(|&p| p >= 1 && p <= trunk.pages());
        pages
    } else {
        (1..=trunk.pages()).collect()
    };
    for pgno in trunk_pages {
        let page = trunk.page(pgno).expect("filtered to the image");
        ship_page(&mut sink, &mut rep, T_TRUNK_PAGE, pgno, pgno, false, page, None, None)?;
        rep.trunk_page_records += 1;
    }

    // 6. Slots whose content is newer than the base, bases first.
    let mut slots: Vec<(u64, std::sync::Arc<SlotView>)> = Vec::new();
    let mut take_slot = |key: u64, it: &Item<SlotView>| {
        if let Some(sv) = it.live() {
            if !incremental || sv.content_seq > base_seq {
                slots.push((key, sv.clone()));
            }
        }
    };
    if incremental {
        v.slots.walk_changed(base_seq, &mut tw, &mut take_slot);
    } else {
        v.slots.walk_all(&mut tw, &mut take_slot);
    }
    // Trunk versions first, newest per page first (each one's base is the next version); then
    // branch slots by owner id (a base belongs to the trunk or to an ancestor, a smaller id).
    slots.sort_unstable_by_key(|(slot, sv)| match sv.owner {
        SlotOwner::Trunk { born, .. } => (0u8, u64::from(sv.page), u64::MAX - born, *slot),
        SlotOwner::Branch(owner) => (1u8, owner, 0, *slot),
    });
    for (slot, sv) in &slots {
        rep.slots_visited += 1;
        let fresh = !incremental || sv.alloc_seq > base_seq;
        let fb = fork_base(v, sv)?;
        ship_page(
            &mut sink,
            &mut rep,
            T_SLOT,
            *slot as Slot,
            sv.page,
            fresh,
            &sv.data[..],
            trunk.page(sv.page),
            base_bytes(fb),
        )?;
        rep.slot_records += 1;
    }

    let end = Rec::new(T_END).u64(v.seq);
    rep.meta_bytes += end.0.len() as u64;
    sink.put(&end.0)?;
    sink.flush()?;
    rep.total_bytes = sink.bytes;
    rep.nodes_visited = tw.nodes_visited;
    rep.items_checked = tw.items_checked;
    rep.index_visited = tw.nodes_visited;
    Ok(rep)
}

impl StoreInner {
    /// Insert a state that arrived in a stream, and hang it on its parent.
    fn insert_state(
        &mut self,
        id: BranchId,
        parent: BranchId,
        fork_epoch: u64,
        schema: Arc<Schema>,
    ) -> Result<()> {
        if self.branches.contains_key(&id) {
            return Err(corrupt(format!("state {} arrives as new but exists", id.0)));
        }
        let trunk_at = if parent.is_trunk() {
            self.trunk.lineage.children.insert(fork_epoch, id);
            fork_epoch
        } else {
            let p = self
                .branches
                .get_mut(&parent)
                .ok_or_else(|| corrupt(format!("state {} arrives before its parent", id.0)))?;
            p.lineage.children.insert(fork_epoch, id);
            p.trunk_at
        };
        self.branches.insert(
            id,
            BranchState::new(parent, fork_epoch, schema, trunk_at, PageMap::default(), 0),
        );
        Ok(())
    }

    /// Remove a state a tombstone names: release its pages and run its parent's garbage query,
    /// exactly as `collect` does on the sender. Returns the pages freed.
    fn remove_state(&mut self, id: BranchId) -> Result<usize> {
        let st = self
            .branches
            .remove(&id)
            .ok_or_else(|| corrupt(format!("tombstone for state {} the replica lacks", id.0)))?;
        if !st.lineage.children.is_empty() {
            return Err(corrupt(format!(
                "tombstone for state {} which still has children on the replica",
                id.0
            )));
        }
        let StoreInner {
            arena,
            trunk,
            branches,
            work,
            ..
        } = self;
        let arena = arena.as_mut().expect("a state existed, so the arena does");
        let mut freed = 0;
        for owned in st.current.values() {
            arena.release(owned.slot);
            freed += 1;
        }
        freed += st.lineage.release_all(arena).len();
        let lineage: &mut Lineage = if st.parent.is_trunk() {
            &mut trunk.lineage
        } else {
            &mut branches
                .get_mut(&st.parent)
                .ok_or_else(|| corrupt("a removed state's parent is missing"))?
                .lineage
        };
        freed += lineage.child_gone(st.fork_epoch, arena, work).len();
        Ok(freed)
    }

    /// Build `inherited` for each newly arrived state: its parent's `inherited` plus, per page the
    /// parent owned at the fork, the version whose `[born, died)` holds the fork epoch. The parent's
    /// versions are swept once in birth order for all of its new children, so a parent with K new
    /// children and V versions costs V + K trie operations. Returns the trie inserts made.
    fn derive_inherited(&mut self, new_states: &[BranchId]) -> u64 {
        let mut by_parent: HashMap<BranchId, Vec<(u64, BranchId)>> = HashMap::new();
        for &id in new_states {
            let st = &self.branches[&id];
            if !st.parent.is_trunk() {
                by_parent.entry(st.parent).or_default().push((st.fork_epoch, id));
            }
        }
        // Parents in id order: a parent's own `inherited` is final before its children's are built.
        let mut parents: Vec<BranchId> = by_parent.keys().copied().collect();
        parents.sort_unstable();
        let mut inserts = 0u64;
        for parent in parents {
            let mut kids = by_parent.remove(&parent).unwrap();
            kids.sort_unstable();
            let p = &self.branches[&parent];
            let mut versions: Vec<(u64, u32, Slot)> = p
                .current
                .iter()
                .map(|(&page, o)| (o.born, page, o.slot))
                .collect();
            for (&page, vs) in &p.lineage.retained {
                versions.extend(vs.values().map(|v| (v.born, page, v.slot)));
            }
            versions.sort_unstable();
            let mut map = p.inherited.clone();
            let mut i = 0;
            let mut built = Vec::with_capacity(kids.len());
            for (f, kid) in kids {
                while i < versions.len() && versions[i].0 <= f {
                    map.insert(versions[i].1, versions[i].2);
                    inserts += 1;
                    i += 1;
                }
                built.push((kid, map.clone()));
            }
            for (kid, m) in built {
                self.branches.get_mut(&kid).unwrap().inherited = m;
            }
        }
        inserts
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{image, page_with, Rng, PAGE, PAGES};
    use super::*;
    use crate::storage::pager::PageRef;

    fn trunk_image(trunk: &HashMap<u32, u64>) -> TrunkImage {
        let mut bytes = Vec::new();
        for page in 0..PAGES {
            bytes.extend_from_slice(&image(trunk[&page]));
        }
        TrunkImage::new(PAGE, bytes)
    }

    /// Random branch trees — forks from the trunk and from branches, writes on both, reaps that
    /// defer and cascade — with the source shipped to a replica by one full stream and then an
    /// incremental stream every few steps, and a fresh replica seeded by a full stream beside it.
    /// After every stream both replicas must equal the source by digest, and every live branch must
    /// read every page identically on all three. The shapes the stream exists for must occur:
    /// tombstones, trunk pre-image references, deferred reaps, derived page maps.
    #[test]
    fn replicas_equal_the_source_under_random_trees_and_incremental_streams() {
        for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
            run(seed);
        }
    }

    fn run(seed: u64) {
        let store = BranchStore::new();
        store.enable_shipping().unwrap();
        let mut rng = Rng(seed);
        // Pages are numbered 1..=PAGES in the image; the store sees them as 0-based page numbers
        // shifted by one so that trunk page `p` is image page `p + 1`.
        let mut trunk: HashMap<u32, u64> = (0..PAGES).map(|p| (p, 0)).collect();
        let mut live: Vec<crate::branch::BranchId> = Vec::new();
        let mut generation = 1000u64;
        let replica = BranchStore::new();
        let mut rtrunk = TrunkImage::empty(PAGE);
        let mut at: Option<u64> = None;
        let (mut refs, mut deads, mut deferred, mut trie, mut streams) = (0, 0, 0, 0, 0);
        let mut fork_deltas = 0;
        let mut pending: Option<(ShipSnap, TrunkImage, Digest)> = None;
        for step in 0..3000 {
            match rng.below(12) {
                0 if live.len() < 50 => {
                    let id = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
                    live.push(id);
                }
                1..=3 if !live.is_empty() && live.len() < 50 => {
                    let parent = if rng.below(2) == 0 {
                        *live.last().unwrap()
                    } else {
                        live[rng.below(live.len() as u64) as usize]
                    };
                    live.push(store.fork_branch(parent).unwrap());
                }
                4..=5 => {
                    let page = rng.below(u64::from(PAGES)) as u32;
                    store.first_write_trunk(page + 1, &image(trunk[&page]));
                    generation += 1;
                    trunk.insert(page, generation);
                }
                6..=9 if !live.is_empty() => {
                    let id = live[rng.below(live.len() as u64) as usize];
                    store.begin_write(id).unwrap();
                    let mut committed: Vec<PageRef> = Vec::new();
                    for _ in 0..=rng.below(2) {
                        let page = rng.below(u64::from(PAGES)) as u32 + 1;
                        if committed.iter().any(|p| p.get().id == page as usize) {
                            continue;
                        }
                        let mut pre = vec![0u8; PAGE];
                        if !store.resolve_into(id, page, &mut pre).unwrap() {
                            pre = image(trunk[&(page - 1)]);
                        }
                        store.first_write_branch(id, page, &pre).unwrap();
                        generation += 1;
                        committed.push(page_with(page, generation));
                    }
                    store.commit_pages(id, &committed).unwrap();
                    store.end_write(id);
                }
                _ if !live.is_empty() => {
                    let id = live.swap_remove(rng.below(live.len() as u64) as usize);
                    if store.release_handle(id).deferred {
                        deferred += 1;
                    }
                }
                _ => {}
            }
            if step % 37 != 0 {
                continue;
            }
            let img = trunk_image(&trunk);
            let want = store.digest(&img);
            // The snapshot taken one checkpoint ago goes out now, 37 random operations later: the
            // replica must equal the store as it was at that snapshot, not as it is.
            if let Some((snap, snap_img, snap_want)) = pending.take() {
                let mut buf = Vec::new();
                let mode = if at.is_some() {
                    SendMode::IncrFix
                } else {
                    SendMode::FullFix
                };
                let delta = rng.below(2) == 0;
                let rep = send_snapshot(&snap, mode, at, &snap_img, delta, Plant::None, true, Some(&mut buf))
                    .unwrap();
                refs += rep.ref_records;
                deads += rep.dead_records;
                let work = replica.receive(&mut &buf[..], &mut rtrunk, &mut at, false).unwrap();
                trie += work.trie_inserts;
                fork_deltas += work.fork_deltas;
                streams += 1;
                store.forget_tombstones(at.unwrap());
                assert_eq!(
                    replica.digest(&rtrunk),
                    snap_want,
                    "seed {seed:#x} step {step}: the replica differs from the store at the snapshot"
                );
            }
            pending = Some((store.snapshot().unwrap().0, img.clone(), want));
            let delta = rng.below(2) == 0;
            let fresh = BranchStore::new();
            let (mut ftrunk, mut fat) = (TrunkImage::empty(PAGE), None);
            let mut full = Vec::new();
            store
                .send(SendMode::FullFix, None, &img, !delta, Plant::None, true, Some(&mut full))
                .unwrap();
            fresh.receive(&mut &full[..], &mut ftrunk, &mut fat, false).unwrap();
            assert_eq!(fresh.digest(&ftrunk), want, "seed {seed:#x} step {step}: full replica");
            let (mut a, mut c) = (vec![0u8; PAGE], vec![0u8; PAGE]);
            for &id in &live {
                for page in 1..=PAGES {
                    let sa = store.resolve_into(id, page, &mut a).unwrap();
                    let sc = fresh.resolve_into(id, page, &mut c).unwrap();
                    assert_eq!(sa, sc, "seed {seed:#x} step {step}");
                    if sa {
                        assert!(a == c, "seed {seed:#x} step {step}: branch {} page {page}", id.0);
                    }
                }
            }
        }
        assert!(
            streams > 50 && refs > 0 && deads > 0 && deferred > 0 && trie > 0 && fork_deltas > 0,
            "seed {seed:#x}: streams {streams}, refs {refs}, tombstones shipped {deads}, deferred \
             reaps {deferred}, derived trie inserts {trie}, fork deltas decoded {fork_deltas}"
        );
    }

    /// The receiver's checks must fail on a planted defect — a flipped payload byte (digest), a
    /// dropped tombstone (digest), a corrupted reference hash (refused at the reference) — and the
    /// same stream unplanted must be accepted and equal: without that control a receiver that
    /// refused everything would pass.
    #[test]
    fn a_planted_defect_is_caught_and_the_unplanted_stream_is_not() {
        for plant in [Plant::FlipPayload, Plant::DropTombstone, Plant::BadRefHash] {
            assert!(scenario(plant), "{plant:?} went unnoticed");
            assert!(!scenario(Plant::None), "the unplanted stream was refused or differs");
        }
    }

    /// Returns whether the replica refused the incremental stream or ended up different.
    fn scenario(plant: Plant) -> bool {
        let store = BranchStore::new();
        store.enable_shipping().unwrap();
        let mut trunk: HashMap<u32, u64> = (0..PAGES).map(|p| (p, 0)).collect();
        let a = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
        let doomed = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
        store.begin_write(a).unwrap();
        store.first_write_branch(a, 1, &image(0)).unwrap();
        store.commit_pages(a, &[page_with(1, 7)]).unwrap();
        store.end_write(a);
        let replica = BranchStore::new();
        let (mut rtrunk, mut at) = (TrunkImage::empty(PAGE), None);
        let mut buf = Vec::new();
        store
            .send(SendMode::FullFix, None, &trunk_image(&trunk), false, Plant::None, true, Some(&mut buf))
            .unwrap();
        replica.receive(&mut &buf[..], &mut rtrunk, &mut at, false).unwrap();
        // After the base: a death, a trunk overwrite that retains a pre-image the replica holds (a
        // reference, pinned by `a`), and a fresh branch with a page of its own.
        store.release_handle(doomed);
        store.first_write_trunk(2, &image(trunk[&1]));
        trunk.insert(1, 99);
        let keep = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
        store.begin_write(keep).unwrap();
        store.first_write_branch(keep, 3, &image(0)).unwrap();
        store.commit_pages(keep, &[page_with(3, 8)]).unwrap();
        store.end_write(keep);
        let img = trunk_image(&trunk);
        let mut buf = Vec::new();
        let rep = store
            .send(SendMode::IncrFix, at, &img, false, plant, true, Some(&mut buf))
            .unwrap();
        let dead_expected = if plant == Plant::DropTombstone { 0 } else { 1 };
        assert_eq!((rep.ref_records, rep.dead_records), (1, dead_expected), "{plant:?}: {rep:?}");
        match replica.receive(&mut &buf[..], &mut rtrunk, &mut at, false) {
            Err(_) => true,
            Ok(_) => replica.digest(&rtrunk) != store.digest(&img),
        }
    }

    /// A committed page holding exactly `bytes`.
    fn page_from(page: u32, bytes: &[u8]) -> PageRef {
        let p = Arc::new(crate::storage::pager::Page::new(i64::from(page)));
        let buffer = Arc::new(crate::Buffer::new_temporary(PAGE));
        buffer.as_mut_slice().copy_from_slice(bytes);
        p.get().buffer = Some(buffer);
        p
    }

    /// Commit `bytes` as `page` on branch `id` in one write transaction.
    fn write_page(store: &BranchStore, id: crate::branch::BranchId, page: u32, pre: &[u8], bytes: &[u8]) {
        store.begin_write(id).unwrap();
        store.first_write_branch(id, page, pre).unwrap();
        store.commit_pages(id, &[page_from(page, bytes)]).unwrap();
        store.end_write(id);
    }

    /// The stream's writer: records whether the store lock was free at each write, and, the first
    /// time it is, changes the store under the running send (a fork that writes, a reap, and an
    /// in-place rewrite of the page the send serialises last).
    struct Probe<'a> {
        store: &'a BranchStore,
        buf: Vec<u8>,
        free: u64,
        held: u64,
        mutated: bool,
        victim: crate::branch::BranchId,
        last: crate::branch::BranchId,
    }

    impl Write for Probe<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            match self.store.inner.try_lock() {
                None => self.held += 1,
                Some(guard) => {
                    drop(guard);
                    self.free += 1;
                    if !self.mutated {
                        self.mutated = true;
                        let d = self.store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
                        write_page(self.store, d, 3, &image(0), &image(4242));
                        self.store.release_handle(self.victim);
                        write_page(self.store, self.last, 1, &image(0), &image(9999));
                    }
                }
            }
            self.buf.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// R1 (PREREG A5, F-S1): a send holds no store lock while it serialises, and ships the state
    /// it started from even though the store changes under it. The stream is made larger than the
    /// sink's 1 MiB buffer so that the writer runs while records are still being produced.
    #[test]
    fn a_send_runs_without_the_store_lock_and_ships_the_state_it_started_from() {
        let store = BranchStore::new();
        store.enable_shipping().unwrap();
        let trunk: HashMap<u32, u64> = (0..PAGES).map(|p| (p, 0)).collect();
        let mut ids = Vec::new();
        // 3,000 states × 6 pages of 64 B: about 1.4 MB of slot records.
        for k in 0..3000u64 {
            let id = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
            for page in 1..=PAGES {
                write_page(&store, id, page, &image(0), &image(10_000 + k * 8 + u64::from(page)));
            }
            ids.push(id);
        }
        let img = trunk_image(&trunk);
        let want = store.digest(&img);
        let mut probe = Probe {
            store: &store,
            buf: Vec::new(),
            free: 0,
            held: 0,
            mutated: false,
            victim: ids[0],
            last: *ids.last().unwrap(),
        };
        store
            .send(SendMode::FullFix, None, &img, false, Plant::None, true, Some(&mut probe))
            .unwrap();
        let (free, held, mutated, buf) = (probe.free, probe.held, probe.mutated, probe.buf);
        assert!(
            held == 0 && free >= 2 && mutated,
            "the send held the store lock at {held} of {} writes",
            free + held
        );
        let replica = BranchStore::new();
        let (mut rtrunk, mut at) = (TrunkImage::empty(PAGE), None);
        replica.receive(&mut &buf[..], &mut rtrunk, &mut at, false).unwrap();
        assert_eq!(
            replica.digest(&rtrunk),
            want,
            "the stream carried changes made after the send began"
        );
    }

    /// R2 (PREREG A5, the refuter's point): with deltas on, every branch page travels as a
    /// run-diff against its FORK BASE, and the replica decodes it. The trunk rewrites the page after
    /// the fork, so the same-path base (the trunk's current page) is all different and only the fork
    /// base is close.
    #[test]
    fn a_delta_stream_encodes_each_page_against_its_fork_base_and_the_replica_decodes_it() {
        let store = BranchStore::new();
        store.enable_shipping().unwrap();
        // Trunk pages as explicit bytes; image page 2 is `tpages[1]`.
        let mut tpages: Vec<Vec<u8>> = (0..PAGES).map(|p| image(u64::from(p) + 1)).collect();
        for k in 0..20u8 {
            let id = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
            let mut bytes = tpages[1].clone();
            bytes[5] = k;
            bytes[40] = k ^ 0x55;
            write_page(&store, id, 2, &tpages[1], &bytes);
            // The trunk edits page 2 after this fork; the child keeps the version it forked from.
            store.first_write_trunk(2, &tpages[1]);
            tpages[1][20] = k.wrapping_add(100);
            tpages[1][60] = k;
        }
        let img = TrunkImage::new(PAGE, tpages.concat());
        let want = store.digest(&img);
        let mut buf = Vec::new();
        let rep = store
            .send(SendMode::FullFix, None, &img, true, Plant::None, true, Some(&mut buf))
            .unwrap();
        assert_eq!(
            rep.payload_shipped, rep.payload_delta_fork,
            "the stream's payloads are not the fork-base deltas: {rep:?}"
        );
        assert!(rep.payload_delta_fork < rep.payload_raw / 2, "{rep:?}");
        let replica = BranchStore::new();
        let (mut rtrunk, mut at) = (TrunkImage::empty(PAGE), None);
        replica.receive(&mut &buf[..], &mut rtrunk, &mut at, false).unwrap();
        assert_eq!(replica.digest(&rtrunk), want, "the replica decoded the deltas wrongly");
    }

    /// R3 (PREREG A5, F-S2 bound; green at the base too, see A5): after one state changes among
    /// K, an incremental send's visits are bounded by a constant, not by K.
    #[test]
    fn an_incremental_after_one_change_among_many_states_visits_a_bounded_number_of_things() {
        for k in [200u64, 2000] {
            let store = BranchStore::new();
            store.enable_shipping().unwrap();
            let trunk: HashMap<u32, u64> = (0..PAGES).map(|p| (p, 0)).collect();
            let mut ids = Vec::new();
            for i in 0..k {
                let id = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
                write_page(&store, id, 1, &image(0), &image(20_000 + i));
                ids.push(id);
            }
            let img = trunk_image(&trunk);
            let mut buf = Vec::new();
            let full = store
                .send(SendMode::FullFix, None, &img, false, Plant::None, true, Some(&mut buf))
                .unwrap();
            write_page(&store, ids[(k / 2) as usize], 1, &image(0), &image(77));
            let rep = store
                .send(SendMode::IncrFix, Some(full.to), &img, false, Plant::None, true, None)
                .unwrap();
            let visits = rep.states_visited + rep.entries_visited + rep.slots_visited + rep.index_visited;
            assert!(visits <= 64, "k = {k}: an incremental after one change visited {visits}: {rep:?}");
            assert_eq!((rep.state_records, rep.slot_records), (1, 1), "k = {k}: {rep:?}");
        }
    }
}
