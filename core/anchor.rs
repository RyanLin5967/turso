//! r11-coherence FU (PREREG amendment 15): the Database-owned objects every connection's pager reaches, borrowed for
//! the Database's lifetime, with that lifetime carried per thread.
//!
//! Without FU, every connection clones the `Arc` of the database's IO, storage, buffer pool, branch store, shared WAL
//! state, init lock and page-1 slot into its pager and WAL, and drops them at close: two writes per object per
//! connection on each object's reference-count line, which every thread's connections share (the one-line census:
//! 8, 8, 4, 8, 4 writes per conc cycle on the buffer pool, IO, storage, branch store and WAL lines).
//!
//! With FU, the database keeps one [`Anchor`] per thread slot (built on the slot's first use), which holds the only
//! strong references a pager needs. The pager's storage, branch-store, init-lock and page-1 handles and the WAL's
//! shared-state handle are [`DbRef`]s: a pointer to the object plus a clone of the anchor's `Arc`, so the object lives
//! while any handle does, and a handle's clone and drop write the anchor's count, which only that thread's
//! connections touch (the shape of Linux's percpu_ref, whose count is per CPU; and of RadixVM's Refcache, Clements
//! et al. EuroSys 2013, which keeps per-core deltas). The IO stays an `Arc<dyn IO>` field and gets the same effect
//! from the anchor's own forwarder ([`crate::io::IoFwd`]). An anchor holds no reference to the database, so there is
//! no cycle: a database dropped while pagers live leaves their anchors, and so the objects, alive until the last of
//! them goes, as the `Arc`s did.
//!
//! Not covered: the buffer pool (a concrete `Arc<BufferPool>` the pager and WAL pass on by value to many readers),
//! and the connection's and branch handles' `Arc<Database>` (a database cannot hold its own anchor without a cycle).

use crate::bravo::BravoRwLock;
use crate::branch::store::BranchStore;
use crate::io::IO;
use crate::storage::database::DatabaseStorage;
use crate::storage::pager::Page;
use crate::storage::wal::WalFileShared;
use crate::sync::{Arc, Mutex, OnceLock};
use arc_swap::ArcSwapOption;
use crossbeam_utils::CachePadded;
use std::ptr::NonNull;

/// One thread slot's strong references to the objects a connection's pager and WAL reach. `io` is this slot's own
/// forwarder to the database's IO ([`crate::io::IoFwd`]): the pager and the WAL keep `Arc<dyn IO>` fields, and a
/// clone of the forwarder writes only the forwarder's count.
pub struct Anchor {
    pub(crate) io: Arc<dyn IO>,
    pub(crate) db_file: Arc<dyn DatabaseStorage>,
    pub(crate) branches: Arc<BranchStore>,
    pub(crate) shared_wal: Arc<BravoRwLock<WalFileShared>>,
    pub(crate) init_lock: Arc<Mutex<()>>,
    pub(crate) init_page_1: Arc<ArcSwapOption<Page>>,
}

/// The database's anchors, one per thread slot ([`crate::bravo::thread_index`]), or none without FU.
pub(crate) struct Anchors {
    slots: Box<[CachePadded<OnceLock<Arc<Anchor>>>]>,
}

impl Anchors {
    pub(crate) fn new() -> Self {
        let n = if crate::coherence::fix(crate::coherence::FIX_ANCHOR) {
            crate::bravo::THREADS
        } else {
            0
        };
        Self {
            slots: (0..n).map(|_| CachePadded::new(OnceLock::new())).collect(),
        }
    }

    /// This thread's anchor, built from `make` on the slot's first use; `None` without FU.
    pub(crate) fn get(&self, make: impl FnOnce() -> Anchor) -> Option<Arc<Anchor>> {
        if self.slots.is_empty() {
            return None;
        }
        let slot = &self.slots[crate::bravo::thread_index() % self.slots.len()];
        Some(slot.get_or_init(|| Arc::new(make())).clone())
    }
}

/// A handle on a database-owned object: its own `Arc` (without FU, or wherever one was handed in), or a pointer to
/// the anchor's `Arc` plus a clone of the anchor.
pub enum DbRef<T: ?Sized> {
    Owned(Arc<T>),
    Anchored(NonNull<Arc<T>>, Arc<Anchor>),
}

// SAFETY: an anchored handle is a shared reference to an `Arc<T>` that its own anchor clone keeps alive; it is
// `Send`/`Sync` exactly when `Arc<T>` is.
unsafe impl<T: ?Sized> Send for DbRef<T> where Arc<T>: Send {}
unsafe impl<T: ?Sized> Sync for DbRef<T> where Arc<T>: Sync {}

impl<T: ?Sized> DbRef<T> {
    /// A handle on the object `field` names in `anchor`.
    pub(crate) fn anchored(anchor: &Arc<Anchor>, field: impl FnOnce(&Anchor) -> &Arc<T>) -> Self {
        let ptr = NonNull::from(field(&**anchor));
        DbRef::Anchored(ptr, anchor.clone())
    }

    /// The object's own `Arc`, for a caller that keeps it beyond this handle (a shared count write).
    pub fn to_arc(&self) -> Arc<T> {
        match self {
            DbRef::Owned(a) => a.clone(),
            // SAFETY: the anchor clone held by this handle keeps the anchor, and so the `Arc` it points into, alive.
            DbRef::Anchored(p, _) => unsafe { p.as_ref() }.clone(),
        }
    }

    /// The object's address, as `Arc::as_ptr` would give it.
    pub fn as_ptr(&self) -> *const T {
        &**self as *const T
    }
}

impl<T: ?Sized> std::ops::Deref for DbRef<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        match self {
            DbRef::Owned(a) => a,
            // SAFETY: as in `to_arc`.
            DbRef::Anchored(p, _) => unsafe { p.as_ref() },
        }
    }
}

impl<T: ?Sized> AsRef<T> for DbRef<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T: ?Sized> Clone for DbRef<T> {
    #[inline]
    fn clone(&self) -> Self {
        match self {
            DbRef::Owned(a) => DbRef::Owned(a.clone()),
            DbRef::Anchored(p, anchor) => DbRef::Anchored(*p, anchor.clone()),
        }
    }
}

impl<T: ?Sized> From<Arc<T>> for DbRef<T> {
    fn from(a: Arc<T>) -> Self {
        DbRef::Owned(a)
    }
}

impl<T: ?Sized + std::fmt::Debug> std::fmt::Debug for DbRef<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(f)
    }
}
