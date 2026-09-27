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
//! U2 (amendment 20) adds the buffer pool (the pager's and the WAL's handles; `to_arc()` where a page read keeps an
//! Arc) and a per-thread copy of the builtin symbol maps. Not covered: the connection's and branch handles'
//! `Arc<Database>` (a database cannot hold its own anchor without a cycle).

use crate::bravo::BravoRwLock;
use crate::branch::store::BranchStore;
use crate::io::IO;
use crate::storage::buffer_pool::BufferPool;
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
    /// U2 (amendment 20): the buffer pool, for the pager's and the WAL's handles.
    pub(crate) buffer_pool: Arc<BufferPool>,
    /// U2: this thread slot's own deep copy of the builtin symbol maps, and the builtin generation it copied
    /// ([`crate::database::BuiltinSyms`]); a connect extends from it only while the generation still matches.
    pub(crate) builtins: OnceLock<(u64, crate::connection::SymbolTable)>,
    /// FZ (amendment 21): the schema this slot copied from (kept, so its address is never reused) and this thread's
    /// own deep copy of it; a branch whose schema is that source gets the copy.
    pub(crate) schema: OnceLock<Option<(Arc<crate::schema::Schema>, Arc<crate::schema::Schema>)>>,
    pub(crate) branches: Arc<BranchStore>,
    pub(crate) shared_wal: Arc<BravoRwLock<WalFileShared>>,
    pub(crate) init_lock: Arc<Mutex<()>>,
    pub(crate) init_page_1: Arc<ArcSwapOption<Page>>,
}

/// FZ (U-ARC, amendment 21): the one strong reference to a database that this thread's handles share. The database
/// cannot hold it (that would be a cycle), so a thread-local table holds it weakly: the keeper lives while any handle
/// does, and a handle's clone and drop write only the keeper's count.
pub(crate) struct DbKeeper {
    db: Arc<crate::Database>,
}

thread_local! {
    static DB_KEEPERS: std::cell::RefCell<Vec<(usize, crate::sync::Weak<DbKeeper>)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// A handle on `db` for a connection or a branch: through this thread's keeper with FZ, else its own `Arc`.
pub(crate) fn database_handle(db: &Arc<crate::Database>) -> DbRef<crate::Database> {
    if !crate::coherence::fix(crate::coherence::FIX_UARC) {
        // The Arc<Database> clone and its drop.
        crate::coherence::bump(crate::coherence::Class::DbArc, 2);
        return DbRef::Owned(db.clone());
    }
    let key = Arc::as_ptr(db) as usize;
    let keeper = DB_KEEPERS.with(|k| {
        let mut k = k.borrow_mut();
        k.retain(|(_, w)| w.strong_count() > 0);
        if let Some(live) = k.iter().find(|(p, _)| *p == key).and_then(|(_, w)| w.upgrade()) {
            return live;
        }
        // The keeper's one Arc<Database> clone, once per keeper.
        crate::coherence::bump(crate::coherence::Class::DbArc, 2);
        let keeper = Arc::new(DbKeeper { db: db.clone() });
        k.push((key, Arc::downgrade(&keeper)));
        keeper
    });
    DbRef::Anchored(NonNull::from(&keeper.db), keeper)
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

    /// FZ: whether `s` is some thread slot's deep copy of the schema `src` (read-only walk of the slots).
    pub(crate) fn is_copy_of(
        &self,
        src: &Arc<crate::schema::Schema>,
        s: &Arc<crate::schema::Schema>,
    ) -> bool {
        self.slots.iter().any(|slot| {
            slot.get()
                .and_then(|a| a.schema.get())
                .and_then(|entry| entry.as_ref())
                .is_some_and(|(src2, copy2)| Arc::ptr_eq(src2, src) && Arc::ptr_eq(copy2, s))
        })
    }

    /// FZ: the source schema of which `s` is some thread slot's copy, if any.
    pub(crate) fn source_of(&self, s: &Arc<crate::schema::Schema>) -> Option<Arc<crate::schema::Schema>> {
        self.slots.iter().find_map(|slot| {
            slot.get()
                .and_then(|a| a.schema.get())
                .and_then(|entry| entry.as_ref())
                .filter(|(_, copy)| Arc::ptr_eq(copy, s))
                .map(|(src, _)| src.clone())
        })
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
    /// A pointer to an `Arc<T>` that the second field keeps alive: an [`Anchor`], or a [`DbKeeper`].
    Anchored(NonNull<Arc<T>>, Arc<dyn std::any::Any + Send + Sync>),
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

    /// The object's own `Arc`, borrowed (no count written): the handle's own, or the anchor's.
    pub fn as_arc(&self) -> &Arc<T> {
        match self {
            DbRef::Owned(a) => a,
            // SAFETY: as in `to_arc`.
            DbRef::Anchored(p, _) => unsafe { p.as_ref() },
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
