//! The branch store's own entry points, for a driver that builds worst-case inputs for its data
//! structures (r11-adversarial's `examples/branch_w8x` and `examples/branch_w8c`). Not an API:
//! `#[doc(hidden)]`, and nothing in the engine calls it.
//!
//! The calls are the ones the pager makes, with page numbers chosen by the caller, so that a
//! version can be placed at a chosen `(born, died)`, which the SQL path cannot do. Ported from the
//! volatile store (turso r11-adv-x-f7fix 192ee35ad) to the composed store, which differs from it in
//! three ways a driver sees:
//!
//! * The store is VOLATILE and opens in the F7 splice arm (`DatabaseOpts::with_branch_splice`): the
//!   store the cross-check was written against splices unconditionally; here the splice is an arm,
//!   off by default (r11-ever amendment 15).
//! * A branch's first write of a page only reserves the slot its commit fills (the volatile store
//!   copied the pre-image there at once), so `branch_write` and `write_page` commit too: each page
//!   is published holding zeros. The drivers measure where versions sit, not what they hold.
//! * A trunk fork is the first-child kind the pager makes under the trunk's WAL write lock, and a
//!   trunk write is a one-page trunk commit decided at once plus its barrier, as the store's model
//!   tests drive it. Errors are returned, never swallowed: `reap` and `stats` return `Result` here.

use super::store::{BranchStore, TrunkFork};
use super::{BranchDurability, BranchId, BranchStats, Reaped, SyncClass};
use crate::schema::Schema;
use crate::storage::pager::{Page, PageRef};
use crate::sync::Arc;
use crate::{Buffer, Result};

pub struct StoreBench {
    store: BranchStore,
    image: Vec<u8>,
    schema: Arc<Schema>,
}

impl StoreBench {
    /// A volatile store in the F7 splice arm whose arena pages are `page_size` bytes. Panics only
    /// if a volatile in-memory store cannot be opened, which no input here can cause.
    pub fn new(page_size: usize) -> Self {
        let store = BranchStore::open_resolved(BranchDurability::Volatile, None, true, false, ":memory:")
            .expect("a volatile in-memory branch store opens");
        Self {
            store,
            image: vec![0u8; page_size],
            schema: Arc::new(Schema::default()),
        }
    }

    /// A trunk fork under the trunk's WAL write lock (`seen: None`), which always registers.
    pub fn fork_trunk(&self) -> Result<BranchId> {
        match self
            .store
            .fork_trunk(self.schema.clone(), self.image.len(), None, None)?
        {
            TrunkFork::Forked { id, lsn, .. } => {
                self.store.wait_durable(lsn, self.store.sync_class())?;
                Ok(id)
            }
            TrunkFork::NeedsWriterLock => unreachable!("a fork under the WAL write lock always registers"),
        }
    }

    /// A branch fork, waited on until it is durable (a volatile store's is at once).
    pub fn fork_branch(&self, parent: BranchId) -> Result<BranchId> {
        let (id, lsn) = self.store.fork_branch(parent, None)?;
        self.store.wait_durable(lsn, self.store.sync_class())?;
        Ok(id)
    }

    /// The trunk's write of `page`, as a one-page trunk commit: its copy decision is taken only
    /// while the trunk has a live child (a writer that saw none captured no pre-image), then the
    /// commit's barrier.
    pub fn trunk_write(&self, page: u32) -> Result<()> {
        let required = if self.store.trunk_has_children() {
            let decided = self.store.begin_trunk_commit([(page, Some(self.image.as_slice()))]);
            self.store.end_trunk_commit();
            decided?
        } else {
            self.store.barrier_floor()
        };
        self.store.durability_barrier_to(SyncClass::Off, required)
    }

    /// One write transaction on branch `id` that first-writes each of `pages` and commits them.
    pub fn branch_write(&self, id: BranchId, pages: &[u32]) -> Result<()> {
        self.store.begin_write(id)?;
        let written = self.write_pages(id, pages);
        self.store.end_write(id);
        written
    }

    pub fn begin_write(&self, id: BranchId) -> Result<()> {
        self.store.begin_write(id)
    }

    /// One page's first write inside an open write transaction, committed at once.
    pub fn write_page(&self, id: BranchId, page: u32) -> Result<()> {
        self.write_pages(id, &[page])
    }

    pub fn end_write(&self, id: BranchId) {
        self.store.end_write(id)
    }

    pub fn reap(&self, id: BranchId) -> Result<Reaped> {
        self.store.release_handle(id)
    }

    pub fn resolve_into(&self, id: BranchId, page: u32, out: &mut [u8]) -> Result<bool> {
        self.store.resolve_into(id, page, out)
    }

    pub fn stats(&self) -> Result<BranchStats> {
        self.store.stats()
    }

    /// Each of `pages` (duplicates once) reserved by its first write, then committed holding zeros.
    fn write_pages(&self, id: BranchId, pages: &[u32]) -> Result<()> {
        let mut committed: Vec<PageRef> = Vec::with_capacity(pages.len());
        for &page in pages {
            if committed.iter().any(|p| p.get().id == page as usize) {
                continue;
            }
            self.store.first_write_branch(id, page)?;
            let p = Arc::new(Page::new(i64::from(page)));
            let buffer = Arc::new(Buffer::new_temporary(self.image.len()));
            buffer.as_mut_slice().copy_from_slice(&self.image);
            p.get().buffer = Some(buffer);
            committed.push(p);
        }
        self.store.commit_pages(id, &committed)
    }
}
