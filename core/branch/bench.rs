//! The branch store's own entry points, for a driver that builds worst-case inputs for its data
//! structures (r11-adversarial's `examples/branch_w8x` and `examples/branch_w8c`). Not an API:
//! `#[doc(hidden)]`, and nothing in the engine calls it.
//!
//! The calls are the ones the pager makes — a trunk fork under the WAL write lock, `fork_branch`,
//! `first_write_trunk` plus its durability barrier, `begin_write` + `first_write_branch` +
//! `commit_pages` + `end_write`, `release_handle`, `resolve_into` — with page numbers chosen by the
//! caller, so that a version can be placed at a chosen `(born, died)`, which the SQL path cannot do.
//!
//! Ported (resolve-vol-bushy-ever, merging r11-adv-x-f7fix) from the volatile store it was written
//! for, whose F7 splice was unconditional and whose first write copied the pre-image into the
//! branch's map. Here: the store is VOLATILE and opened in the F7 SPLICE ARM
//! (`DatabaseOpts::with_branch_splice`, off by default), the store the cross-check of the
//! `inherited_at` fix targets; a branch's first write only RESERVES a slot and its commit moves the
//! map, so a write is visible (and indexed) once [`StoreBench::commit`] has run, which
//! [`StoreBench::branch_write`] does; and the store's calls return `Result`.

use super::store::{BranchStore, TrunkFork};
use super::{BranchDurability, BranchId, BranchStats, Reaped, SyncClass};
use crate::schema::Schema;
use crate::storage::pager::{Page, PageRef};
use crate::sync::Arc;
use crate::Result;

pub struct StoreBench {
    store: BranchStore,
    image: Vec<u8>,
    schema: Arc<Schema>,
}

impl StoreBench {
    /// A volatile store in the F7 splice arm, for pages of `page_size` bytes.
    pub fn new(page_size: usize) -> Result<Self> {
        Ok(Self {
            store: BranchStore::open_mode(BranchDurability::Volatile, None, true, ":memory:")?,
            image: vec![0u8; page_size],
            schema: Arc::new(Schema::default()),
        })
    }

    /// A trunk fork as the pager makes it under the trunk's WAL write lock: it always registers.
    pub fn fork_trunk(&self) -> Result<BranchId> {
        match self
            .store
            .fork_trunk(self.schema.clone(), self.image.len(), None, None)?
        {
            TrunkFork::Forked { id, lsn, .. } => {
                self.store.wait_durable(lsn, self.store.sync_class())?;
                Ok(id)
            }
            TrunkFork::NeedsWriterLock => {
                unreachable!("a fork under the WAL write lock always registers")
            }
        }
    }

    pub fn fork_branch(&self, parent: BranchId) -> Result<BranchId> {
        let (id, lsn) = self.store.fork_branch(parent, None)?;
        self.store.wait_durable(lsn, self.store.sync_class())?;
        Ok(id)
    }

    /// The trunk's first write of `page` in a transaction, as the pager makes it: its pre-image is
    /// captured only while the trunk has a live child, and the commit's barrier makes what it
    /// retained durable before the commit's first frame.
    pub fn trunk_write(&self, page: u32) -> Result<()> {
        let required = if self.store.trunk_has_children() {
            self.store.first_write_trunk(page, &self.image)?
        } else {
            self.store.barrier_floor()
        };
        self.store.durability_barrier_to(SyncClass::Off, required)
    }

    /// One write transaction on branch `id` that writes each of `pages` and commits them.
    pub fn branch_write(&self, id: BranchId, pages: &[u32]) -> Result<()> {
        self.store.begin_write(id)?;
        for &page in pages {
            self.store.first_write_branch(id, page)?;
        }
        let committed = self.commit(id, pages);
        self.store.end_write(id);
        committed
    }

    pub fn begin_write(&self, id: BranchId) -> Result<()> {
        self.store.begin_write(id)
    }

    /// The copy decision of `page`'s first write in the open transaction: a slot reserved, not yet
    /// visible.
    pub fn write_page(&self, id: BranchId, page: u32) -> Result<()> {
        self.store.first_write_branch(id, page)
    }

    /// Commit the open transaction's `pages` (each written with `write_page` first), holding the
    /// bench's page image: the branch's map moves to their slots.
    pub fn commit(&self, id: BranchId, pages: &[u32]) -> Result<()> {
        let refs: Vec<PageRef> = pages.iter().map(|&page| self.page_ref(page)).collect();
        self.store.commit_pages(id, &refs)
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

    /// A committed page holding the bench's image, as the pager hands one to `commit_pages`.
    fn page_ref(&self, page: u32) -> PageRef {
        let p = Arc::new(Page::new(i64::from(page)));
        let buffer = Arc::new(crate::Buffer::new_temporary(self.image.len()));
        buffer.as_mut_slice().copy_from_slice(&self.image);
        p.get().buffer = Some(buffer);
        p
    }
}
