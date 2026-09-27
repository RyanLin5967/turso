//! The branch store's own entry points, for a driver that builds worst-case inputs for its data
//! structures (`examples/branch_adv`). Not an API: `#[doc(hidden)]`, and nothing in the engine
//! calls it.
//!
//! The calls are the ones the pager makes — `fork_trunk`, `fork_branch`, `first_write_trunk`,
//! `begin_write` + `first_write_branch` + `end_write`, `release_handle`, `resolve_into` — with
//! page numbers and pre-images chosen by the caller, so that a version can be placed at a chosen
//! `(born, died)`, which the SQL path cannot do. `commit_pages` is not exposed: it copies bytes
//! into a slot the copy decision already chose and touches no index.

use super::store::BranchStore;
use super::{BranchId, BranchStats, HoldMax, Reaped};
use crate::schema::Schema;
use crate::sync::Arc;
use crate::Result;

pub struct StoreBench {
    store: BranchStore,
    image: Vec<u8>,
    schema: Arc<Schema>,
}

impl StoreBench {
    pub fn new(page_size: usize) -> Self {
        Self {
            store: BranchStore::new(),
            image: vec![0u8; page_size],
            schema: Arc::new(Schema::default()),
        }
    }

    pub fn fork_trunk(&self) -> Result<BranchId> {
        self.store.fork_trunk(self.schema.clone(), self.image.len())
    }

    pub fn fork_branch(&self, parent: BranchId) -> Result<BranchId> {
        self.store.fork_branch(parent)
    }

    /// The trunk's first write of `page` in a transaction, as the pager makes it: only while the
    /// trunk has a live child.
    pub fn trunk_write(&self, page: u32) {
        if self.store.trunk_has_children() {
            self.store.first_write_trunk(page, &self.image);
        }
    }

    /// One write transaction on branch `id` that first-writes each of `pages`.
    pub fn branch_write(&self, id: BranchId, pages: &[u32]) -> Result<()> {
        self.store.begin_write(id)?;
        for &page in pages {
            self.store.first_write_branch(id, page, &self.image)?;
        }
        self.store.end_write(id);
        Ok(())
    }

    pub fn begin_write(&self, id: BranchId) -> Result<()> {
        self.store.begin_write(id)
    }

    pub fn write_page(&self, id: BranchId, page: u32) -> Result<()> {
        self.store.first_write_branch(id, page, &self.image)
    }

    pub fn end_write(&self, id: BranchId) {
        self.store.end_write(id)
    }

    pub fn reap(&self, id: BranchId) -> Reaped {
        self.store.release_handle(id)
    }

    pub fn resolve_into(&self, id: BranchId, page: u32, out: &mut [u8]) -> Result<bool> {
        self.store.resolve_into(id, page, out)
    }

    pub fn stats(&self) -> BranchStats {
        self.store.stats()
    }

    /// The per-hold maxima since the previous call, which this call resets.
    pub fn take_hold_max(&self) -> HoldMax {
        self.store.take_hold_max()
    }

    /// Bytes of one entry of the `branches` table (key plus `BranchState`), for a space estimate.
    pub fn branch_entry_bytes() -> usize {
        BranchStore::branch_entry_bytes()
    }
}
