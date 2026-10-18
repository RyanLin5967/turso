//! LEAP L1, the async-durable create (ASYNC-DESIGN §2 as Revision 1 amends it): STUBS ONLY.
//!
//! The stubs land first, so the reds compile and fail by assertion (§2: "each delegates to the
//! durable path", the 35f845259 pattern). The reds are `core/tests/async_s0.rs` (AD-T5, PREREG-v1
//! amendment 2) and the lib's T-series tests.
//! - `create_branch_async` creates durably (D2) and acknowledges after the flight, as
//!   `create_branch` does. Its creating thread therefore flushes and writes, and AD-T5's readings
//!   say so.
//! - No flusher and no housekeeping thread exists. The hold hooks report that nothing was held, so
//!   every arm whose premise is a held thread is void.
//! - The pool hook reports the ids the table's D2 pool covers (LEAP L2(b), 1,024), so an arm
//!   needing a deeper pool is void.
//! - The counters L2(c) and L1 bring (`pending_reallocs`, `pool_refills`, the flusher's) read 0.
//!   They are not measured yet, and `measured` says which ones are.
//!
//! The three `fe-mutants` seams the AD-T5 fire-checks name live here, so the stub's create path
//! carries them: `create_path_pwrite`, `create_path_fullfsync` and `record_alloc_on_create`.
//! Each acts once per async create, on the creating thread, on a scratch file in the store's own
//! directory, so under AD-T5's trace root.

use super::BranchId;
use crate::sync::Arc;
use crate::{Connection, Database, Result};

/// What an async create's caller holds to ask about its durability later (opaque).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurableToken {
    incarnation: u64,
    lsn: u64,
}

/// An async create's acknowledgement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AsyncCreated {
    pub id: BranchId,
    pub token: DurableToken,
    /// Every id at or below this is durable (or LostInCrash in a later incarnation).
    pub durable_through: BranchId,
}

/// Where a branch stands (`Database::branch_status`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BranchStatus {
    Durable,
    NotYetDurable,
    Released,
    LostInCrash,
    Unknown,
}

/// `BranchAsyncStats::async_fallbacks`, by cause.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AsyncFallbacks {
    pub lag: u64,
    pub id_floor: u64,
    pub cap: u64,
    pub cold: u64,
}

/// The async machinery's counters (ASYNC-DESIGN §2, R1.2, R1.7, R1.17).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BranchAsyncStats {
    pub async_creates: u64,
    pub async_fallbacks: AsyncFallbacks,
    pub fz_backpressure_waits: u64,
    pub fz_backpressure_ns: u64,
    pub flusher_wakes: u64,
    pub flusher_flights: u64,
    pub housekeeping_passes: u64,
    /// Branch-table chunks and segments a create built itself, the pool being empty (LEAP L2(b)).
    pub chunk_inline_allocs: u64,
    pub pool_refills: u64,
    pub pending_reallocs: u64,
    /// The group's flights, every `take_flight` caller's (`group_counters` flights): AD-T5's take
    /// count, whose parity its green reads.
    pub group_flights: u64,
    /// Whether the async machinery these counters describe exists in this build: `false` while
    /// only the stubs do, so a 0 here says "not measured", never "none happened".
    pub measured: bool,
}

impl Connection {
    /// STUB (LEAP L1): a durable create, acknowledged after its flight, as `create_branch`.
    pub fn create_branch_async(self: &Arc<Self>, name: &str) -> Result<AsyncCreated> {
        let id = self.create_branch(name)?;
        self.db.branches.async_stub_mutants();
        Ok(AsyncCreated {
            id,
            token: DurableToken {
                incarnation: self.db.incarnation,
                lsn: 0,
            },
            durable_through: id,
        })
    }
}

impl Database {
    /// STUB (LEAP L1): nothing is ever left to wait for, since every stub create waited.
    pub fn branch_wait_durable(&self, token: DurableToken) -> Result<()> {
        let _ = token.lsn;
        if token.incarnation != self.incarnation {
            return Err(crate::LimboError::InvalidArgument("branch_token_foreign".to_string()));
        }
        Ok(())
    }

    /// STUB (LEAP L1): as `branch_wait_durable`.
    pub fn branch_sync(&self) -> Result<()> {
        Ok(())
    }

    /// STUB (LEAP L1): a live branch is durable (every stub create waited), any other Unknown.
    pub fn branch_status(&self, id: BranchId) -> Result<BranchStatus> {
        Ok(if self.branch_ids()?.contains(&id) {
            BranchStatus::Durable
        } else {
            BranchStatus::Unknown
        })
    }

    /// The async counters: the branch table's inline allocations, the log buffer's growths and the
    /// group's flights are measured; the rest read 0 and `measured` is false until L1 lands.
    pub fn branch_async_stats(&self) -> BranchAsyncStats {
        BranchAsyncStats {
            chunk_inline_allocs: self.branches.table_inline_allocs(),
            pending_reallocs: self.branches.log_buffer_reallocs(),
            group_flights: self.branches.group_counters()[0],
            measured: false,
            ..Default::default()
        }
    }

    /// AD-T5's hold hook for the flusher (R1.4 item 2): `true` when held. STUB: no flusher
    /// exists, so nothing is held, and an arm that needs one held is void.
    #[doc(hidden)]
    pub fn branch_flusher_hold(&self, on: bool) -> bool {
        let _ = on;
        false
    }

    /// AD-T5's hold hook for the housekeeping thread. STUB: none exists, as above.
    #[doc(hidden)]
    pub fn branch_housekeeping_hold(&self, on: bool) -> bool {
        let _ = on;
        false
    }

    /// Pre-provision the branch table's pool for `ids` more creates (AD-T5: "the pool
    /// pre-provisioned to >= N/1024 segments") and return how many ids it now covers. STUB: the
    /// table's D2 pool is filled to its fixed bound (LEAP L2(b)), which may cover fewer.
    #[doc(hidden)]
    pub fn branch_pool_provision(&self, ids: u64) -> u64 {
        let _ = ids;
        self.branches.provision_table_pool()
    }
}

/// Whether this build carries the `fe-mutants` feature (PREREG-v1 amendment 2): printed with every
/// AD-T5 reading, since a reading binary built with it is void.
#[doc(hidden)]
pub fn fe_mutants_compiled() -> bool {
    cfg!(feature = "fe-mutants")
}
