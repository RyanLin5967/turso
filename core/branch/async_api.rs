//! LEAP L1, the async-durable create (ASYNC-DESIGN §2 as Revision 1 amends it). The reds are
//! `core/tests/async_s0.rs` (AD-T5, PREREG-v1 amendment 2) and the lib's T-series tests.
//! - BUILT: `create_branch_async` buffers and applies its fork and returns, waiting for nothing;
//!   the flusher (`branch-async-flusher`) flies it within FLY_AGE + POLL; the housekeeping thread
//!   (`branch-housekeeping`) runs the expiry pass an async create skips (L2(a)). Both start at the
//!   store's first async create. Async branches are found, listed and connected at once, and lost
//!   visibly when their flight fails (§8).
//! - NOT YET: the id floor across a crash (§9: a lost id can be reissued after a reopen, the
//!   registered red `an_id_lost_in_a_crash_is_never_reissued`), the lag and cap fallbacks (§3),
//!   the durable-fork watermark (`durable_through` is conservative), `Released`/`LostInCrash`.
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
    /// LEAP L1: create a named branch and acknowledge it once its records are buffered and
    /// applied, before they are durable (ASYNC-DESIGN §2, §5). It is found, listed and connected at
    /// once (§8); the flusher makes it durable within about FLY_AGE + POLL plus a flight
    /// (`Database::branch_status`, `branch_wait_durable`), and so does any later flight, the
    /// branch's own first write's among them. Refused unless the store can fly it: a catalog store
    /// in a class that drains the device, not fail-stopped, with the flusher running.
    pub fn create_branch_async(self: &Arc<Self>, name: &str) -> Result<AsyncCreated> {
        let store = &self.db.branches;
        if let Some(why) = store.async_refusal() {
            return Err(crate::LimboError::InvalidArgument(why.to_string()));
        }
        store.start_async()?;
        let (id, lsn) = self.fork_with(Some(name), super::store::Ack::Async)?;
        store.note_async_ack(lsn);
        store.async_stub_mutants();
        let token = DurableToken {
            incarnation: self.db.incarnation,
            lsn,
        };
        Ok(AsyncCreated {
            id,
            token,
            // Every id at or below this is durable. Conservative until the durable-fork watermark
            // lands (ASYNC-DESIGN §4 `durable_fork_id`): this create's own id when its records are
            // durable already, else the trunk's.
            durable_through: if store.fork_status(id) == Some(true) { id } else { BranchId::TRUNK },
        })
    }
}

impl Database {
    /// Wait until the async create `token` names is durable, leading a flight if none is in the
    /// air. Refused for a token of another incarnation (`branch_token_foreign`): ask
    /// `branch_status` then.
    pub fn branch_wait_durable(&self, token: DurableToken) -> Result<()> {
        if token.incarnation != self.incarnation {
            return Err(crate::LimboError::InvalidArgument("branch_token_foreign".to_string()));
        }
        let store = &self.branches;
        store.wait_durable(token.lsn, store.sync_class())
    }

    /// Wait until everything buffered when called is durable.
    pub fn branch_sync(&self) -> Result<()> {
        let store = &self.branches;
        store.wait_durable(store.journal_lsn(), store.sync_class())
    }

    /// Where a branch stands: `NotYetDurable` while an async create's records are not durable,
    /// `Durable` once they are (and for any live branch not resident in memory: the catalog holds
    /// only durable rows), `Unknown` for an id not live. It never leads a flight. NOT YET:
    /// `Released` and `LostInCrash` (an id floor across a crash, ASYNC-DESIGN §9) read `Unknown`.
    pub fn branch_status(&self, id: BranchId) -> Result<BranchStatus> {
        match self.branches.fork_status(id) {
            Some(true) => Ok(BranchStatus::Durable),
            Some(false) => Ok(BranchStatus::NotYetDurable),
            None => Ok(if self.branch_ids()?.contains(&id) {
                BranchStatus::Durable
            } else {
                BranchStatus::Unknown
            }),
        }
    }

    /// The async counters. Measured: async creates, the flusher's wakes and flights, housekeeping
    /// passes, the branch table's inline allocations, the log buffer's growths and the group's
    /// flights. The fallbacks, back-pressure and pool refills read 0, and `measured` stays false
    /// until the rest of L1 lands (AD-T5 reads it).
    pub fn branch_async_stats(&self) -> BranchAsyncStats {
        let [async_creates, flusher_wakes, flusher_flights, housekeeping_passes] = self.branches.async_counters();
        BranchAsyncStats {
            async_creates,
            flusher_wakes,
            flusher_flights,
            housekeeping_passes,
            chunk_inline_allocs: self.branches.table_inline_allocs(),
            pending_reallocs: self.branches.log_buffer_reallocs(),
            group_flights: self.branches.group_counters()[0],
            measured: false,
            ..Default::default()
        }
    }

    /// AD-T5's hold hook for the flusher (R1.4 item 2): while held it wakes and leads nothing.
    /// `true` when the flusher runs (it starts at the store's first async create), so the hold is
    /// real.
    #[doc(hidden)]
    pub fn branch_flusher_hold(&self, on: bool) -> bool {
        self.branches.async_hold(true, on)
    }

    /// AD-T5's hold hook for the housekeeping thread, as `branch_flusher_hold`.
    #[doc(hidden)]
    pub fn branch_housekeeping_hold(&self, on: bool) -> bool {
        self.branches.async_hold(false, on)
    }

    /// Pre-provision the branch table's pool for `ids` more creates (AD-T5: "the pool
    /// pre-provisioned to >= N/1024 segments") and return how many ids it now covers. The table's
    /// D2 pool is filled to its fixed bound (LEAP L2(b)), which may cover fewer.
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
