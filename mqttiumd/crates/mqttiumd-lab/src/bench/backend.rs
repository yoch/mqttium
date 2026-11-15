//! The durable store a campaign runs on.
//!
//! The runner drives any `DurableStore` through the store adapter of lot
//! L2. What differs between backends is outside the process under test:
//! how a data set is created, what survives a crash and how the next
//! start-up reopens it, how the inspector reads the durable state without
//! the broker, and what the manifest must say about the engine. A backend
//! states those four things and nothing else.

use serde_json::{Value, json};

use mqttiumd_contract::ids::StoreIncarnation;

use crate::store::durable::{DurableState, DurableStore, Limits, MemoryStore};

/// A durable store the runner can play a campaign on.
pub trait Backend {
    type Store: DurableStore;

    /// The name of the backend in the trace header, the snapshots and
    /// the run manifest (`sim-store` or `sqlite`).
    fn name(&self) -> &'static str;

    /// A new, empty data set: no session, no epoch open.
    fn create(
        &mut self,
        incarnation: StoreIncarnation,
        limits: Limits,
    ) -> Result<Self::Store, String>;

    /// What a crash of the process under test leaves, reopened for the
    /// next start-up: the durable state and nothing else.
    fn reopen_after_crash(&mut self, store: Self::Store) -> Result<Self::Store, String>;

    /// The durable state, read independently of the broker.
    fn inspect(&self, store: &Self::Store) -> Result<DurableState, String>;

    /// What the campaign states about the engine: for SQLite its exact
    /// version, compile options and the settings read back (DEM-011).
    fn describe(&self) -> Value;

    /// What a crash is on this backend, for the coverage of a campaign.
    fn crash(&self) -> &'static str;
}

/// The deterministic store of the bench, in memory (lot L2).
#[derive(Clone, Copy, Debug, Default)]
pub struct Memory;

impl Backend for Memory {
    type Store = MemoryStore;

    fn name(&self) -> &'static str {
        "sim-store"
    }

    fn create(
        &mut self,
        incarnation: StoreIncarnation,
        limits: Limits,
    ) -> Result<MemoryStore, String> {
        Ok(MemoryStore::create(incarnation, limits))
    }

    fn reopen_after_crash(&mut self, store: MemoryStore) -> Result<MemoryStore, String> {
        Ok(MemoryStore::reopen(store.snapshot()))
    }

    fn inspect(&self, store: &MemoryStore) -> Result<DurableState, String> {
        Ok(store.snapshot())
    }

    fn describe(&self) -> Value {
        json!({"engine": "the deterministic store of the bench, in memory"})
    }

    fn crash(&self) -> &'static str {
        "simulated: the process under test is dropped and its store reopened from its durable state, in the same operating-system process"
    }
}
