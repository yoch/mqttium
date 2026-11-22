//! File-backed handle pairing `VolatileStore` with `SqliteStore`'s exclusive
//! writer lease and atomic Schema-14 / Schema-15 snapshot writer.

use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use mqttiumd_contract::command::{Command, EpochLedger, OpenEpoch};
use mqttiumd_contract::ids::{DeliveryId, OperationId, PublicationId, SessionId, StoreIncarnation};
use mqttiumd_contract::message_expiry::MessageTime;
use mqttiumd_contract::outcome::{OpeningOutcome, StoreOutcome};
use mqttiumd_contract::receipts::{
    ClosedReceiptCollection, ClosedReceiptReply, ReceiptClosure, ReceiptCollection,
    ReceiptCollectionReply, ReceiptOrigin,
};
use mqttiumd_store_api::{
    AdminCommand, AdminDone, AdminRefused, Applied, DurableState, DurableStore, Limits,
    MessageExpirySweep, ReadBudget, ReadError, RecoveryCursor, RecoveryPage, RecoveryRefused,
    Resolved, SessionClockAnchor, SessionClockReading, WillPage, WorkPage,
};
use mqttiumd_store_sqlite::{
    OpenError, Settings, SqliteStore, StoragePolicy, WriterLease, acquire_writer_lock,
    save_state_atomic,
};

use crate::VolatileStore;

/// Owns a `VolatileStore` alongside the exclusive `<path>.writer-lock` lease
/// so that no second broker can concurrently write to `path` and periodic or
/// shutdown snapshots can be written atomically in Schema 14 / Schema 15.
pub struct VolatileHandle {
    store: VolatileStore,
    path: PathBuf,
    policy: Option<StoragePolicy>,
    sqlite_settings: Option<Settings>,
    lease: WriterLease,
}

impl std::fmt::Debug for VolatileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VolatileHandle")
            .field("path", &self.path)
            .field("secured", &self.policy.is_some())
            .finish_non_exhaustive()
    }
}

impl VolatileHandle {
    /// Opens an existing SQLite database into RAM (transferring the writer
    /// lease with no lock gap), or creates an empty `VolatileStore` under the
    /// exclusive writer lease if `path` does not yet exist.
    pub fn open_or_create(
        path: &Path,
        incarnation: StoreIncarnation,
        limits: Limits,
        policy: Option<StoragePolicy>,
        require_existing: bool,
    ) -> Result<Self, OpenError> {
        let exists = path.try_exists().map_err(OpenError::WriterLock)?;
        if exists {
            let sqlite = match policy.as_ref() {
                Some(p) => SqliteStore::open_secured(path, p)?,
                None => SqliteStore::open(path)?,
            };
            let (state, settings, lease) = sqlite.into_state_and_lease()?;
            let store = VolatileStore::reopen(state);
            Ok(Self {
                store,
                path: path.to_path_buf(),
                policy,
                sqlite_settings: Some(settings),
                lease,
            })
        } else if require_existing {
            Err(OpenError::Read(ReadError::Unavailable))
        } else {
            if let Some(p) = policy.as_ref()
                && (p.owners.is_empty() || p.owners.len() > 4_096)
            {
                return Err(OpenError::SecurityPolicyMismatch);
            }
            let lease = acquire_writer_lock(path)?;
            let store = VolatileStore::create(incarnation, limits);
            Ok(Self {
                store,
                path: path.to_path_buf(),
                policy,
                sqlite_settings: None,
                lease,
            })
        }
    }

    /// Atomically snapshots the current in-memory state to `self.path` as a
    /// valid Schema-14 (or Schema-15 when secured) SQLite database.
    pub fn save_snapshot(&self) -> Result<(), OpenError> {
        let state = self.store.snapshot().map_err(OpenError::Read)?;
        save_state_atomic(&self.path, &state, self.policy.as_ref(), &self.lease)
    }

    /// Snapshot target path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// SQLite settings from initial load, if loaded from an existing file.
    #[must_use]
    pub fn sqlite_settings(&self) -> Option<&Settings> {
        self.sqlite_settings.as_ref()
    }
}

impl Deref for VolatileHandle {
    type Target = VolatileStore;
    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl DerefMut for VolatileHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.store
    }
}

impl DurableStore for VolatileHandle {
    fn read_wills(
        &self,
        after: Option<OperationId>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<WillPage, ReadError> {
        self.store.read_wills(after, limit, budget)
    }

    fn expire_messages(
        &mut self,
        now: MessageTime,
        after: Option<PublicationId>,
        limit: u32,
    ) -> Result<MessageExpirySweep, ReadError> {
        self.store.expire_messages(now, after, limit)
    }

    fn checkpoint_session_clock(
        &mut self,
        expected: Option<SessionClockAnchor>,
        reading: SessionClockReading,
    ) -> Result<SessionClockAnchor, ReadError> {
        self.store.checkpoint_session_clock(expected, reading)
    }

    fn collect_receipts(
        &mut self,
        collection: ReceiptCollection,
    ) -> Result<ReceiptCollectionReply, ReadError> {
        self.store.collect_receipts(collection)
    }

    fn receipt_frontier(&self, origin: ReceiptOrigin) -> Result<Option<ReceiptClosure>, ReadError> {
        self.store.receipt_frontier(origin)
    }

    fn collect_closed_receipts(
        &mut self,
        collection: ClosedReceiptCollection,
    ) -> Result<ClosedReceiptReply, ReadError> {
        self.store.collect_closed_receipts(collection)
    }

    fn ledger(&self) -> Result<EpochLedger, ReadError> {
        self.store.ledger()
    }

    fn open_epoch(&mut self, command: &OpenEpoch) -> Result<OpeningOutcome, ReadError> {
        self.store.open_epoch(command)
    }

    fn apply(&mut self, command: &Command) -> Applied {
        self.store.apply(command)
    }

    fn established(&self, operation: OperationId) -> Result<Option<StoreOutcome>, ReadError> {
        self.store.established(operation)
    }

    fn resolve_and_fence(&mut self, operation: OperationId) -> Resolved {
        self.store.resolve_and_fence(operation)
    }

    fn admin(&mut self, command: AdminCommand) -> Result<AdminDone, AdminRefused> {
        self.store.admin(command)
    }

    fn recover_with_budget(
        &self,
        cursor: Option<RecoveryCursor>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<RecoveryPage, RecoveryRefused> {
        self.store.recover_with_budget(cursor, limit, budget)
    }

    fn read_work_with_budget(
        &self,
        session: SessionId,
        after: Option<DeliveryId>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<WorkPage, ReadError> {
        self.store
            .read_work_with_budget(session, after, limit, budget)
    }

    fn snapshot_with_budget(&self, budget: ReadBudget) -> Result<DurableState, ReadError> {
        self.store.snapshot_with_budget(budget)
    }
}
