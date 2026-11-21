//! The durable store a campaign runs on.
//!
//! The runner drives any `DurableStore` through the store adapter of lot
//! L2. What differs between backends is outside the process under test:
//! how a data set is created, what survives a crash and how the next
//! start-up reopens it, how the inspector reads the durable state without
//! the broker, and what the manifest must say about the engine. A backend
//! states those four things and nothing else.

use std::path::PathBuf;

use serde_json::{Value, json};

use mqttiumd_contract::ids::StoreIncarnation;
use mqttiumd_store_api::ReadError;

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
    ) -> Result<Self::Store, ReadError>;

    /// What a crash of the process under test leaves, reopened for the
    /// next start-up: the durable state and nothing else.
    fn reopen_after_crash(&mut self, store: Self::Store) -> Result<Self::Store, ReadError>;

    /// The durable state, read independently of the broker.
    /// `store` is the store of the process when it runs in the process of
    /// the supervisor, `None` when it runs in a child or does not run.
    fn inspect(&self, store: Option<&Self::Store>) -> Result<DurableState, ReadError>;

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
    ) -> Result<MemoryStore, ReadError> {
        Ok(MemoryStore::create(incarnation, limits))
    }

    fn reopen_after_crash(&mut self, store: MemoryStore) -> Result<MemoryStore, ReadError> {
        Ok(MemoryStore::reopen(store.snapshot()?))
    }

    fn inspect(&self, store: Option<&MemoryStore>) -> Result<DurableState, ReadError> {
        store.ok_or(ReadError::Unavailable)?.snapshot()
    }

    fn describe(&self) -> Value {
        json!({"engine": "the deterministic store of the bench, in memory"})
    }

    fn crash(&self) -> &'static str {
        "simulated: the process under test is dropped and its store reopened from its durable state, in the same operating-system process"
    }
}

/// How the inspector of the SQLite backend reads the durable state.
const INSPECTOR: &str = "SqliteStore::inspect: a connection of its own opened read-only (mode=ro, never immutable), one read transaction, closed before the snapshot is written; no checkpoint; never the writer's connection";

/// The settings the SQLite engine read back when `store` was opened, as
/// the manifest records them and as a child process reports them at its
/// start-up for the supervisor to compare.
#[must_use]
pub fn sqlite_settings(store: &mqttiumd_store_sqlite::SqliteStore) -> Value {
    let settings = store.effective_settings();
    json!({
        "engine": "sqlite",
        "sqlite_version": settings.sqlite_version,
        "compile_options": settings.compile_options,
        "journal_mode": settings.journal_mode,
        "synchronous": settings.synchronous,
        "user_version": settings.user_version,
        "page_size": settings.page_size,
        "wal_autocheckpoint": settings.wal_autocheckpoint,
        "kill_points": settings.kill_points,
        "checkpoint_policy": settings.checkpoint_policy,
        "read_back": "after they were set, at each opening",
    })
}

/// The SQLite store of lot L7, in one file of the campaign.
#[derive(Clone, Debug)]
pub struct Sqlite {
    /// The file of the data set.
    pub path: PathBuf,
    /// The settings the engine read back at the last opening.
    settings: Option<Value>,
}

impl Sqlite {
    /// The store of a campaign, in `path`, which does not exist yet.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            settings: None,
        }
    }

    fn remember(&mut self, store: &mqttiumd_store_sqlite::SqliteStore) {
        self.settings = Some(sqlite_settings(store));
    }
}

impl Backend for Sqlite {
    type Store = mqttiumd_store_sqlite::SqliteStore;

    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn create(
        &mut self,
        incarnation: StoreIncarnation,
        limits: Limits,
    ) -> Result<Self::Store, ReadError> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| match error.kind() {
                std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::NotFound => {
                    ReadError::Unavailable
                }
                _ => ReadError::Io,
            })?;
        }
        let store = mqttiumd_store_sqlite::SqliteStore::create(&self.path, incarnation, limits)
            .map_err(|error| error.read_error())?;
        self.remember(&store);
        Ok(store)
    }

    fn reopen_after_crash(&mut self, store: Self::Store) -> Result<Self::Store, ReadError> {
        // The connection goes with the process; the next start-up opens
        // the same file and finds what the engine made durable.
        drop(store);
        let store = mqttiumd_store_sqlite::SqliteStore::open(&self.path)
            .map_err(|error| error.read_error())?;
        self.remember(&store);
        Ok(store)
    }

    fn inspect(&self, _store: Option<&Self::Store>) -> Result<DurableState, ReadError> {
        // Without the writer: a connection of its own, read-only, one read
        // transaction, closed before the answer; never a checkpoint.
        mqttiumd_store_sqlite::SqliteStore::inspect(&self.path)
    }

    fn describe(&self) -> Value {
        let mut settings = self.settings.clone().unwrap_or(Value::Null);
        if let Some(settings) = settings.as_object_mut() {
            settings.insert("inspector".to_owned(), json!(INSPECTOR));
        }
        settings
    }

    fn crash(&self) -> &'static str {
        "simulated: the store adapter and its connection are dropped and the same file is opened again, in the same operating-system process; no process is killed"
    }
}
