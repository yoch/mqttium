//! Exclusive SQLite worker. A closed socket never cancels an accepted command.
use crate::events::{Events, operation};
use crate::session_time::{SessionClock, SystemClock};
use mqttiumd_contract::capacity::{ExchangeSlots, PayloadBytes, ReceiptSlots, References};
use mqttiumd_contract::command::{Recipe, StartupNonce};
use mqttiumd_contract::effects::{Effect, OwnedEffect};
use mqttiumd_contract::ids::{
    ConnectionGeneration, EffectId, OperationId, Revision, SessionId, StoreIncarnation,
};
use mqttiumd_contract::message_expiry::MessageTime;
use mqttiumd_contract::outcome::{
    EpochGrant, EpochOpening, OpeningReply, OpeningStage, StoreReply,
};
use mqttiumd_contract::packet::SharedBytes;
use mqttiumd_contract::receipts::{
    ClosedReceiptCollection, ClosedReceiptReply, ReceiptCollection, ReceiptCollectionReply,
};
use mqttiumd_contract::session::{SessionAction, SessionDetachment, SessionExpiry};
use mqttiumd_store_api::{
    AdminCommand, AdminDone, DurableStore, Limits, ReadBudget, ReadError, ReadPhase,
    RecoveryCursor, RecoveryPage, RecoveryRefused, SessionClockAnchor, SessionClockReading,
    SessionState, WillPage, WorkPage,
};
use mqttiumd_store_sqlite::SqliteStore;
use serde_json::json;
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc::{self, Receiver, TryRecvError, TrySendError},
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Storage durability profile chosen explicitly by the operator (ADR 0052,
/// ARC-020, DUR-001, DUR-002). Defaults to `Durable`; never degrades silently.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StorageMode {
    /// SQLite WAL + `synchronous=FULL` (every commit fsynced before reply).
    #[default]
    Durable,
    /// Log-first durable store: CRC-32C-framed append-only journal (`.mlog`)
    /// with per-group `fdatasync` before reply + indexed in-memory state +
    /// periodic and shutdown atomic Schema-14 / Schema-15 SQLite checkpoints.
    Log,
    /// Indexed in-memory `VolatileStore` with optional periodic atomic SQLite
    /// snapshots. Never implicitly substitutes for `Durable`.
    Volatile {
        /// Periodic atomic snapshot interval; `None` disables snapshots (pure RAM).
        snapshot_interval: Option<Duration>,
    },
}

impl StorageMode {
    #[must_use]
    pub const fn mode_name(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::Log => "log",
            Self::Volatile { .. } => "volatile",
        }
    }

    #[must_use]
    pub const fn durability_guarantee(self) -> &'static str {
        match self {
            Self::Durable => "per_commit_fsync_wal_full",
            Self::Log => "per_commit_fdatasync_append_log",
            Self::Volatile {
                snapshot_interval: Some(_),
            } => "periodic_atomic_snapshot",
            Self::Volatile {
                snapshot_interval: None,
            } => "none_in_memory_only",
        }
    }

    #[must_use]
    pub const fn snapshot_interval_secs(self) -> u64 {
        match self {
            Self::Durable | Self::Log => 0,
            Self::Volatile { snapshot_interval } => match snapshot_interval {
                Some(duration) => duration.as_secs(),
                None => 0,
            },
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum StoreBackend {
    Durable(SqliteStore),
    Log(mqttiumd_store_volatile::LogStore),
    Volatile {
        handle: mqttiumd_store_volatile::VolatileHandle,
        snapshot_interval: Option<Duration>,
        last_snapshot: Instant,
        dirty: bool,
    },
}

impl StoreBackend {
    #[inline]
    fn has_expiring_publications(&self) -> bool {
        match self {
            Self::Durable(_) => true,
            Self::Log(s) => s.store().has_expiring_publications(),
            Self::Volatile { handle, .. } => handle.has_expiring_publications(),
        }
    }

    fn has_will_for_client(&self, client: &[u8]) -> Result<bool, ReadError> {
        match self {
            Self::Durable(s) => s.has_will_for_client(client),
            Self::Log(s) => s.has_will_for_client(client),
            Self::Volatile { handle, .. } => handle.has_will_for_client(client),
        }
    }

    #[inline]
    fn apply_owned(
        &mut self,
        command: mqttiumd_contract::command::Command,
    ) -> mqttiumd_store_api::Applied {
        match self {
            Self::Durable(s) => s.apply(&command),
            Self::Log(s) => s.apply(&command),
            Self::Volatile { handle, dirty, .. } => {
                let res = handle.apply_owned(command);
                if res.wrote {
                    *dirty = true;
                }
                res
            }
        }
    }

    fn apply_group(
        &mut self,
        commands: &[&mqttiumd_contract::command::Command],
    ) -> Result<Vec<mqttiumd_store_api::Applied>, mqttiumd_store_sqlite::GroupRefused> {
        match self {
            Self::Durable(s) => s.apply_group(commands),
            Self::Log(s) => s.apply_group(commands),
            Self::Volatile { handle, dirty, .. } => {
                let res = handle.apply_group(commands)?;
                if res.iter().any(|r| r.wrote) {
                    *dirty = true;
                }
                Ok(res)
            }
        }
    }

    fn maybe_snapshot(&mut self, events: &Events, force: bool) -> Result<(), Failure> {
        match self {
            Self::Durable(_) => Ok(()),
            Self::Log(store) => {
                if !force {
                    return Ok(());
                }
                match store.compact() {
                    Ok(()) => {
                        let _ =
                            events.emit("log_checkpoint_saved", json!({"details":{"forced":true}}));
                        Ok(())
                    }
                    Err(error) => {
                        let read_err = error.read_error();
                        let _ = events.emit(
                            "log_checkpoint_failed",
                            json!({"details":{"cause":read_err.code(),"forced":true}}),
                        );
                        Err(Failure::read(read_err, ReadPhase::Snapshot))
                    }
                }
            }
            Self::Volatile {
                handle,
                snapshot_interval: Some(interval),
                last_snapshot,
                dirty,
            } => {
                if !*dirty || (!force && last_snapshot.elapsed() < *interval) {
                    return Ok(());
                }
                match handle.save_snapshot() {
                    Ok(()) => {
                        *last_snapshot = Instant::now();
                        *dirty = false;
                        let _ = events.emit(
                            "volatile_snapshot_saved",
                            json!({"details":{"forced":force}}),
                        );
                        Ok(())
                    }
                    Err(error) => {
                        *last_snapshot = Instant::now();
                        let read_err = error.read_error();
                        let _ = events.emit(
                            "volatile_snapshot_failed",
                            json!({"details":{"cause":read_err.code(),"forced":force}}),
                        );
                        Err(Failure::read(read_err, ReadPhase::Snapshot))
                    }
                }
            }
            Self::Volatile {
                snapshot_interval: None,
                ..
            } => Ok(()),
        }
    }

    #[cfg(feature = "test-hooks")]
    fn set_kill_hook(
        &mut self,
        hook: Option<mqttiumd_store_sqlite::kill_points::Hook>,
    ) -> Result<(), mqttiumd_store_sqlite::OpenError> {
        match self {
            Self::Durable(s) => s.set_kill_hook(hook),
            Self::Log(_) | Self::Volatile { .. } => Ok(()),
        }
    }

    #[cfg(feature = "test-hooks")]
    fn set_receipt_hook(&mut self, hook: Option<mqttiumd_store_sqlite::receipt_points::Hook>) {
        if let Self::Durable(s) = self {
            s.set_receipt_hook(hook);
        }
    }
}

impl DurableStore for StoreBackend {
    fn read_wills(
        &self,
        after: Option<OperationId>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<WillPage, ReadError> {
        match self {
            Self::Durable(s) => s.read_wills(after, limit, budget),
            Self::Log(s) => s.read_wills(after, limit, budget),
            Self::Volatile { handle, .. } => handle.read_wills(after, limit, budget),
        }
    }

    fn expire_messages(
        &mut self,
        now: MessageTime,
        after: Option<mqttiumd_contract::ids::PublicationId>,
        limit: u32,
    ) -> Result<mqttiumd_store_api::MessageExpirySweep, ReadError> {
        match self {
            Self::Durable(s) => s.expire_messages(now, after, limit),
            Self::Log(s) => s.expire_messages(now, after, limit),
            Self::Volatile { handle, dirty, .. } => {
                let res = handle.expire_messages(now, after, limit)?;
                if !res.removed.is_empty() || res.retained_removed > 0 {
                    *dirty = true;
                }
                Ok(res)
            }
        }
    }

    fn checkpoint_session_clock(
        &mut self,
        expected: Option<SessionClockAnchor>,
        reading: SessionClockReading,
    ) -> Result<SessionClockAnchor, ReadError> {
        match self {
            Self::Durable(s) => s.checkpoint_session_clock(expected, reading),
            Self::Log(s) => s.checkpoint_session_clock(expected, reading),
            Self::Volatile { handle, dirty, .. } => {
                let anchor = handle.checkpoint_session_clock(expected, reading)?;
                *dirty = true;
                Ok(anchor)
            }
        }
    }

    fn collect_receipts(
        &mut self,
        collection: ReceiptCollection,
    ) -> Result<ReceiptCollectionReply, ReadError> {
        match self {
            Self::Durable(s) => s.collect_receipts(collection),
            Self::Log(s) => s.collect_receipts(collection),
            Self::Volatile { handle, dirty, .. } => {
                let reply = handle.collect_receipts(collection)?;
                if matches!(reply, ReceiptCollectionReply::Applied { .. }) {
                    *dirty = true;
                }
                Ok(reply)
            }
        }
    }

    fn receipt_frontier(
        &self,
        origin: mqttiumd_contract::receipts::ReceiptOrigin,
    ) -> Result<Option<mqttiumd_contract::receipts::ReceiptClosure>, ReadError> {
        match self {
            Self::Durable(s) => s.receipt_frontier(origin),
            Self::Log(s) => s.receipt_frontier(origin),
            Self::Volatile { handle, .. } => handle.receipt_frontier(origin),
        }
    }

    fn collect_closed_receipts(
        &mut self,
        collection: ClosedReceiptCollection,
    ) -> Result<ClosedReceiptReply, ReadError> {
        match self {
            Self::Durable(s) => s.collect_closed_receipts(collection),
            Self::Log(s) => s.collect_closed_receipts(collection),
            Self::Volatile { handle, dirty, .. } => {
                let reply = handle.collect_closed_receipts(collection)?;
                if matches!(reply, ClosedReceiptReply::Applied { .. }) {
                    *dirty = true;
                }
                Ok(reply)
            }
        }
    }

    fn ledger(&self) -> Result<mqttiumd_contract::command::EpochLedger, ReadError> {
        match self {
            Self::Durable(s) => s.ledger(),
            Self::Log(s) => s.ledger(),
            Self::Volatile { handle, .. } => handle.ledger(),
        }
    }

    fn open_epoch(
        &mut self,
        command: &mqttiumd_contract::command::OpenEpoch,
    ) -> Result<mqttiumd_contract::outcome::OpeningOutcome, ReadError> {
        match self {
            Self::Durable(s) => s.open_epoch(command),
            Self::Log(s) => s.open_epoch(command),
            Self::Volatile { handle, dirty, .. } => {
                let res = handle.open_epoch(command)?;
                *dirty = true;
                Ok(res)
            }
        }
    }

    fn apply(
        &mut self,
        command: &mqttiumd_contract::command::Command,
    ) -> mqttiumd_store_api::Applied {
        match self {
            Self::Durable(s) => s.apply(command),
            Self::Log(s) => s.apply(command),
            Self::Volatile { handle, dirty, .. } => {
                let res = handle.apply(command);
                if res.wrote {
                    *dirty = true;
                }
                res
            }
        }
    }

    fn established(
        &self,
        operation: OperationId,
    ) -> Result<Option<mqttiumd_contract::outcome::StoreOutcome>, ReadError> {
        match self {
            Self::Durable(s) => s.established(operation),
            Self::Log(s) => s.established(operation),
            Self::Volatile { handle, .. } => handle.established(operation),
        }
    }

    fn resolve_and_fence(&mut self, operation: OperationId) -> mqttiumd_store_api::Resolved {
        match self {
            Self::Durable(s) => s.resolve_and_fence(operation),
            Self::Log(s) => s.resolve_and_fence(operation),
            Self::Volatile { handle, dirty, .. } => {
                let res = handle.resolve_and_fence(operation);
                if res.fenced_now {
                    *dirty = true;
                }
                res
            }
        }
    }

    fn admin(
        &mut self,
        command: AdminCommand,
    ) -> Result<AdminDone, mqttiumd_store_api::AdminRefused> {
        match self {
            Self::Durable(s) => s.admin(command),
            Self::Log(s) => s.admin(command),
            Self::Volatile { handle, dirty, .. } => {
                let res = handle.admin(command)?;
                *dirty = true;
                Ok(res)
            }
        }
    }

    fn recover_with_budget(
        &self,
        cursor: Option<RecoveryCursor>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<RecoveryPage, RecoveryRefused> {
        match self {
            Self::Durable(s) => s.recover_with_budget(cursor, limit, budget),
            Self::Log(s) => s.recover_with_budget(cursor, limit, budget),
            Self::Volatile { handle, .. } => handle.recover_with_budget(cursor, limit, budget),
        }
    }

    fn read_work_with_budget(
        &self,
        session: SessionId,
        after: Option<mqttiumd_contract::ids::DeliveryId>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<WorkPage, ReadError> {
        match self {
            Self::Durable(s) => s.read_work_with_budget(session, after, limit, budget),
            Self::Log(s) => s.read_work_with_budget(session, after, limit, budget),
            Self::Volatile { handle, .. } => {
                handle.read_work_with_budget(session, after, limit, budget)
            }
        }
    }

    fn snapshot_with_budget(
        &self,
        budget: ReadBudget,
    ) -> Result<mqttiumd_store_api::DurableState, ReadError> {
        match self {
            Self::Durable(s) => s.snapshot_with_budget(budget),
            Self::Log(s) => s.snapshot_with_budget(budget),
            Self::Volatile { handle, .. } => handle.snapshot_with_budget(budget),
        }
    }
}

pub const QUEUE: usize = 1024;
pub const CONTROL_QUEUE: usize = 4;
/// Commands one transaction may carry: bounded by both request lane and SQLite group cap.
const GROUP: usize = if QUEUE < mqttiumd_store_sqlite::MAX_GROUP {
    QUEUE
} else {
    mqttiumd_store_sqlite::MAX_GROUP
};
pub const SESSIONS: usize = 4_096;
pub const MAX_CLIENT: usize = 1024;
pub const MAX_TOPIC: usize = 1024;
pub const MAX_PAYLOAD: usize = 1_048_576;
fn hex_domain(domain: [u8; 32]) -> String {
    domain.iter().map(|byte| format!("{byte:02x}")).collect()
}
pub fn message_expiry_details(
    now: MessageTime,
    after: Option<mqttiumd_contract::ids::PublicationId>,
) -> serde_json::Value {
    json!({
        "domain":hex_domain(now.domain),
        "tick":now.tick.to_string(),
        "after":after.map(|id|format!("{}/{}/{}/{}",id.incarnation().get(),id.epoch().get(),id.owner().get(),id.sequence().get())).unwrap_or_default()
    })
}
pub fn limits() -> Limits {
    Limits {
        payload_bytes: PayloadBytes::new(256 * 1024 * 1024),
        references: References::new(65_536),
        exchange_slots: ExchangeSlots::new(32_768),
        receipt_slots: ReceiptSlots::new(65_536),
        max_targets_per_admission: 1_024,
        max_topic_bytes: MAX_TOPIC,
        max_payload_bytes: MAX_PAYLOAD,
        max_sessions: SESSIONS,
        max_route_targets: 1_024,
    }
}
pub fn recipe_kind(recipe: &Recipe) -> &'static str {
    match recipe {
        Recipe::AnnotatedPublication(value) => recipe_kind(value.inner.as_ref()),
        Recipe::WillPublication(_) => "WillAdmission",
        Recipe::SessionLifecycle(change) => match change.action {
            SessionAction::Open { .. }
            | SessionAction::OpenAt { .. }
            | SessionAction::OpenWillAt { .. } => "SessionOpen",
            SessionAction::EndWill { .. } => "WillEnd",
            SessionAction::Detach { .. } => "SessionDetach",
            SessionAction::Expire { .. } => "SessionExpire",
            SessionAction::Close { .. } => "SessionClose",
        },
        Recipe::ExpiringPublication(value) => match value.admission {
            mqttiumd_contract::message_expiry::ExpiringAdmission::Qos12(_) => "Admission",
            mqttiumd_contract::message_expiry::ExpiringAdmission::Qos0(_) => "RetainedQoS0",
        },
        Recipe::TimedReplaySubscriptions(_) => "Subscriptions",
        Recipe::MarkDeliveryStarted(_) => "DeliveryStarted",
        Recipe::PreparePubrel(_) => "DeliveryPubrelReady",
        Recipe::ExpireDelivery(_) => "MessageExpire",
        Recipe::RetainedPublication(_)
        | Recipe::AdmitPublication(_)
        | Recipe::ClaimedPublication(_) => "Admission",
        Recipe::RetainedQos0(_) => "RetainedQoS0",
        Recipe::ReplaySubscriptions(_) => "Subscriptions",
        Recipe::ReleaseInbound(_) => "Release",
        Recipe::ChangeSubscriptions(_) => "Subscriptions",
        Recipe::ClaimDelivery(_) => "Claim",
        Recipe::FinishDelivery(_) => "Finish",
        Recipe::ConsumeDelivery(_) => "Consume",
        Recipe::UpdateSubscriptions(_) => "Subscriptions",
    }
}
#[derive(Clone, Debug)]
pub struct Failure {
    pub code: &'static str,
    pub read: Option<(ReadError, ReadPhase)>,
}
impl Failure {
    pub fn plain(code: &'static str) -> Self {
        Self { code, read: None }
    }
    pub fn read(error: ReadError, phase: ReadPhase) -> Self {
        Self {
            code: "durable_read",
            read: Some((error, phase)),
        }
    }
    pub fn fields(&self) -> serde_json::Value {
        match self.read {
            Some((cause, phase)) => {
                json!({"cause":cause.code(),"phase":phase.code(),"details":{"code":self.code}})
            }
            None => json!({"details":{"code":self.code}}),
        }
    }
}
#[derive(Clone, Debug)]
pub struct Session {
    pub id: SessionId,
    pub revision: Revision,
    pub client: SharedBytes,
    pub present: bool,
    pub expiry: SessionExpiry,
    pub opened_by: Option<OperationId>,
    pub detached: Option<Box<SessionDetachment>>,
}
pub struct Boot {
    pub grant: EpochGrant,
    pub sessions: BTreeMap<SharedBytes, Session>,
    pub collect_closed: bool,
    pub sqlite: Option<mqttiumd_store_sqlite::Settings>,
    pub storage_mode: StorageMode,
    pub clock: Arc<dyn SessionClock>,
    pub clock_anchor: SessionClockAnchor,
}
pub enum Request {
    Recovery(Option<RecoveryCursor>),
    Session { request: u64, client: SharedBytes },
    Effect(OwnedEffect),
    Stop,
}
/// A reserved lane: maintenance never waits for a network/business slot.
pub enum ControlRequest {
    WillBarrier {
        connection: ConnectionGeneration,
        client: SharedBytes,
    },
    MessageExpiry {
        now: MessageTime,
        after: Option<mqttiumd_contract::ids::PublicationId>,
        limit: u32,
    },
    ClockCheckpoint {
        expected: SessionClockAnchor,
        reading: SessionClockReading,
    },
    Lifecycle(OwnedEffect),
    Collect {
        effect: EffectId,
        collection: ReceiptCollection,
    },
    Closed(ClosedReceiptCollection),
}
#[derive(Clone)]
pub enum ControlResponse {
    WillBarrier {
        connection: ConnectionGeneration,
        result: Result<bool, Failure>,
    },
    MessageExpiryUnknown {
        now: MessageTime,
        after: Option<mqttiumd_contract::ids::PublicationId>,
    },
    MessageExpiry {
        now: MessageTime,
        after: Option<mqttiumd_contract::ids::PublicationId>,
        result: Result<Arc<mqttiumd_store_api::MessageExpirySweep>, Failure>,
    },
    ClockCheckpoint {
        expected: SessionClockAnchor,
        reading: SessionClockReading,
        result: Result<SessionClockAnchor, Failure>,
    },
    Lifecycle {
        effect: EffectId,
        operation: OperationId,
        reply: StoreReply,
    },
    Failed(Failure),
    Collected {
        effect: EffectId,
        collection: ReceiptCollection,
        result: Result<ReceiptCollectionReply, Failure>,
    },
    Closed {
        collection: ClosedReceiptCollection,
        result: Result<ClosedReceiptReply, Failure>,
    },
}
pub enum Response {
    Wills {
        effect: EffectId,
        result: Result<WillPage, ReadError>,
    },
    Recovery(Result<RecoveryPage, RecoveryRefused>),
    Session {
        request: u64,
        result: Result<Session, Failure>,
    },
    Store {
        effect: EffectId,
        operation: OperationId,
        reply: StoreReply,
        targets: Vec<SessionId>,
    },
    Work {
        effect: EffectId,
        session: SessionId,
        result: Result<WorkPage, ReadError>,
    },
    Stopped,
}
pub fn collection_details(collection: ReceiptCollection) -> serde_json::Value {
    let closure = collection.closure;
    json!({"incarnation":closure.origin.incarnation.get().to_string(),"epoch":closure.origin.epoch.get().to_string(),"owner":closure.origin.owner.get().to_string(),"through":closure.through.get().to_string(),"publication_through":closure.publication_through.get().to_string(),"payload_through":closure.payload_through.get().to_string(),"limit":collection.limit})
}

pub fn returned_closure_details(
    details: &mut serde_json::Value,
    closure: mqttiumd_contract::receipts::ReceiptClosure,
) {
    details["returned_incarnation"] = json!(closure.origin.incarnation.get().to_string());
    details["returned_epoch"] = json!(closure.origin.epoch.get().to_string());
    details["returned_owner"] = json!(closure.origin.owner.get().to_string());
    details["returned_through"] = json!(closure.through.get().to_string());
    details["returned_publication_through"] = json!(closure.publication_through.get().to_string());
    details["returned_payload_through"] = json!(closure.payload_through.get().to_string());
}
#[derive(Clone, Default)]
pub struct Hooks {
    pub directory: Option<PathBuf>,
    pub pause: Vec<String>,
}
impl Hooks {
    #[cfg(feature = "test-hooks")]
    fn barrier(
        &self,
        point: &'static str,
        kind: &'static str,
        id: OperationId,
        events: &Events,
        cancel: &AtomicBool,
    ) -> Result<(), Failure> {
        self.barrier_fields(
            point,
            json!({"point":point,"command_kind":kind,"operation":operation(id)}),
            events,
            cancel,
        )
    }
    #[cfg(feature = "test-hooks")]
    pub(crate) fn barrier_fields(
        &self,
        point: &'static str,
        fields: serde_json::Value,
        events: &Events,
        cancel: &AtomicBool,
    ) -> Result<(), Failure> {
        if !self.pause.iter().any(|p| p == point) {
            return Ok(());
        }
        let dir = self
            .directory
            .as_ref()
            .ok_or_else(|| Failure::plain("hook_directory"))?;
        events
            .emit("barrier_hit", fields)
            .map_err(|_| Failure::plain("events_unavailable"))?;
        let release = dir.join(format!("release-{point}"));
        let start = Instant::now();
        while !cancel.load(Ordering::Relaxed) {
            if release.exists() {
                std::fs::remove_file(release).map_err(|_| Failure::plain("hook_release"))?;
                return Ok(());
            }
            if start.elapsed() > Duration::from_secs(30) {
                return Err(Failure::plain("hook_deadline"));
            }
            thread::sleep(Duration::from_millis(2));
        }
        // Cancellation releases a submitted command. It never means certain rejection.
        Ok(())
    }
}
pub struct Writer {
    pub sender: crate::wake::Signalled<Request>,
    pub receiver: Receiver<Response>,
    pub control_sender: crate::wake::Signalled<ControlRequest>,
    pub control_receiver: Receiver<ControlResponse>,
    pub boot: Receiver<Result<Boot, Failure>>,
    /// Signalled after each reply the worker publishes.
    pub wake: crate::wake::Wake,
    handle: Option<JoinHandle<Result<(), Failure>>>,
}
impl Writer {
    pub fn spawn(
        path: PathBuf,
        events: Events,
        hooks: Hooks,
        cancel: Arc<AtomicBool>,
    ) -> Result<Self, Failure> {
        let clock = Arc::new(SystemClock::new().map_err(|error| Failure::plain(error.code()))?);
        Self::spawn_with_clock(path, events, hooks, cancel, clock)
    }

    pub fn spawn_with_clock(
        path: PathBuf,
        events: Events,
        hooks: Hooks,
        cancel: Arc<AtomicBool>,
        clock: Arc<dyn SessionClock>,
    ) -> Result<Self, Failure> {
        Self::spawn_bound(
            path,
            events,
            hooks,
            cancel,
            clock,
            None,
            false,
            StorageMode::Durable,
        )
    }

    pub(crate) fn spawn_configured(
        path: PathBuf,
        events: Events,
        hooks: Hooks,
        cancel: Arc<AtomicBool>,
        policy: Option<mqttiumd_store_sqlite::StoragePolicy>,
        require_existing: bool,
        storage_mode: StorageMode,
    ) -> Result<Self, Failure> {
        let clock = Arc::new(SystemClock::new().map_err(|error| Failure::plain(error.code()))?);
        Self::spawn_bound(
            path,
            events,
            hooks,
            cancel,
            clock,
            policy,
            require_existing,
            storage_mode,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_bound(
        path: PathBuf,
        events: Events,
        hooks: Hooks,
        cancel: Arc<AtomicBool>,
        clock: Arc<dyn SessionClock>,
        policy: Option<mqttiumd_store_sqlite::StoragePolicy>,
        require_existing: bool,
        storage_mode: StorageMode,
    ) -> Result<Self, Failure> {
        let _ = &hooks;
        if hooks.directory.is_some() && storage_mode != StorageMode::Durable {
            return Err(Failure::plain("volatile_test_hooks_unsupported"));
        }
        // The worker waits on `requests` when both request lanes are empty.
        let requests = crate::wake::Wake::new();
        let (tx, rx) = crate::wake::signalled(QUEUE, &requests);
        let (rtx, rrx) = mpsc::sync_channel(QUEUE * 2);
        let (btx, brx) = mpsc::sync_channel(1);
        let (ctx, crx) = crate::wake::signalled(CONTROL_QUEUE, &requests);
        // File-polled test controls have no producer to signal the worker.
        let idle = if hooks.directory.is_some() {
            Duration::from_millis(1)
        } else {
            Duration::from_millis(100)
        };
        let (cctx, ccrx) = mpsc::sync_channel(CONTROL_QUEUE);
        let wake = crate::wake::Wake::new();
        let worker_wake = wake.clone();
        let handle = thread::Builder::new()
            .name(match storage_mode {
                StorageMode::Durable => "sqlite-writer".into(),
                StorageMode::Log => "log-writer".into(),
                StorageMode::Volatile { .. } => "volatile-writer".into(),
            })
            .spawn(move || -> Result<(), Failure> {
                let (mut store, boot) = match bootstrap_bound(
                    &path,
                    clock,
                    policy,
                    require_existing,
                    storage_mode,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = btx.send(Err(error));
                        return Ok(());
                    }
                };
                if btx.send(Ok(boot)).is_err() {
                    return Ok(());
                }
                let mut pending: VecDeque<Response> = VecDeque::new();
                let mut pending_control = None;
                // Barriers and one-shot controls of a test run address one
                // command at a time: no group is formed while they exist.
                let group = if hooks.directory.is_some() {
                    1
                } else if storage_mode == StorageMode::Log {
                    QUEUE.min(mqttiumd_store_volatile::LOG_MAX_GROUP)
                } else {
                    GROUP
                };
                let mut stopping = false;
                let mut stop_result: Result<(), Failure> = Ok(());
                let mut state = BusinessState::default();
                let mut members: Vec<Member> = Vec::with_capacity(group);
                let mut deferred_reads: Vec<(EffectId, Effect)> = Vec::new();
                #[cfg(feature="test-hooks")]
                let mut duplicate_control: Option<ControlResponse> = None;
                loop {
                    #[cfg(feature="test-hooks")]
                    if replay_controls(&mut store,&state,&hooks,&events).is_err() { cancel.store(true,Ordering::Relaxed); }
                    // A reply lane the core has not drained is retried on a
                    // short timer; nothing else in this loop waits on a timer.
                    let mut stalled = false;
                    if let Some(reply) = pending_control.take() {
                        match cctx.try_send(reply) {
                            Ok(()) => worker_wake.wake(),
                            Err(TrySendError::Full(reply)) => {
                                stalled = true;
                                pending_control = Some(reply);
                            }
                            Err(TrySendError::Disconnected(_)) => {
                                return if stopping {
                                    stop_result
                                } else {
                                    store.maybe_snapshot(&events, true)
                                };
                            }
                        }
                    }
                    // Replies leave in the order of their requests.
                    let mut sent_reply = false;
                    while let Some(reply) = pending.pop_front() {
                        match rtx.try_send(reply) {
                            Ok(()) => sent_reply = true,
                            Err(TrySendError::Full(reply)) => {
                                stalled = true;
                                pending.push_front(reply);
                                break;
                            }
                            Err(TrySendError::Disconnected(_)) => {
                                return if stopping {
                                    stop_result
                                } else {
                                    store.maybe_snapshot(&events, true)
                                };
                            }
                        }
                    }
                    if sent_reply {
                        worker_wake.wake();
                    }
                    if stopping && pending.is_empty() {
                        return stop_result;
                    }
                    // Even a completely unread ordinary result lane cannot prevent
                    // a reserved receipt closure from committing and replying.
                    #[cfg(feature="test-hooks")]
                    if pending_control.is_none() { pending_control = duplicate_control.take(); }
                    if pending_control.is_none() {
                        match crx.try_recv() {
                            Ok(request) => {
                                let reply = control_response(&mut store, request, &events, &hooks, &cancel, &mut state);
                                #[cfg(feature="test-hooks")]
                                if let ControlResponse::Collected { collection, result:Ok(ReceiptCollectionReply::Applied { .. }),.. } = &reply
                                    && hooks.directory.as_ref().is_some_and(|d| consume_file(&d.join("duplicate-close-result"))) {
                                    let _ = events.emit("receipt_collection_result_duplicated",json!({"command_kind":"ReceiptCollection","details":collection_details(*collection)}));
                                    duplicate_control = Some(reply.clone());
                                }
                                #[cfg(feature="test-hooks")]
                                if let ControlResponse::MessageExpiry { now, after, result:Ok(_),.. } = &reply
                                    && hooks.directory.as_ref().is_some_and(|d| consume_file(&d.join("duplicate-next-message-expire-result"))) {
                                    let _ = events.emit("message_expiry_result_duplicated",json!({"details":message_expiry_details(*now,*after)}));
                                    duplicate_control = Some(reply.clone());
                                }
                                pending_control = Some(reply);
                            }
                            Err(TryRecvError::Empty) => {}
                            Err(TryRecvError::Disconnected) => {
                                return if stopping {
                                    stop_result
                                } else {
                                    store.maybe_snapshot(&events, true)
                                };
                            }
                        }
                    }
                    if pending.is_empty() && !stopping {
                        // Consecutive commands waiting in the lane commit as
                        // one group. Interleaved read-only LoadWork requests do
                        // not shatter the write group: they run immediately
                        // after the group commit so they see the committed rows.
                        members.clear();
                        deferred_reads.clear();
                        let mut other = None;
                        let mut closed = false;
                        while members.len() < group && deferred_reads.len() < group {
                            match rx.try_recv() {
                                Ok(Request::Effect(effect)) => match effect.into_parts() {
                                    (effect, Effect::Persist { command }) if group > 1 => {
                                        members.push(Member { effect, command });
                                    }
                                    (effect, work @ Effect::LoadWork { .. }) if group > 1 => {
                                        deferred_reads.push((effect, work));
                                    }
                                    (effect, work) => {
                                        other = Some(Ok((effect, work)));
                                        break;
                                    }
                                },
                                Ok(request) => {
                                    other = Some(Err(request));
                                    break;
                                }
                                Err(TryRecvError::Empty) => break,
                                Err(TryRecvError::Disconnected) => {
                                    closed = true;
                                    break;
                                }
                            }
                        }
                        let idle_turn =
                            members.is_empty() && deferred_reads.is_empty() && other.is_none();
                        if !members.is_empty() {
                            group_responses_into(&mut store, &members, &events, &cancel, &mut pending);
                            members.clear();
                        }
                        for (effect, work) in deferred_reads.drain(..) {
                            pending.push_back(effect_response(
                                &mut store, effect, work, &events, &hooks, &cancel, &mut state,
                            ));
                        }
                        match other {
                            Some(Err(Request::Stop)) => {
                                stop_result = store.maybe_snapshot(&events, true);
                                pending.push_back(Response::Stopped);
                                stopping = true;
                            }
                            Some(Err(request)) => pending.push_back(business_response(
                                &mut store, request, &events, &hooks, &cancel, &mut state,
                            )),
                            Some(Ok((effect, work))) => pending.push_back(effect_response(
                                &mut store, effect, work, &events, &hooks, &cancel, &mut state,
                            )),
                            None => {}
                        }
                        if !stopping {
                            let _ = store.maybe_snapshot(&events, false);
                        }
                        if closed && pending.is_empty() {
                            return store.maybe_snapshot(&events, true);
                        }
                        // Both request lanes signal `requests`. A reply
                        // produced in this turn is published before waiting.
                        if idle_turn && !closed && pending_control.is_none() && !stalled {
                            requests.wait(idle);
                        }
                    }
                    if stalled {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            })
            .map_err(|_| Failure::plain("writer_spawn"))?;
        Ok(Self {
            sender: tx,
            receiver: rrx,
            control_sender: ctx,
            control_receiver: ccrx,
            boot: brx,
            wake,
            handle: Some(handle),
        })
    }
    pub fn is_finished(&self) -> bool {
        self.handle.as_ref().is_none_or(|h| h.is_finished())
    }
    pub fn join_finished(&mut self) -> Result<(), Failure> {
        if self.is_finished() {
            if let Some(h) = self.handle.take() {
                h.join().map_err(|_| Failure::plain("writer_panicked"))??;
            }
            Ok(())
        } else {
            Err(Failure::plain("writer_not_finished"))
        }
    }
}

/// Single-threaded synchronous store engine for `--storage-mode volatile`
/// (when test-hooks are inactive), eliminating cross-thread channels, mutex
/// handoffs, Condvar wakeups, and `Arc` cache-line bouncing on every durable
/// command while preserving the exact `VolatileStore`, `DurableStore`, and
/// atomic SQLite snapshot semantics.
pub(crate) struct DirectWriter {
    store: StoreBackend,
    events: Events,
    hooks: Hooks,
    cancel: Arc<AtomicBool>,
    state: BusinessState,
    responses: VecDeque<Response>,
    control_responses: VecDeque<ControlResponse>,
    last_snapshot_check: Instant,
}

impl DirectWriter {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open(
        path: PathBuf,
        events: Events,
        hooks: Hooks,
        cancel: Arc<AtomicBool>,
        policy: Option<mqttiumd_store_sqlite::StoragePolicy>,
        require_existing: bool,
        storage_mode: StorageMode,
    ) -> Result<(Self, Boot), Failure> {
        if hooks.directory.is_some() {
            return Err(Failure::plain("volatile_test_hooks_unsupported"));
        }
        let clock = Arc::new(SystemClock::new().map_err(|error| Failure::plain(error.code()))?);
        let (store, boot) = bootstrap_bound(&path, clock, policy, require_existing, storage_mode)?;
        Ok((
            Self {
                store,
                events,
                hooks,
                cancel,
                state: BusinessState::default(),
                responses: VecDeque::new(),
                control_responses: VecDeque::new(),
                last_snapshot_check: Instant::now(),
            },
            boot,
        ))
    }

    #[inline]
    pub(crate) fn apply_persist_inline(
        &mut self,
        command: mqttiumd_contract::command::Command,
        out_targets: &mut Vec<SessionId>,
    ) -> StoreReply {
        use mqttiumd_contract::outcome::{CommitFacts, StoreOutcome};
        let is_will = matches!(command.recipe.get(), Recipe::WillPublication(_));
        let needs_result_record = matches!(
            command.recipe.get(),
            Recipe::SessionLifecycle(_) | Recipe::WillPublication(_)
        ) || self.events.records("store_result");
        let is_single_claimed = matches!(
            command.recipe.get().inner_publication(),
            Recipe::ClaimedPublication(claimed) if claimed.admission.targets.len() <= 1
        );
        let recipe_for_post = if is_will || needs_result_record || !is_single_claimed {
            Some(command.recipe.clone())
        } else {
            None
        };
        let applied = self.store.apply_owned(command);
        if let Some(frozen) = recipe_for_post {
            let recipe = frozen.get();
            if is_will {
                record_applied(recipe, &applied, &self.events, &self.cancel);
            }
            if needs_result_record {
                record_result(recipe, &applied, &self.events, &self.cancel);
            }
            if !is_single_claimed
                && matches!(
                    applied.reply,
                    StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::PublicationAdmitted { .. }
                            | CommitFacts::RetainedQos0Stored { .. }
                            | CommitFacts::SubscriptionsChanged { .. }
                    ))
                )
            {
                push_targets_of(recipe, out_targets);
            }
        }
        applied.reply
    }

    pub(crate) fn exec_request(&mut self, request: Request) {
        let reply = business_response(
            &mut self.store,
            request,
            &self.events,
            &self.hooks,
            &self.cancel,
            &mut self.state,
        );
        self.responses.push_back(reply);
    }

    pub(crate) fn exec_control(&mut self, request: ControlRequest) {
        let reply = control_response(
            &mut self.store,
            request,
            &self.events,
            &self.hooks,
            &self.cancel,
            &mut self.state,
        );
        self.control_responses.push_back(reply);
    }

    pub(crate) fn try_recv(&mut self) -> Result<Response, TryRecvError> {
        self.responses.pop_front().ok_or(TryRecvError::Empty)
    }

    pub(crate) fn try_recv_control(&mut self) -> Result<ControlResponse, TryRecvError> {
        self.control_responses
            .pop_front()
            .ok_or(TryRecvError::Empty)
    }

    pub(crate) fn maybe_periodic_snapshot(&mut self, now: Instant) {
        if now.saturating_duration_since(self.last_snapshot_check) >= Duration::from_millis(100) {
            self.last_snapshot_check = now;
            let _ = self.store.maybe_snapshot(&self.events, false);
        }
    }

    pub(crate) fn finish(mut self) -> Result<(), Failure> {
        self.store.maybe_snapshot(&self.events, true)
    }
}

/// Abstracts the exclusive store worker between the threaded writer (SQLite
/// durable, `.mlog` log-first, and test-hook runs) and the zero-hop in-thread
/// `DirectWriter` used for `--storage-mode volatile`.
pub(crate) enum StoreHandle<'a> {
    Threaded(&'a mut Writer),
    Direct(&'a mut DirectWriter),
}

#[expect(
    clippy::result_large_err,
    reason = "the refused request is handed back by value"
)]
impl StoreHandle<'_> {
    #[cfg(test)]
    pub(crate) fn threaded_mut(&mut self) -> &mut Writer {
        match self {
            Self::Threaded(writer) => writer,
            Self::Direct(_) => panic!("expected threaded writer"),
        }
    }

    #[inline]
    pub(crate) fn is_direct(&self) -> bool {
        matches!(self, Self::Direct(_))
    }

    #[inline]
    pub(crate) fn skip_empty_message_expiry_sweep(&self) -> bool {
        match self {
            Self::Threaded(_) => false,
            Self::Direct(direct) => !direct.store.has_expiring_publications(),
        }
    }

    pub(crate) fn try_send(&mut self, request: Request) -> Result<(), TrySendError<Request>> {
        match self {
            Self::Threaded(writer) => writer.sender.try_send(request),
            Self::Direct(direct) => {
                direct.exec_request(request);
                Ok(())
            }
        }
    }

    pub(crate) fn try_recv(&mut self) -> Result<Response, TryRecvError> {
        match self {
            Self::Threaded(writer) => writer.receiver.try_recv(),
            Self::Direct(direct) => direct.try_recv(),
        }
    }

    pub(crate) fn try_send_control(
        &mut self,
        request: ControlRequest,
    ) -> Result<(), TrySendError<ControlRequest>> {
        match self {
            Self::Threaded(writer) => writer.control_sender.try_send(request),
            Self::Direct(direct) => {
                direct.exec_control(request);
                Ok(())
            }
        }
    }

    pub(crate) fn try_recv_control(&mut self) -> Result<ControlResponse, TryRecvError> {
        match self {
            Self::Threaded(writer) => writer.control_receiver.try_recv(),
            Self::Direct(direct) => direct.try_recv_control(),
        }
    }

    #[inline]
    pub(crate) fn apply_persist_direct(
        &mut self,
        command: mqttiumd_contract::command::Command,
        out_targets: &mut Vec<SessionId>,
    ) -> StoreReply {
        match self {
            Self::Threaded(_) => unreachable!("apply_persist_direct requires Direct writer"),
            Self::Direct(direct) => direct.apply_persist_inline(command, out_targets),
        }
    }

    #[inline]
    pub(crate) fn maybe_periodic_snapshot(&mut self, now: Instant) {
        if let Self::Direct(direct) = self {
            direct.maybe_periodic_snapshot(now);
        }
    }

    pub(crate) fn exec_recovery(
        &mut self,
        cursor: Option<RecoveryCursor>,
    ) -> Result<Option<Response>, Failure> {
        match self {
            Self::Threaded(writer) => {
                writer
                    .sender
                    .send(Request::Recovery(cursor))
                    .map_err(|_| Failure::plain("writer_closed"))?;
                Ok(None)
            }
            Self::Direct(direct) => {
                direct.exec_request(Request::Recovery(cursor));
                Ok(direct.responses.pop_front())
            }
        }
    }

    pub(crate) fn exec_closed_collection(
        &mut self,
        collection: ClosedReceiptCollection,
    ) -> Result<Option<ControlResponse>, Failure> {
        match self {
            Self::Threaded(writer) => {
                writer
                    .control_sender
                    .send(ControlRequest::Closed(collection))
                    .map_err(|_| Failure::plain("writer_control_disconnected"))?;
                Ok(None)
            }
            Self::Direct(direct) => {
                direct.exec_control(ControlRequest::Closed(collection));
                Ok(direct.control_responses.pop_front())
            }
        }
    }
}
#[derive(Default)]
struct BusinessState {
    #[cfg(feature = "test-hooks")]
    held_will_admission: Option<OperationId>,
    #[cfg(feature = "test-hooks")]
    held_will_end: Option<OperationId>,
    #[cfg(feature = "test-hooks")]
    held_session_open: Option<OperationId>,
    #[cfg(feature = "test-hooks")]
    held_session_close: Option<OperationId>,
    #[cfg(feature = "test-hooks")]
    held_session_detach: Option<OperationId>,
    #[cfg(feature = "test-hooks")]
    held_session_expire: Option<OperationId>,
    #[cfg(feature = "test-hooks")]
    held_admission: Option<OperationId>,
    #[cfg(feature = "test-hooks")]
    held_claim: Option<OperationId>,
    #[cfg(feature = "test-hooks")]
    held_finish: Option<OperationId>,
    #[cfg(feature = "test-hooks")]
    held_pubrel_ready: Option<OperationId>,
    #[cfg(feature = "test-hooks")]
    captured: Option<mqttiumd_contract::command::Command>,
}
fn business_response(
    store: &mut StoreBackend,
    request: Request,
    events: &Events,
    hooks: &Hooks,
    cancel: &Arc<AtomicBool>,
    state: &mut BusinessState,
) -> Response {
    let _ = (hooks, cancel, &state);
    match request {
        Request::Stop => Response::Stopped,
        Request::Recovery(cursor) => Response::Recovery(store.recover(cursor, 64)),
        Request::Session { request, client } => Response::Session {
            request,
            result: session(store, client),
        },
        Request::Effect(effect) => {
            let (effect, work) = effect.into_parts();
            effect_response(store, effect, work, events, hooks, cancel, state)
        }
    }
}

/// The sessions a committed command may have given work to.
#[inline]
fn push_targets_of(recipe: &Recipe, out: &mut Vec<SessionId>) {
    if let Recipe::ClaimedPublication(claimed) = recipe.inner_publication() {
        if claimed.admission.targets.len() <= 1 {
            return;
        }
        let claimed_idx = usize::from(claimed.claim.target_index);
        for (i, t) in claimed.admission.targets.as_slice().iter().enumerate() {
            if i != claimed_idx {
                out.push(t.session);
            }
        }
        return;
    }
    if let Recipe::WillPublication(a) = recipe {
        out.extend(a.targets.as_slice().iter().map(|t| t.session));
    } else if let Some(a) = recipe.admission() {
        out.extend(a.targets.as_slice().iter().map(|t| t.session));
    } else if let Some(a) = recipe.retained_qos0() {
        out.extend(a.targets.as_slice().iter().map(|t| t.session));
    } else if let Some(a) = recipe.replay_subscriptions() {
        out.push(a.update.session);
    }
}

fn targets_of(recipe: &Recipe) -> Vec<SessionId> {
    let mut out = Vec::new();
    push_targets_of(recipe, &mut out);
    out
}

/// The records of one applied command. They are written after its commit
/// returned, for a command applied alone and for a member of a group.
fn record_applied(
    recipe: &Recipe,
    applied: &mqttiumd_store_api::Applied,
    events: &Events,
    cancel: &AtomicBool,
) {
    let id = recipe.operation();
    let kind = recipe_kind(recipe);
    if kind == "WillAdmission"
        && applied.committed_now
        && matches!(
            applied.reply,
            StoreReply::Outcome(mqttiumd_contract::outcome::StoreOutcome::Committed(
                mqttiumd_contract::outcome::CommitFacts::WillPublished { .. }
            ))
        )
        && let Some(ns) = crate::events::monotonic_ns()
        && events
            .emit(
                "will_commit_observed",
                json!({"operation":operation(id),
            "details":{"monotonic_ns":ns.to_string(),"authority":"sqlite_commit_return"}}),
            )
            .is_err()
    {
        cancel.store(true, Ordering::Relaxed);
    }
}

/// The lifecycle records and the result record of one applied command.
fn record_result(
    recipe: &Recipe,
    applied: &mqttiumd_store_api::Applied,
    events: &Events,
    cancel: &AtomicBool,
) {
    let id = recipe.operation();
    let kind = recipe_kind(recipe);
    if applied.committed_now {
        use mqttiumd_contract::outcome::{CommitFacts, StoreOutcome};
        let will_event = match (recipe, applied.reply) {
            (
                Recipe::SessionLifecycle(change),
                StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(_))),
            ) => match &change.action {
                SessionAction::OpenWillAt {
                    will: Some(spec), ..
                } => Some((
                    "will_installed",
                    json!({"operation":operation(id),"will":operation(id),"details":{"topic_bytes":spec.topic.len(),"payload_bytes":spec.payload.len(),"authority":"durable_open_commit"}}),
                )),
                SessionAction::EndWill { guard, publish, .. } => Some((
                    "will_decided",
                    json!({"operation":operation(id),"will":guard.opened_by.map(operation),"details":{"publish":publish,"session":guard.session.get().to_string(),"authority":"durable_end_commit"}}),
                )),
                _ => None,
            },
            (
                Recipe::WillPublication(value),
                StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::WillPublished {
                    will,
                    publication,
                })),
            ) => Some((
                "will_admitted",
                json!({"operation":operation(id),"will":operation(will),"details":{"publication":format!("{}/{}/{}/{}",publication.incarnation().get(),publication.epoch().get(),publication.owner().get(),publication.sequence().get()),"targets":value.targets.len(),"retained":value.live_retain.is_some(),"authority":"durable_admission_commit"}}),
            )),
            _ => None,
        };
        if let Some((name, fields)) = will_event
            && events.emit(name, fields).is_err()
        {
            cancel.store(true, Ordering::Relaxed);
        }
    }
    if events.emit_with("store_result",||json!({"operation":operation(id),"command_kind":kind,"details":{"committed_now":applied.committed_now,"wrote":applied.wrote,"outcome":outcome_name(applied.reply)}})).is_err(){cancel.store(true,Ordering::Relaxed);}
}

/// One command of a group and the effect that carries it.
struct Member {
    effect: EffectId,
    command: mqttiumd_contract::command::Command,
}

/// Applies the commands in one transaction (record 0045) and appends each
/// answer in order into `out`. No answer exists before the commit returned.
fn group_responses_into(
    store: &mut StoreBackend,
    members: &[Member],
    events: &Events,
    cancel: &AtomicBool,
    out: &mut VecDeque<Response>,
) {
    let commands: Vec<_> = members.iter().map(|member| &member.command).collect();
    let count = commands.len();
    if events
        .emit_with("store_group", || json!({"details":{"commands":count}}))
        .is_err()
    {
        cancel.store(true, Ordering::Relaxed);
    }
    let applied = store.apply_group(&commands).unwrap_or_else(|_| {
        // The caller bounds a group; an oversized one is applied in turn.
        commands
            .iter()
            .map(|command| store.apply(command))
            .collect()
    });
    out.reserve(members.len());
    for (member, applied) in members.iter().zip(applied) {
        let recipe = member.command.recipe.get();
        record_applied(recipe, &applied, events, cancel);
        record_result(recipe, &applied, events, cancel);
        out.push_back(Response::Store {
            effect: member.effect,
            operation: recipe.operation(),
            reply: applied.reply,
            targets: targets_of(recipe),
        });
    }
}

fn effect_response(
    store: &mut StoreBackend,
    effect: EffectId,
    work: Effect,
    events: &Events,
    hooks: &Hooks,
    cancel: &Arc<AtomicBool>,
    state: &mut BusinessState,
) -> Response {
    let _ = (hooks, cancel, &state);
    match work {
        Effect::Persist { command } => {
            let id = command.recipe.operation();
            #[cfg(feature = "test-hooks")]
            let kind = recipe_kind(command.recipe.get());
            let recipe = command.recipe.get();
            let targets = targets_of(recipe);
            #[cfg(feature = "test-hooks")]
            {
                let hc = hooks.clone();
                let ev = events.clone();
                let stop = cancel.clone();
                let point = match kind {
                    "SessionOpen" => "session.open.prepared",
                    "WillAdmission" => "will.admission.prepared",
                    "WillEnd" => "will.end.prepared",
                    "SessionClose" => "session.close.prepared",
                    "SessionDetach" => "session.detach.prepared",
                    "SessionExpire" => "session.expire.prepared",
                    "Admission" => "admission.prepared",
                    "RetainedQoS0" => "retained.prepared",
                    "Release" => "release.prepared",
                    "Subscriptions" => "subscription.prepared",
                    "Claim" => "claim.prepared",
                    "Consume" => "consume.prepared",
                    "DeliveryStarted" => "delivery.started.prepared",
                    "MessageExpire" => "message.delivery.expire.prepared",
                    _ => "finish.prepared",
                };
                // The hook is inside the actual transaction, after all writes and before COMMIT.
                let hook = Box::new(move |p| {
                    if p == mqttiumd_store_sqlite::kill_points::KillPoint::CommitPrepared
                        && hc.barrier(point, kind, id, &ev, &stop).is_err()
                    {
                        stop.store(true, Ordering::Relaxed);
                    }
                });
                if store.set_kill_hook(Some(hook)).is_err() {
                    cancel.store(true, Ordering::Relaxed);
                }
            }
            #[cfg(feature = "test-hooks")]
            if kind == "Admission"
                && hooks
                    .directory
                    .as_ref()
                    .is_some_and(|d| consume_file(&d.join("capture-next-admission")))
            {
                state.captured = Some(command.clone());
                if let Some(a) = command.recipe.get().admission() {
                    let _ = events.emit("receipt_replay_captured",json!({"command_kind":"Admission","operation":operation(id),"details":{"publication":identity_text(a.publication.incarnation().get(),a.publication.epoch().get(),a.publication.owner().get(),a.publication.sequence().get()),"payload":identity_text(a.payload.id.incarnation().get(),a.payload.id.epoch().get(),a.payload.id.owner().get(),a.payload.id.sequence().get())}}));
                }
            }
            let applied = store.apply(&command);
            record_applied(recipe, &applied, events, cancel);
            #[cfg(feature = "test-hooks")]
            {
                let _ = store.set_kill_hook(None);
                let point = match kind {
                    "SessionOpen" => Some("session.open.committed"),
                    "WillAdmission" => Some("will.admission.committed"),
                    "WillEnd" => Some("will.end.committed"),
                    "SessionClose" => Some("session.close.committed"),
                    "SessionDetach" => Some("session.detach.committed"),
                    "SessionExpire" => Some("session.expire.committed"),
                    "Admission" => Some("admission.committed"),
                    "RetainedQoS0" => Some("retained.committed"),
                    "Subscriptions" => Some("subscription.committed"),
                    "Claim" => Some("claim.committed"),
                    "Finish" => Some("finish.committed"),
                    "Consume" => Some("consume.committed"),
                    "DeliveryStarted" => Some("delivery.started.committed"),
                    "MessageExpire" => Some("message.delivery.expire.committed"),
                    _ => None,
                };
                if applied.committed_now
                    && let Some(point) = point
                    && hooks.barrier(point, kind, id, events, cancel).is_err()
                {
                    cancel.store(true, Ordering::Relaxed);
                }
            }
            record_result(recipe, &applied, events, cancel);
            #[cfg(not(feature = "test-hooks"))]
            let reply = applied.reply;
            #[cfg(feature = "test-hooks")]
            let mut reply = applied.reply;
            #[cfg(feature = "test-hooks")]
            if applied.committed_now
                && matches!(
                    kind,
                    "WillAdmission"
                        | "WillEnd"
                        | "Claim"
                        | "Admission"
                        | "RetainedQoS0"
                        | "Subscriptions"
                        | "Finish"
                        | "Consume"
                        | "SessionOpen"
                        | "SessionClose"
                        | "SessionDetach"
                        | "SessionExpire"
                        | "DeliveryStarted"
                        | "MessageExpire"
                )
                && let Some(dir) = &hooks.directory
            {
                let file_name = match kind {
                    "WillAdmission" => "drop-next-will-admission-result".to_owned(),
                    "WillEnd" => "drop-next-will-end-result".to_owned(),
                    "SessionOpen" => "drop-next-session-open-result".to_owned(),
                    "SessionClose" => "drop-next-session-close-result".to_owned(),
                    "SessionDetach" => "drop-next-session-detach-result".to_owned(),
                    "SessionExpire" => "drop-next-session-expire-result".to_owned(),
                    "DeliveryStarted" => "drop-next-delivery-started-result".to_owned(),
                    "MessageExpire" => "drop-next-message-expire-delivery-result".to_owned(),
                    _ => format!("drop-next-{}-result", kind.to_ascii_lowercase()),
                };
                let file = dir.join(file_name);
                if file.exists() && std::fs::remove_file(file).is_ok() {
                    let _ = events.emit(
                        "store_result_dropped",
                        json!({"operation":operation(id),"command_kind":kind}),
                    );
                    reply = StoreReply::Outcome(
                        mqttiumd_contract::outcome::StoreOutcome::PendingOrUnknown,
                    );
                }
            }
            #[cfg(feature = "test-hooks")]
            let reply = hold_business_reply(state, hooks, id, kind, reply, events);
            Response::Store {
                effect,
                operation: id,
                reply,
                targets,
            }
        }
        Effect::ResolveAndFence { operation: id } => {
            let reply = StoreReply::Outcome(store.resolve_and_fence(id).outcome);
            #[cfg(feature = "test-hooks")]
            let reply = hold_business_reply(state, hooks, id, "Resolution", reply, events);
            if events.emit("store_resolved",json!({"operation":operation(id),"details":{"outcome":outcome_name(reply).to_ascii_lowercase()}})).is_err(){cancel.store(true,Ordering::Relaxed);}
            Response::Store {
                effect,
                operation: id,
                reply,
                targets: Vec::new(),
            }
        }
        Effect::LoadWills { after, limit } => Response::Wills {
            effect,
            result: store.read_wills(after, limit as usize, ReadBudget::PAGE),
        },
        Effect::LoadWork {
            session,
            after,
            limit,
        } => {
            // A one-shot corruption-control barrier, before any read
            // transaction. The exclusive writer is idle here: an
            // external test mutation cannot race maintenance's BEGIN.
            #[cfg(feature = "test-hooks")]
            {
                let armed = hooks
                    .directory
                    .as_ref()
                    .is_some_and(|directory| consume_file(&directory.join("arm-work-read")));
                if armed
                            && hooks
                                .barrier_fields(
                                    "work.read.before",
                                    json!({"point":"work.read.before","details":{
                                        "session":session.get().to_string(),
                                        "effect":format!("{}/{}/{}",effect.epoch.get(),effect.owner.get(),effect.sequence.get())
                                    }}),
                                    events,
                                    cancel,
                                )
                                .is_err()
                        {
                            cancel.store(true, Ordering::Relaxed);
                            return Response::Work {
                                effect,
                                session,
                                result: Err(ReadError::Unavailable),
                            };
                        }
            }
            Response::Work {
                effect,
                session,
                result: store.read_work(session, after, limit as usize),
            }
        }
        _ => {
            cancel.store(true, Ordering::Relaxed);
            Response::Stopped
        }
    }
}

#[cfg(feature = "test-hooks")]
fn hold_business_reply(
    state: &mut BusinessState,
    hooks: &Hooks,
    id: OperationId,
    kind: &str,
    reply: StoreReply,
    events: &Events,
) -> StoreReply {
    let reply = hold_admission_reply(state, hooks, id, kind, reply, events);
    for (name, file, held) in [
        (
            "WillAdmission",
            "hold-will-admission-result",
            &mut state.held_will_admission,
        ),
        ("WillEnd", "hold-will-end-result", &mut state.held_will_end),
        (
            "SessionOpen",
            "hold-session-open-result",
            &mut state.held_session_open,
        ),
        (
            "SessionClose",
            "hold-session-close-result",
            &mut state.held_session_close,
        ),
        (
            "SessionDetach",
            "hold-session-detach-result",
            &mut state.held_session_detach,
        ),
        (
            "SessionExpire",
            "hold-session-expire-result",
            &mut state.held_session_expire,
        ),
        ("Claim", "hold-claim-result", &mut state.held_claim),
        ("Finish", "hold-finish-result", &mut state.held_finish),
        (
            "DeliveryPubrelReady",
            "hold-pubrel-ready-result",
            &mut state.held_pubrel_ready,
        ),
    ] {
        let active = hooks
            .directory
            .as_ref()
            .is_some_and(|dir| dir.join(file).exists());
        if !active {
            *held = None;
            continue;
        }
        if kind == name
            && held.is_none()
            && matches!(
                reply,
                StoreReply::Outcome(mqttiumd_contract::outcome::StoreOutcome::Committed(_))
            )
        {
            *held = Some(id);
        }
        if *held == Some(id) {
            let _=events.emit("store_result_held",json!({"operation":operation(id),"command_kind":name,"details":{"outcome":outcome_name(reply),"source":kind}}));
            return StoreReply::Outcome(mqttiumd_contract::outcome::StoreOutcome::PendingOrUnknown);
        }
    }
    reply
}
#[cfg(feature = "test-hooks")]
fn hold_admission_reply(
    state: &mut BusinessState,
    hooks: &Hooks,
    id: OperationId,
    kind: &str,
    reply: StoreReply,
    events: &Events,
) -> StoreReply {
    let hold = hooks
        .directory
        .as_ref()
        .is_some_and(|d| d.join("hold-admission-result").exists());
    if !hold {
        state.held_admission = None;
        return reply;
    }
    if kind == "Admission"
        && state.held_admission.is_none()
        && matches!(
            reply,
            StoreReply::Outcome(mqttiumd_contract::outcome::StoreOutcome::Committed(_))
        )
    {
        state.held_admission = Some(id);
    }
    if state.held_admission == Some(id) {
        let _ = events.emit("store_result_held",json!({"operation":operation(id),"command_kind":"Admission","details":{"outcome":outcome_name(reply),"source":kind}}));
        StoreReply::Outcome(mqttiumd_contract::outcome::StoreOutcome::PendingOrUnknown)
    } else {
        reply
    }
}
#[cfg(feature = "test-hooks")]
fn identity_text(incarnation: u64, epoch: u64, owner: u64, sequence: u64) -> String {
    format!("{incarnation}/{epoch}/{owner}/{sequence}")
}

#[cfg(feature = "test-hooks")]
fn replay_controls(
    store: &mut StoreBackend,
    state: &BusinessState,
    hooks: &Hooks,
    events: &Events,
) -> Result<(), Failure> {
    use mqttiumd_contract::command::{Command, FrozenRecipe};
    use mqttiumd_contract::ids::{
        ExchangeGeneration, OperationSeq, PayloadId, PayloadSeq, PublicationId, PublicationSeq,
        RequestId, RequestSeq,
    };
    let Some(directory) = &hooks.directory else {
        return Ok(());
    };
    for mode in ["same", "different", "publication", "payload"] {
        if !consume_file(&directory.join(format!("replay-retired-{mode}"))) {
            continue;
        }
        let captured = state
            .captured
            .as_ref()
            .ok_or_else(|| Failure::plain("receipt_replay_capture_missing"))?;
        let before = store
            .snapshot()
            .map_err(|e| Failure::read(e, ReadPhase::Snapshot))?;
        let mut bytes = Vec::new();
        captured
            .recipe
            .get()
            .encode_canonical(&mut bytes)
            .map_err(|_| Failure::plain("receipt_replay_recipe"))?;
        let mut recipe =
            Recipe::decode_canonical(&bytes, limits().max_targets_per_admission, MAX_TOPIC)
                .map_err(|_| Failure::plain("receipt_replay_recipe"))?;
        let admission = match &mut recipe {
            Recipe::AdmitPublication(a) => a,
            Recipe::ClaimedPublication(a) => &mut a.admission,
            Recipe::RetainedPublication(a) => &mut a.admission,
            _ => return Err(Failure::plain("receipt_replay_kind")),
        };
        let old = admission.operation;
        let origin = mqttiumd_contract::receipts::ReceiptOrigin::of(old);
        let frontier = before
            .receipt_frontiers
            .get(&origin)
            .ok_or_else(|| Failure::plain("receipt_replay_origin_missing"))?;
        // These probes target retired identities, never a remaining live key or
        // an unrelated stale routing/session guard. They run at the end of an
        // isolated reception: no later owner command may use these new IDs.
        if frontier.through < old.sequence()
            || before.operations.contains_key(&old)
            || before.publications.contains_key(&admission.publication)
            || before.payloads.contains_key(&admission.payload.id)
        {
            return Err(Failure::plain("receipt_replay_not_retired"));
        }
        if mode == "different" {
            admission.topic = SharedBytes::copy_from(b"replay/different", MAX_TOPIC)
                .map_err(|_| Failure::plain("receipt_replay_recipe"))?;
        } else if matches!(mode, "publication" | "payload") {
            let next_op = before
                .operations
                .keys()
                .chain(before.fences.iter())
                .filter(|id| mqttiumd_contract::receipts::ReceiptOrigin::of(**id) == origin)
                .map(|id| id.sequence().get())
                .chain([frontier.through.get()])
                .max()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| Failure::plain("receipt_replay_exhausted"))?;
            let mut next_publication = frontier.publication_through.get();
            let mut next_payload = frontier.payload_through.get();
            // The recent recipes above the closed prefix also own identities.
            // Refresh the unrelated identity above both the compact guard and
            // every retained recipe, even when its business object was freed.
            for record in before.operations.values() {
                let known = Recipe::decode_canonical(
                    &record.recipe,
                    limits().max_targets_per_admission,
                    MAX_TOPIC,
                )
                .map_err(|_| Failure::plain("receipt_replay_recipe"))?;
                if mqttiumd_contract::receipts::ReceiptOrigin::of(known.operation()) != origin {
                    continue;
                }
                let (publication, payload) = recipe_identity_highwater(&known)?;
                next_publication = next_publication.max(publication);
                next_payload = next_payload.max(payload);
            }
            next_publication = next_publication
                .checked_add(1)
                .ok_or_else(|| Failure::plain("receipt_replay_exhausted"))?;
            next_payload = next_payload
                .checked_add(1)
                .ok_or_else(|| Failure::plain("receipt_replay_exhausted"))?;
            admission.operation = OperationId::new(
                origin.incarnation,
                origin.epoch,
                origin.owner,
                OperationSeq::new(next_op),
            );
            admission.request = RequestId::new(
                origin.incarnation,
                origin.epoch,
                origin.owner,
                RequestSeq::new(next_op),
            );
            admission.route_revision = before.route_revision;
            admission.source.session_revision = before
                .sessions
                .get(&admission.source.session)
                .filter(|s| s.state == SessionState::Active)
                .ok_or_else(|| Failure::plain("receipt_replay_source_missing"))?
                .revision;
            admission.source.exchange.generation = ExchangeGeneration::new(next_op);
            if mode == "publication" {
                admission.payload.id = PayloadId::new(
                    origin.incarnation,
                    origin.epoch,
                    origin.owner,
                    PayloadSeq::new(next_payload),
                );
            } else {
                admission.publication = PublicationId::new(
                    origin.incarnation,
                    origin.epoch,
                    origin.owner,
                    PublicationSeq::new(next_publication),
                );
            }
            for target in admission.targets.as_mut_slice() {
                target.delivery.publication = admission.publication;
                target.session_revision = before
                    .sessions
                    .get(&target.session)
                    .filter(|s| s.state == SessionState::Active)
                    .ok_or_else(|| Failure::plain("receipt_replay_target_missing"))?
                    .revision;
            }
        }
        let probe = Command {
            recipe: FrozenRecipe::freeze(recipe)
                .map_err(|_| Failure::plain("receipt_replay_recipe"))?,
            payload: captured.payload.clone(),
        };
        let applied = store.apply(&probe);
        let after = store
            .snapshot()
            .map_err(|e| Failure::read(e, ReadPhase::Snapshot))?;
        let business_changed = before.publications != after.publications
            || before.payloads != after.payloads
            || before.obligations != after.obligations
            || before.inbound != after.inbound
            || before.outbound != after.outbound
            || before.accounts.references != after.accounts.references
            || before.accounts.payload_bytes != after.accounts.payload_bytes
            || before.accounts.exchange_slots != after.accounts.exchange_slots;
        let reason = match applied.reply {
            StoreReply::Outcome(mqttiumd_contract::outcome::StoreOutcome::RejectedCertain(
                reason,
            )) => format!("{reason:?}"),
            _ => String::new(),
        };
        events.emit("receipt_replay_result",json!({"operation":operation(probe.recipe.operation()),"command_kind":"Admission","details":{"mode":mode,"outcome":outcome_name(applied.reply),"reason":reason,"business_changed":business_changed,"wrote":applied.wrote,"committed_now":applied.committed_now,"before_receipts":before.accounts.receipt_slots.get().to_string(),"after_receipts":after.accounts.receipt_slots.get().to_string(),"before_references":before.accounts.references.get().to_string(),"after_references":after.accounts.references.get().to_string(),"before_payload_bytes":before.accounts.payload_bytes.get().to_string(),"after_payload_bytes":after.accounts.payload_bytes.get().to_string(),"before_publications":before.publications.len(),"after_publications":after.publications.len(),"before_payloads":before.payloads.len(),"after_payloads":after.payloads.len(),"before_obligations":before.obligations.len(),"after_obligations":after.obligations.len(),"current_route_revision":before.route_revision.get().to_string(),"closure_through":frontier.through.get().to_string(),"publication_through":frontier.publication_through.get().to_string(),"payload_through":frontier.payload_through.get().to_string()}})).map_err(|_|Failure::plain("events_unavailable"))?;
    }
    Ok(())
}

fn control_response(
    store: &mut StoreBackend,
    request: ControlRequest,
    events: &Events,
    hooks: &Hooks,
    cancel: &Arc<AtomicBool>,
    state: &mut BusinessState,
) -> ControlResponse {
    let _ = hooks;
    match request {
        ControlRequest::WillBarrier { connection, client } => ControlResponse::WillBarrier {
            connection,
            result: store
                .has_will_for_client(client.as_slice())
                .map_err(|error| Failure::read(error, ReadPhase::Lookup)),
        },
        ControlRequest::MessageExpiry { now, after, limit } => {
            #[cfg(feature = "test-hooks")]
            {
                let hc = hooks.clone();
                let ev = events.clone();
                let stop = cancel.clone();
                let hook = Box::new(move |point| {
                    if point == mqttiumd_store_sqlite::kill_points::KillPoint::CommitPrepared
                        && hc.barrier_fields("message.expire.prepared", json!({"point":"message.expire.prepared","command_kind":"MessageExpiry","details":message_expiry_details(now, after)}), &ev, &stop).is_err() {
                        stop.store(true, Ordering::Relaxed);
                    }
                });
                if store.set_kill_hook(Some(hook)).is_err() {
                    cancel.store(true, Ordering::Relaxed);
                }
            }
            let result = store
                .expire_messages(now, after, limit)
                .map(Arc::new)
                .map_err(|error| Failure::read(error, ReadPhase::Maintenance));
            #[cfg(feature = "test-hooks")]
            {
                let _ = store.set_kill_hook(None);
            }
            if let Ok(sweep) = &result {
                #[cfg(feature = "test-hooks")]
                if (!sweep.removed.is_empty() || sweep.retained_removed > 0)
                    && hooks.barrier_fields("message.expire.committed", json!({"point":"message.expire.committed","command_kind":"MessageExpiry","details":message_expiry_details(now, after)}), events, cancel).is_err() {
                    cancel.store(true, Ordering::Relaxed);
                }
                for removed in sweep.removed.as_slice() {
                    let publication = removed.delivery.publication;
                    if events.emit("message_expiry_removed",json!({"details":{
                        "source":"durable_commit","publication":format!("{}/{}/{}/{}",publication.incarnation().get(),publication.epoch().get(),publication.owner().get(),publication.sequence().get()),
                        "index":removed.delivery.index,"target":removed.target.get().to_string()
                    }})).is_err() { return ControlResponse::Failed(Failure::plain("events_unavailable")); }
                }
                let removed = sweep.removed.len();
                if (removed != 0 || sweep.retained_removed > 0 || sweep.quarantined > 0)
                    && events.emit("message_expiry_sweep", json!({"details":{
                        "source":"durable_commit","domain":hex_domain(now.domain),"tick":now.tick.to_string(),
                        "removed":removed,"retained_removed":sweep.retained_removed,
                        "quarantined":sweep.quarantined,"more":sweep.more
                    }})).is_err() {
                    return ControlResponse::Failed(Failure::plain("events_unavailable"));
                }
            }
            #[cfg(feature = "test-hooks")]
            if result.is_ok()
                && hooks
                    .directory
                    .as_ref()
                    .is_some_and(|dir| consume_file(&dir.join("drop-next-message-expire-result")))
            {
                let _ = events.emit(
                    "message_expiry_result_dropped",
                    json!({"details":message_expiry_details(now, after)}),
                );
                return ControlResponse::MessageExpiryUnknown { now, after };
            }
            ControlResponse::MessageExpiry { now, after, result }
        }
        ControlRequest::ClockCheckpoint { expected, reading } => {
            let result = store
                .checkpoint_session_clock(Some(expected), reading)
                .map_err(|error| Failure::read(error, ReadPhase::Maintenance));
            if let Ok(anchor) = result {
                let _ = events.emit("session_clock_checkpoint", json!({"details": {
                    "logical": anchor.logical.to_string(), "tick": anchor.reading.tick.to_string(),
                    "same_domain": expected.reading.domain == anchor.reading.domain,
                    "source": "maintenance_commit"
                }}));
            }
            ControlResponse::ClockCheckpoint {
                expected,
                reading,
                result,
            }
        }
        ControlRequest::Lifecycle(effect) => {
            match business_response(store, Request::Effect(effect), events, hooks, cancel, state) {
                Response::Store {
                    effect,
                    operation,
                    reply,
                    ..
                } => ControlResponse::Lifecycle {
                    effect,
                    operation,
                    reply,
                },
                _ => {
                    cancel.store(true, Ordering::Relaxed);
                    ControlResponse::Failed(Failure::plain("invalid_lifecycle_control"))
                }
            }
        }
        ControlRequest::Closed(collection) => ControlResponse::Closed {
            collection,
            result: store
                .collect_closed_receipts(collection)
                .map_err(|e| Failure::read(e, ReadPhase::Maintenance)),
        },
        ControlRequest::Collect { effect, collection } => {
            #[cfg(not(feature = "test-hooks"))]
            let effective = collection;
            #[cfg(feature = "test-hooks")]
            let effective = ReceiptCollection {
                limit: if hooks
                    .directory
                    .as_ref()
                    .is_some_and(|d| d.join("receipt-collection-limit-one").exists())
                {
                    1
                } else {
                    collection.limit
                },
                ..collection
            };
            #[cfg(feature = "test-hooks")]
            {
                let hc = hooks.clone();
                let ev = events.clone();
                let stop = cancel.clone();
                store.set_receipt_hook(Some(Box::new(move |point, current| {
                    let mut details = collection_details(current);
                    details["requested_limit"] = json!(collection.limit);
                    if current.closure.through.get()!=0 && hc.barrier_fields(point.name(), json!({"point":point.name(),"command_kind":"ReceiptCollection","details":details}), &ev, &stop).is_err() { stop.store(true, Ordering::Relaxed); }
                })));
            }
            let result = store
                .collect_receipts(effective)
                .map_err(|e| Failure::read(e, ReadPhase::Maintenance));
            #[cfg(feature = "test-hooks")]
            store.set_receipt_hook(None);
            let mut details = collection_details(effective);
            details["requested_limit"] = json!(collection.limit);
            details["effect"] = json!(format!(
                "{}/{}/{}",
                effect.epoch.get(),
                effect.owner.get(),
                effect.sequence.get()
            ));
            match result {
                Ok(ReceiptCollectionReply::Applied {
                    closure,
                    removed,
                    retained,
                    more,
                }) => {
                    returned_closure_details(&mut details, closure);
                    details["outcome"] = json!("Applied");
                    details["removed"] = json!(removed);
                    details["retained"] = json!(retained.to_string());
                    details["more"] = json!(more);
                }
                Ok(ReceiptCollectionReply::PendingOrUnknown) => {
                    details["outcome"] = json!("PendingOrUnknown")
                }
                Ok(ReceiptCollectionReply::Refused(reason)) => {
                    details["outcome"] = json!("Refused");
                    details["reason"] = json!(format!("{reason:?}"));
                }
                Err(ref failure) => {
                    details["outcome"] = json!("ReadError");
                    if let Some((cause, _)) = failure.read {
                        details["cause"] = json!(cause.code());
                    }
                }
            }
            if events
                .emit(
                    "receipt_collection_result",
                    json!({"command_kind":"ReceiptCollection","details":details}),
                )
                .is_err()
            {
                cancel.store(true, Ordering::Relaxed);
            }
            #[cfg(feature = "test-hooks")]
            let result = if matches!(result, Ok(ReceiptCollectionReply::Applied { .. }))
                && hooks
                    .directory
                    .as_ref()
                    .is_some_and(|d| consume_file(&d.join("drop-next-close-result")))
            {
                let _ = events.emit(
                    "receipt_collection_result_dropped",
                    json!({"command_kind":"ReceiptCollection","details":details}),
                );
                Ok(ReceiptCollectionReply::PendingOrUnknown)
            } else {
                result
            };
            ControlResponse::Collected {
                effect,
                collection,
                result,
            }
        }
    }
}
#[cfg(feature = "test-hooks")]
fn consume_file(path: &Path) -> bool {
    path.exists() && std::fs::remove_file(path).is_ok()
}
#[cfg(feature = "test-hooks")]
fn recipe_identity_highwater(recipe: &Recipe) -> Result<(u64, u64), Failure> {
    if let Recipe::WillPublication(value) = recipe {
        return Ok((
            value.publication.sequence().get(),
            value.payload.id.sequence().get(),
        ));
    }
    if let Some(a) = recipe.admission() {
        return Ok((
            a.publication.sequence().get(),
            a.payload.id.sequence().get(),
        ));
    }
    if let Some(a) = recipe.retained_qos0() {
        return Ok((
            a.publication.sequence().get(),
            a.payload.id.sequence().get(),
        ));
    }
    if let Some(a) = recipe.replay_subscriptions() {
        return Ok((
            a.first_publication
                .sequence()
                .get()
                .checked_add(mqttiumd_contract::retained::MAX_REPLAYS as u64 - 1)
                .ok_or_else(|| Failure::plain("receipt_replay_exhausted"))?,
            0,
        ));
    }
    Ok((0, 0))
}

#[cfg(test)]
fn bootstrap(path: &Path) -> Result<(StoreBackend, Boot), Failure> {
    let clock = Arc::new(SystemClock::new().map_err(|error| Failure::plain(error.code()))?);
    bootstrap_with_clock(path, clock)
}

#[cfg(test)]
fn bootstrap_with_clock(
    path: &Path,
    clock: Arc<dyn SessionClock>,
) -> Result<(StoreBackend, Boot), Failure> {
    bootstrap_bound(path, clock, None, false, StorageMode::Durable)
}

fn create_store(
    path: &Path,
    policy: Option<&mqttiumd_store_sqlite::StoragePolicy>,
) -> Result<SqliteStore, Failure> {
    let mut random = [0u8; 8];
    getrandom::fill(&mut random).map_err(|_| Failure::plain("random_unavailable"))?;
    let incarnation = StoreIncarnation::new(u64::from_be_bytes(random).max(1));
    match policy {
        Some(policy) => SqliteStore::create_secured(path, incarnation, limits(), policy),
        None => SqliteStore::create(path, incarnation, limits()),
    }
    .map_err(database_open_error)
}

fn database_open_error(error: mqttiumd_store_sqlite::OpenError) -> Failure {
    if matches!(
        error,
        mqttiumd_store_sqlite::OpenError::SecurityPolicyMismatch
    ) {
        Failure::plain("security_policy_mismatch")
    } else {
        Failure::read(error.read_error(), ReadPhase::Open)
    }
}

/// Creates the durable secure schema without opening an epoch or listener.
pub fn initialize_secured(
    path: &Path,
    policy: &mqttiumd_store_sqlite::StoragePolicy,
) -> Result<(), Failure> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| Failure::read(io_cause(error.kind()), ReadPhase::Open))?;
    drop(create_store(path, Some(policy))?);
    Ok(())
}

fn bootstrap_bound(
    path: &Path,
    clock: Arc<dyn SessionClock>,
    policy: Option<mqttiumd_store_sqlite::StoragePolicy>,
    require_existing: bool,
    storage_mode: StorageMode,
) -> Result<(StoreBackend, Boot), Failure> {
    let mut store = match storage_mode {
        StorageMode::Durable => {
            // Existing files, including empty/truncated files, are never silently initialised.
            let exists = path
                .try_exists()
                .map_err(|e| Failure::read(io_cause(e.kind()), ReadPhase::Open))?;
            if !exists && require_existing {
                return Err(Failure::plain("existing_database_missing"));
            }
            let sqlite = if exists {
                match policy.as_ref() {
                    Some(policy) => SqliteStore::open_secured(path, policy),
                    None => SqliteStore::open(path),
                }
                .map_err(database_open_error)?
            } else {
                create_store(path, policy.as_ref())?
            };
            StoreBackend::Durable(sqlite)
        }
        StorageMode::Log => {
            let exists = path
                .try_exists()
                .map_err(|e| Failure::read(io_cause(e.kind()), ReadPhase::Open))?;
            if !exists && require_existing {
                return Err(Failure::plain("existing_database_missing"));
            }
            let mut random = [0u8; 8];
            getrandom::fill(&mut random).map_err(|_| Failure::plain("random_unavailable"))?;
            let incarnation = StoreIncarnation::new(u64::from_be_bytes(random).max(1));
            let log_store = mqttiumd_store_volatile::LogStore::open_or_create(
                path,
                incarnation,
                limits(),
                policy,
                require_existing,
            )
            .map_err(database_open_error)?;
            StoreBackend::Log(log_store)
        }
        StorageMode::Volatile {
            snapshot_interval: None,
        } if !require_existing => {
            // Pure in-memory volatile mode (`persistence false` or `--storage-mode volatile`
            // without snapshots or `--existing-db`) never reads, locks, or overwrites any
            // database file on disk.
            let mut random = [0u8; 8];
            getrandom::fill(&mut random).map_err(|_| Failure::plain("random_unavailable"))?;
            let incarnation = StoreIncarnation::new(u64::from_be_bytes(random).max(1));
            let handle =
                mqttiumd_store_volatile::VolatileHandle::in_memory(incarnation, limits(), policy)
                    .map_err(database_open_error)?;
            StoreBackend::Volatile {
                handle,
                snapshot_interval: None,
                last_snapshot: Instant::now(),
                dirty: false,
            }
        }
        StorageMode::Volatile { snapshot_interval } => {
            let exists = path
                .try_exists()
                .map_err(|e| Failure::read(io_cause(e.kind()), ReadPhase::Open))?;
            if !exists && require_existing {
                return Err(Failure::plain("existing_database_missing"));
            }
            let mut random = [0u8; 8];
            getrandom::fill(&mut random).map_err(|_| Failure::plain("random_unavailable"))?;
            let incarnation = StoreIncarnation::new(u64::from_be_bytes(random).max(1));
            let handle = mqttiumd_store_volatile::VolatileHandle::open_or_create(
                path,
                incarnation,
                limits(),
                policy,
                require_existing,
            )
            .map_err(database_open_error)?;
            StoreBackend::Volatile {
                handle,
                snapshot_interval,
                last_snapshot: Instant::now(),
                dirty: !exists,
            }
        }
    };
    let state = store
        .snapshot()
        .map_err(|e| Failure::read(e, ReadPhase::Snapshot))?;
    if state.limits != limits() {
        return Err(Failure::plain("durable_profile_mismatch"));
    }
    let route_count = state
        .routes
        .values()
        .try_fold(0usize, |n, targets| n.checked_add(targets.len()))
        .ok_or_else(|| Failure::read(ReadError::LimitExceeded, ReadPhase::Snapshot))?;
    if route_count > mqttiumd_contract::pubsub::MAX_SUBSCRIPTIONS {
        return Err(Failure::read(ReadError::LimitExceeded, ReadPhase::Snapshot));
    }
    // Establish the logical domain before new-epoch cleanup derives any
    // deadline. A new domain keeps only committed progress; existing detached
    // deadlines remain immutable. Typed snapshot validation above precedes it.
    let reading = clock.read().map_err(|error| Failure::plain(error.code()))?;
    let clock_anchor = store
        .checkpoint_session_clock(state.session_clock, reading)
        .map_err(|error| Failure::read(error, ReadPhase::Maintenance))?;
    if clock_anchor.reading != reading {
        return Err(Failure::plain("session_clock_startup_cas"));
    }
    let ledger = store
        .ledger()
        .map_err(|e| Failure::read(e, ReadPhase::Ledger))?;
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).map_err(|_| Failure::plain("random_unavailable"))?;
    let mut opening = EpochOpening::new(ledger.incarnation(), StartupNonce::new(nonce));
    let command = opening
        .submit(ledger.last_opened())
        .map_err(|_| Failure::plain("opening_submit"))?;
    let outcome = store
        .open_epoch(&command)
        .map_err(|e| Failure::read(e, ReadPhase::Opening))?;
    let _ = opening
        .observe(OpeningReply { command, outcome })
        .map_err(|_| Failure::plain("opening_reply"))?;
    if !matches!(opening.stage(), OpeningStage::Committed(_)) {
        return Err(Failure::plain("opening_unknown"));
    }
    let grant = opening
        .hand_over()
        .map_err(|_| Failure::plain("opening_grant"))?;
    // Epoch opening atomically expires previous zero-retention transports.
    // The first snapshot validated all durable discriminants before cleanup;
    // only the post-opening state supplies recovery/cache authority.
    let state = store
        .snapshot()
        .map_err(|e| Failure::read(e, ReadPhase::Snapshot))?;
    let mut sessions = BTreeMap::new();
    for (id, s) in &state.sessions {
        if s.state != SessionState::Active {
            continue;
        }
        let client = SharedBytes::copy_from(&s.client_id, MAX_CLIENT)
            .map_err(|_| Failure::read(ReadError::LimitExceeded, ReadPhase::Snapshot))?;
        if sessions.len() >= SESSIONS {
            return Err(Failure::read(ReadError::LimitExceeded, ReadPhase::Snapshot));
        }
        sessions.insert(
            client.clone(),
            Session {
                id: *id,
                revision: s.revision,
                client,
                present: true,
                expiry: s.expiry,
                opened_by: s.opened_by,
                detached: s.detached.map(Box::new),
            },
        );
    }
    let collect_closed =
        state.receipt_frontiers.len() >= 32 || state.accounts.receipt_slots.get() >= 4096;
    let sqlite = match &store {
        StoreBackend::Durable(s) => Some(s.effective_settings().clone()),
        StoreBackend::Log(s) => s.sqlite_settings().cloned(),
        StoreBackend::Volatile { handle, .. } => handle.sqlite_settings().cloned(),
    };
    Ok((
        store,
        Boot {
            grant,
            sessions,
            collect_closed,
            sqlite,
            storage_mode,
            clock,
            clock_anchor,
        },
    ))
}
fn session(store: &mut StoreBackend, client: SharedBytes) -> Result<Session, Failure> {
    let state = store
        .snapshot()
        .map_err(|e| Failure::read(e, ReadPhase::Lookup))?;
    if let Some((id, s)) = state
        .sessions
        .iter()
        .find(|(_, s)| s.state == SessionState::Active && s.client_id == client.as_slice())
    {
        return Ok(Session {
            id: *id,
            revision: s.revision,
            client,
            present: true,
            expiry: s.expiry,
            opened_by: s.opened_by,
            detached: s.detached.map(Box::new),
        });
    }
    match store.admin(AdminCommand::CreateSession {
        client_id: client.as_slice().to_vec(),
    }) {
        Ok(AdminDone::SessionCreated { session, revision }) => Ok(Session {
            id: session,
            revision,
            client,
            present: false,
            expiry: SessionExpiry::Never,
            opened_by: None,
            detached: None,
        }),
        // WriteFailed has an unknown outcome. No retry and no success acknowledgement.
        Err(mqttiumd_store_api::AdminRefused::WriteFailed) => {
            Err(Failure::plain("session_creation_unknown"))
        }
        _ => Err(Failure::plain("session_creation_refused")),
    }
}
fn outcome_name(reply: StoreReply) -> &'static str {
    use mqttiumd_contract::outcome::StoreOutcome::*;
    match reply {
        StoreReply::Outcome(Committed(_)) => "Committed",
        StoreReply::Outcome(RejectedCertain(_)) => "RejectedCertain",
        StoreReply::Outcome(PendingOrUnknown) => "PendingOrUnknown",
        StoreReply::Outcome(RetiredOutcomeUnavailable) => "RetiredOutcomeUnavailable",
        StoreReply::Violation(_) => "Violation",
    }
}

fn io_cause(kind: std::io::ErrorKind) -> ReadError {
    use std::io::ErrorKind;
    match kind {
        ErrorKind::PermissionDenied | ErrorKind::WouldBlock | ErrorKind::NotFound => {
            ReadError::Unavailable
        }
        ErrorKind::OutOfMemory => ReadError::AllocationFailed,
        _ => ReadError::Io,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mqttiumd_contract::ids::{EffectSeq, LogicalOwnerId};
    use mqttiumd_contract::receipts::{ReceiptClosure, ReceiptOrigin};
    use std::sync::atomic::AtomicU64;
    use std::time::Instant;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    #[test]
    fn real_will_install_cancel_pending_and_read_barriers_are_correlated() {
        use mqttiumd_contract::command::{Command, FrozenRecipe};
        use mqttiumd_contract::ids::{OperationSeq, RunEpoch};
        use mqttiumd_contract::outcome::{CommitFacts, StoreOutcome};
        use mqttiumd_contract::session::{SessionChange, SessionGuard, SessionLifecycle};
        use mqttiumd_contract::will::{WillSpec, WillState};
        let dir = std::env::temp_dir().join(format!(
            "mqttiumd-will-writer-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&dir).unwrap();
        let (mut store, boot) = bootstrap(&dir.join("store.sqlite")).unwrap();
        let events = Events::open(&dir.join("events.jsonl")).unwrap();
        let cancel = Arc::new(AtomicBool::new(false));
        let hooks = Hooks::default();
        let mut state = BusinessState::default();
        let connection = ConnectionGeneration::new(7);
        let client = SharedBytes::copy_from(b"sensor", MAX_CLIENT).unwrap();
        let operation = |n| {
            OperationId::new(
                boot.grant.incarnation(),
                boot.grant.epoch(),
                LogicalOwnerId::new(0),
                OperationSeq::new(n),
            )
        };
        let effect = |n| EffectId {
            epoch: boot.grant.epoch(),
            owner: LogicalOwnerId::new(0),
            sequence: EffectSeq::new(n),
        };
        let lifecycle = |id, action| {
            OwnedEffect::new(
                effect(id),
                Effect::Persist {
                    command: Command {
                        recipe: FrozenRecipe::freeze(Recipe::SessionLifecycle(SessionLifecycle {
                            operation: operation(id),
                            action,
                        }))
                        .unwrap(),
                        payload: None,
                    },
                },
            )
        };
        let spec = WillSpec {
            retain: false,
            topic: SharedBytes::copy_from(b"status", MAX_TOPIC).unwrap(),
            payload: SharedBytes::copy_from(b"SECRET-WILL", MAX_PAYLOAD).unwrap(),
        };
        let open = |spec| SessionAction::OpenWillAt {
            client_id: client.clone(),
            expected: None,
            clean_start: false,
            expiry: SessionExpiry::OnDisconnect,
            now: 0,
            will: Some(spec),
        };
        let first = control_response(
            &mut store,
            ControlRequest::Lifecycle(lifecycle(1, open(spec.clone()))),
            &events,
            &hooks,
            &cancel,
            &mut state,
        );
        let session = match first {
            ControlResponse::Lifecycle {
                operation: id,
                reply:
                    StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                        SessionChange::Opened { session, .. },
                    ))),
                ..
            } if id == operation(1) => session,
            _ => panic!("real Will Open did not commit"),
        };
        let barrier = control_response(
            &mut store,
            ControlRequest::WillBarrier {
                connection,
                client: client.clone(),
            },
            &events,
            &hooks,
            &cancel,
            &mut state,
        );
        assert!(
            matches!(barrier,ControlResponse::WillBarrier {connection:c,result:Ok(true)} if c==connection),
            "installed Will must hold successor Open barrier"
        );
        let end = |guard, publish| SessionAction::EndWill {
            guard,
            expiry: SessionExpiry::OnDisconnect,
            observed_at: 1,
            publish,
        };
        let normal = control_response(
            &mut store,
            ControlRequest::Lifecycle(lifecycle(
                2,
                end(
                    SessionGuard {
                        session,
                        opened_by: Some(operation(1)),
                    },
                    false,
                ),
            )),
            &events,
            &hooks,
            &cancel,
            &mut state,
        );
        assert!(
            matches!(
                normal,
                ControlResponse::Lifecycle {
                    reply: StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::SessionLifecycle(SessionChange::Closed { .. })
                    )),
                    ..
                }
            ),
            "normal Will cancellation must commit with cleanup"
        );
        assert!(
            matches!(
                control_response(
                    &mut store,
                    ControlRequest::WillBarrier {
                        connection,
                        client: client.clone()
                    },
                    &events,
                    &hooks,
                    &cancel,
                    &mut state
                ),
                ControlResponse::WillBarrier {
                    result: Ok(false),
                    ..
                }
            ),
            "cancelled intention must release successor barrier"
        );
        let second = control_response(
            &mut store,
            ControlRequest::Lifecycle(lifecycle(3, open(spec))),
            &events,
            &hooks,
            &cancel,
            &mut state,
        );
        let next_session = match second {
            ControlResponse::Lifecycle {
                reply:
                    StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                        SessionChange::Opened { session, .. },
                    ))),
                ..
            } => session,
            _ => panic!("second real Will Open did not commit"),
        };
        assert_ne!(
            session, next_session,
            "ClientID reuse must not reuse durable incarnation"
        );
        let _ = control_response(
            &mut store,
            ControlRequest::Lifecycle(lifecycle(
                4,
                end(
                    SessionGuard {
                        session: next_session,
                        opened_by: Some(operation(3)),
                    },
                    true,
                ),
            )),
            &events,
            &hooks,
            &cancel,
            &mut state,
        );
        let pending = business_response(
            &mut store,
            Request::Effect(OwnedEffect::new(
                effect(5),
                Effect::LoadWills {
                    after: None,
                    limit: 1,
                },
            )),
            &events,
            &hooks,
            &cancel,
            &mut state,
        );
        let Response::Wills {
            effect: read,
            result: Ok(page),
        } = pending
        else {
            panic!("bounded pending Will read failed")
        };
        assert_eq!(read, effect(5));
        assert_eq!(page.items.len(), 1);
        let intent = &page.items.as_slice()[0];
        assert_eq!(intent.id, operation(3));
        assert_eq!(intent.state, WillState::Pending);
        assert_eq!(intent.session, next_session);
        assert!(
            !store
                .snapshot()
                .unwrap()
                .sessions
                .contains_key(&next_session),
            "pending Will must survive source expiry0"
        );
        // Old normal close cannot cancel the newer occurrence.
        let _ = control_response(
            &mut store,
            ControlRequest::Lifecycle(lifecycle(
                6,
                end(
                    SessionGuard {
                        session,
                        opened_by: Some(operation(1)),
                    },
                    false,
                ),
            )),
            &events,
            &hooks,
            &cancel,
            &mut state,
        );
        assert_eq!(
            store
                .read_wills(None, 1, ReadBudget::PAGE)
                .unwrap()
                .items
                .as_slice()[0]
                .id,
            operation(3)
        );
        assert!(matches!(
            control_response(
                &mut store,
                ControlRequest::WillBarrier { connection, client },
                &events,
                &hooks,
                &cancel,
                &mut state
            ),
            ControlResponse::WillBarrier {
                result: Ok(true),
                ..
            }
        ));
        let publication = mqttiumd_contract::ids::PublicationId::new(
            boot.grant.incarnation(),
            boot.grant.epoch(),
            LogicalOwnerId::new(0),
            mqttiumd_contract::ids::PublicationSeq::new(7),
        );
        let frozen = Command {
            recipe: FrozenRecipe::freeze(Recipe::WillPublication(
                mqttiumd_contract::will::WillPublication {
                    live_retain: None,
                    operation: operation(7),
                    will: operation(3),
                    publication,
                    route_revision: store.snapshot().unwrap().route_revision,
                    topic: intent.spec.topic.clone(),
                    payload: mqttiumd_contract::command::PayloadRef {
                        id: mqttiumd_contract::ids::PayloadId::new(
                            boot.grant.incarnation(),
                            boot.grant.epoch(),
                            LogicalOwnerId::new(0),
                            mqttiumd_contract::ids::PayloadSeq::new(7),
                        ),
                        length: PayloadBytes::new(intent.spec.payload.len() as u64),
                    },
                    costs: mqttiumd_contract::command::Costs {
                        payload_bytes: PayloadBytes::new(intent.spec.payload.len() as u64),
                        references: References::new(0),
                        exchange_slots: ExchangeSlots::new(0),
                        receipt_slots: ReceiptSlots::new(1),
                    },
                    targets: mqttiumd_contract::capacity::BoundedVec::with_capacity(0).unwrap(),
                },
            ))
            .unwrap(),
            payload: Some(intent.spec.payload.clone()),
        };
        let admission = control_response(
            &mut store,
            ControlRequest::Lifecycle(OwnedEffect::new(
                effect(7),
                Effect::Persist {
                    command: frozen.clone(),
                },
            )),
            &events,
            &hooks,
            &cancel,
            &mut state,
        );
        assert!(
            matches!(admission,ControlResponse::Lifecycle {operation:id,reply:StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::WillPublished {will,publication:p})),..} if id==operation(7) && will==operation(3) && p==publication),
            "Will must admit through reserved control lane without source session"
        );
        assert!(
            store
                .read_wills(None, 1, ReadBudget::PAGE)
                .unwrap()
                .items
                .is_empty()
        );
        let resolved = control_response(
            &mut store,
            ControlRequest::Lifecycle(OwnedEffect::new(
                effect(8),
                Effect::ResolveAndFence {
                    operation: operation(7),
                },
            )),
            &events,
            &hooks,
            &cancel,
            &mut state,
        );
        assert!(
            matches!(resolved,ControlResponse::Lifecycle {reply:StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::WillPublished {will,publication:p})),..} if will==operation(3) && p==publication),
            "resolution must retain same admitted occurrence after intent removal"
        );
        let before = store.snapshot().unwrap();
        let duplicate = store.apply(&frozen);
        assert!(
            !duplicate.committed_now,
            "same operation must never repeat Will mutation"
        );
        assert_eq!(
            store.snapshot().unwrap(),
            before,
            "repeated Will command must preserve exact durable state"
        );
        let diagnostics = std::fs::read_to_string(dir.join("events.jsonl")).unwrap();
        assert!(
            !diagnostics.contains("SECRET-WILL"),
            "Will bytes must not enter product events"
        );
        assert!(diagnostics.contains("will_installed"));
        assert!(diagnostics.contains("will_decided"));
        assert_eq!(boot.grant.epoch(), RunEpoch::new(1));
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn retired_probe_respects_the_entire_reserved_replay_range() {
        use mqttiumd_contract::ids::{
            LogicalOwnerId, OperationSeq, PublicationId, PublicationSeq, RunEpoch,
        };
        let recipe =
            Recipe::ReplaySubscriptions(mqttiumd_contract::retained::ReplaySubscriptions {
                update: mqttiumd_contract::command::UpdateSubscriptions {
                    operation: OperationId::new(
                        StoreIncarnation::new(1),
                        RunEpoch::new(2),
                        LogicalOwnerId::new(0),
                        OperationSeq::new(3),
                    ),
                    session: SessionId::new(1),
                    session_revision: Revision::new(1),
                    route_revision: Revision::new(1),
                    changes: mqttiumd_contract::capacity::BoundedVec::with_capacity(0).unwrap(),
                },
                first_publication: PublicationId::new(
                    StoreIncarnation::new(1),
                    RunEpoch::new(2),
                    LogicalOwnerId::new(0),
                    PublicationSeq::new(1000),
                ),
            });
        assert_eq!(
            recipe_identity_highwater(&recipe).unwrap(),
            (
                1000 + mqttiumd_contract::retained::MAX_REPLAYS as u64 - 1,
                0
            ),
            "retired_probe_reused_reserved_replay_identity"
        );
    }

    #[test]
    fn bootstrap_retains_fixed_deadline_across_process_anchors_and_domain_change() {
        use crate::session_time::testing::FakeClock;
        use mqttiumd_contract::command::{Command, FrozenRecipe};
        use mqttiumd_contract::ids::OperationSeq;
        use mqttiumd_contract::outcome::{CommitFacts, StoreOutcome};
        use mqttiumd_contract::session::{SessionChange, SessionGuard, SessionLifecycle};
        const SECOND: u64 = 1_000_000_000;
        let path = std::env::temp_dir().join(format!(
            "mqttiumd-clock-bootstrap-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        let db = path.join("store.sqlite");
        let reading = |domain, tick| SessionClockReading {
            domain: [domain; 32],
            tick,
        };
        let clock = Arc::new(FakeClock::new(reading(1, 100)));
        let (mut store, boot) = bootstrap_with_clock(&db, clock.clone()).unwrap();
        assert_eq!(boot.clock_anchor.logical, 0);
        assert_eq!(
            store.snapshot().unwrap().session_clock,
            Some(boot.clock_anchor)
        );
        let operation = |sequence| {
            OperationId::new(
                boot.grant.incarnation(),
                boot.grant.epoch(),
                LogicalOwnerId::new(0),
                OperationSeq::new(sequence),
            )
        };
        let apply = |store: &mut StoreBackend, sequence, action| {
            store
                .apply(&Command {
                    recipe: FrozenRecipe::freeze(Recipe::SessionLifecycle(SessionLifecycle {
                        operation: operation(sequence),
                        action,
                    }))
                    .unwrap(),
                    payload: None,
                })
                .reply
        };
        let open = operation(1);
        let reply = apply(
            &mut store,
            1,
            SessionAction::OpenAt {
                client_id: SharedBytes::copy_from(b"fixed-deadline", MAX_CLIENT).unwrap(),
                expected: None,
                clean_start: false,
                expiry: SessionExpiry::After(10),
                now: 0,
            },
        );
        let StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
            SessionChange::Opened { session, .. },
        ))) = reply
        else {
            panic!("real finite Open commit: {reply:?}")
        };
        let guard = SessionGuard {
            session,
            opened_by: Some(open),
        };
        clock.set(reading(1, 100 + 2 * SECOND));
        let reply = apply(
            &mut store,
            2,
            SessionAction::Detach {
                guard,
                expiry: SessionExpiry::After(10),
                observed_at: 2 * SECOND,
            },
        );
        let StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
            SessionChange::Detached { detached, .. },
        ))) = reply
        else {
            panic!("real Detach commit: {reply:?}")
        };
        assert_eq!(detached.deadline, Some(12 * SECOND));
        drop(store);
        for seconds in [4, 8] {
            clock.set(reading(1, 100 + seconds * SECOND));
            let (store, boot) = bootstrap_with_clock(&db, clock.clone()).unwrap();
            assert_eq!(boot.clock_anchor.logical, seconds * SECOND);
            assert_eq!(
                store.snapshot().unwrap().sessions[&session].detached,
                Some(detached),
                "same-domain restart must not rearm the TTL"
            );
        }
        // UTC is deliberately absent from this input. A different boot/origin
        // retains committed eight seconds, even with a much larger raw tick.
        clock.set(reading(2, 100 * SECOND));
        let (store, boot) = bootstrap_with_clock(&db, clock.clone()).unwrap();
        assert_eq!(boot.clock_anchor.logical, 8 * SECOND);
        assert_eq!(
            store.snapshot().unwrap().sessions[&session].detached,
            Some(detached)
        );
        drop(store);
        clock.set(reading(2, 104 * SECOND));
        let (store, boot) = bootstrap_with_clock(&db, clock).unwrap();
        assert_eq!(boot.clock_anchor.logical, 12 * SECOND);
        assert!(
            store.snapshot().unwrap().sessions.is_empty(),
            "exact deadline at startup must expire the durable offline occurrence"
        );
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn reserved_collection_and_lifecycle_progress_while_ordinary_replies_are_unread() {
        let path = std::env::temp_dir().join(format!(
            "mqttiumd-control-lane-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        let db = path.join("store.sqlite");
        let events = Events::open(&path.join("events.jsonl")).unwrap();
        let cancel = Arc::new(AtomicBool::new(false));
        let mut writer =
            Writer::spawn(db.clone(), events, Hooks::default(), cancel.clone()).unwrap();
        let boot = writer
            .boot
            .recv_timeout(Duration::from_secs(3))
            .unwrap()
            .unwrap();
        let origin = ReceiptOrigin {
            incarnation: boot.grant.incarnation(),
            epoch: boot.grant.epoch(),
            owner: LogicalOwnerId::new(0),
        };
        // Exactly 64 ordinary reply slots are left unread. The 65th command
        // still has a real durable session effect, so an independent snapshot
        // establishes the worker reached the blocked reply, rather than merely
        // assuming it did from elapsed time.
        for request in 1..=65 {
            writer
                .sender
                .send(Request::Session {
                    request,
                    client: SharedBytes::copy_from(
                        format!("lane-{request}").as_bytes(),
                        MAX_CLIENT,
                    )
                    .unwrap(),
                })
                .unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while SqliteStore::inspect(&db).unwrap().sessions.len() != 65 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        let established = SqliteStore::inspect(&db).unwrap().sessions.len() == 65;
        let collection = ReceiptCollection {
            closure: ReceiptClosure::empty(origin),
            limit: 128,
        };
        let effect = EffectId {
            epoch: origin.epoch,
            owner: origin.owner,
            sequence: EffectSeq::new(1),
        };
        writer
            .control_sender
            .send(ControlRequest::Collect { effect, collection })
            .unwrap();
        let reply = writer.control_receiver.recv_timeout(Duration::from_secs(2));
        let clock_reading = boot.clock.read().unwrap();
        writer
            .control_sender
            .send(ControlRequest::ClockCheckpoint {
                expected: boot.clock_anchor,
                reading: clock_reading,
            })
            .unwrap();
        let checkpoint_reply = writer.control_receiver.recv_timeout(Duration::from_secs(2));
        let operation = OperationId::new(
            origin.incarnation,
            origin.epoch,
            origin.owner,
            mqttiumd_contract::ids::OperationSeq::new(1),
        );
        let lifecycle_effect = EffectId {
            sequence: EffectSeq::new(2),
            ..effect
        };
        let command = mqttiumd_contract::command::Command {
            recipe: mqttiumd_contract::command::FrozenRecipe::freeze(Recipe::SessionLifecycle(
                mqttiumd_contract::session::SessionLifecycle {
                    operation,
                    action: SessionAction::Open {
                        client_id: SharedBytes::copy_from(b"reserved-lifecycle", MAX_CLIENT)
                            .unwrap(),
                        expected: None,
                        clean_start: false,
                        expiry: SessionExpiry::OnDisconnect,
                    },
                },
            ))
            .unwrap(),
            payload: None,
        };
        writer
            .control_sender
            .send(ControlRequest::Lifecycle(OwnedEffect::new(
                lifecycle_effect,
                Effect::Persist { command },
            )))
            .unwrap();
        let lifecycle_reply = writer.control_receiver.recv_timeout(Duration::from_secs(2));
        let lifecycle_visible = SqliteStore::inspect(&db).unwrap().sessions.len() == 66;
        let resolution_effect = EffectId {
            sequence: EffectSeq::new(3),
            ..effect
        };
        writer
            .control_sender
            .send(ControlRequest::Lifecycle(OwnedEffect::new(
                resolution_effect,
                Effect::ResolveAndFence { operation },
            )))
            .unwrap();
        let resolution_reply = writer.control_receiver.recv_timeout(Duration::from_secs(2));
        let session = SqliteStore::inspect(&db)
            .unwrap()
            .sessions
            .into_iter()
            .find(|(_, value)| value.client_id == b"reserved-lifecycle")
            .map(|(id, _)| id);
        let close_operation = OperationId::new(
            origin.incarnation,
            origin.epoch,
            origin.owner,
            mqttiumd_contract::ids::OperationSeq::new(2),
        );
        let close_effect = EffectId {
            sequence: EffectSeq::new(4),
            ..effect
        };
        let close_reply = session.map(|session| {
            let recipe = Recipe::SessionLifecycle(mqttiumd_contract::session::SessionLifecycle {
                operation: close_operation,
                action: SessionAction::Close {
                    guard: mqttiumd_contract::session::SessionGuard {
                        session,
                        opened_by: Some(operation),
                    },
                    reason: mqttiumd_contract::session::SessionEndReason::Disconnect,
                },
            });
            writer
                .control_sender
                .send(ControlRequest::Lifecycle(OwnedEffect::new(
                    close_effect,
                    Effect::Persist {
                        command: mqttiumd_contract::command::Command {
                            recipe: mqttiumd_contract::command::FrozenRecipe::freeze(recipe)
                                .unwrap(),
                            payload: None,
                        },
                    },
                )))
                .unwrap();
            writer.control_receiver.recv_timeout(Duration::from_secs(2))
        });
        let close_visible = SqliteStore::inspect(&db).unwrap().sessions.len() == 65;
        // Cleanup before asserting: even the original blocked implementation
        // must release its writer and terminate when the semantic test fails.
        cancel.store(true, Ordering::Relaxed);
        while writer.receiver.try_recv().is_ok() {}
        writer.sender.send(Request::Stop).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        while !writer.is_finished() && Instant::now() < deadline {
            while writer.receiver.try_recv().is_ok() {}
            while writer.control_receiver.try_recv().is_ok() {}
            thread::sleep(Duration::from_millis(1));
        }
        writer.join_finished().unwrap();
        let _ = std::fs::remove_dir_all(path);
        assert!(
            established,
            "all65 ordinary commands actually completed while their replies stayed unread"
        );
        assert!(
            matches!(reply,Ok(ControlResponse::Collected { effect:e,collection:c,result:Ok(ReceiptCollectionReply::Applied { removed:0,.. }) }) if e==effect && c==collection),
            "reserved maintenance must reply even when the ordinary result lane is demonstrably full"
        );
        assert!(
            matches!(checkpoint_reply, Ok(ControlResponse::ClockCheckpoint { expected, reading, result: Ok(anchor) })
                if expected == boot.clock_anchor && reading == clock_reading && anchor.reading == clock_reading),
            "one correlated clock checkpoint must progress despite a full ordinary reply lane"
        );
        assert!(
            lifecycle_visible,
            "reserved lifecycle command must actually commit"
        );
        assert!(
            matches!(lifecycle_reply,
                Ok(ControlResponse::Lifecycle { effect, operation: returned, reply: StoreReply::Outcome(
                    mqttiumd_contract::outcome::StoreOutcome::Committed(mqttiumd_contract::outcome::CommitFacts::SessionLifecycle(
                        mqttiumd_contract::session::SessionChange::Opened { expiry: SessionExpiry::OnDisconnect, .. }
                    ))
                ) }) if effect == lifecycle_effect && returned == operation
            ),
            "reserved lifecycle must correlate its committed result while all ordinary results remain unread"
        );
        assert!(
            matches!(resolution_reply,
            Ok(ControlResponse::Lifecycle {effect, operation: returned, reply: StoreReply::Outcome(
                mqttiumd_contract::outcome::StoreOutcome::Committed(_))})
                if effect == resolution_effect && returned == operation),
            "same-operation resolution must progress in the reserved lane"
        );
        assert!(
            close_visible,
            "known close must reclaim its durable session under ordinary reply saturation"
        );
        assert!(
            matches!(close_reply,
            Some(Ok(ControlResponse::Lifecycle {effect, operation: returned, reply: StoreReply::Outcome(
                mqttiumd_contract::outcome::StoreOutcome::Committed(mqttiumd_contract::outcome::CommitFacts::SessionLifecycle(
                    mqttiumd_contract::session::SessionChange::Closed { .. }))) }))
                if effect == close_effect && returned == close_operation),
            "reserved close must return its own committed result"
        );
    }
    #[test]
    fn collection_observation_reports_actual_absolute_frontier_after_reordered_request() {
        use mqttiumd_contract::ids::OperationSeq;
        let path = std::env::temp_dir().join(format!(
            "mqttiumd-absolute-closure-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        let events = Events::open(&path.join("events.jsonl")).unwrap();
        let (mut store, boot) = bootstrap(&path.join("store.sqlite")).unwrap();
        let origin = ReceiptOrigin {
            incarnation: boot.grant.incarnation(),
            epoch: boot.grant.epoch(),
            owner: LogicalOwnerId::new(0),
        };
        let empty = ReceiptClosure::empty(origin);
        assert!(matches!(
            store.collect_receipts(ReceiptCollection {
                closure: empty,
                limit: 128
            }),
            Ok(ReceiptCollectionReply::Applied { .. })
        ));
        for sequence in 1..=64 {
            let _ = store.resolve_and_fence(OperationId::new(
                origin.incarnation,
                origin.epoch,
                origin.owner,
                OperationSeq::new(sequence),
            ));
        }
        let closed = ReceiptClosure {
            through: OperationSeq::new(64),
            ..empty
        };
        assert!(matches!(
            store.collect_receipts(ReceiptCollection {
                closure: closed,
                limit: 128
            }),
            Ok(ReceiptCollectionReply::Applied { .. })
        ));
        let request = ReceiptCollection {
            closure: ReceiptClosure {
                through: OperationSeq::new(32),
                ..empty
            },
            limit: 128,
        };
        let effect = EffectId {
            epoch: origin.epoch,
            owner: origin.owner,
            sequence: EffectSeq::new(1),
        };
        let response = control_response(
            &mut store,
            ControlRequest::Collect {
                effect,
                collection: request,
            },
            &events,
            &Hooks::default(),
            &Arc::new(AtomicBool::new(false)),
            &mut BusinessState::default(),
        );
        assert!(
            matches!(response,ControlResponse::Collected {result:Ok(ReceiptCollectionReply::Applied {closure,..}),..} if closure==closed)
        );
        let text = std::fs::read_to_string(path.join("events.jsonl")).unwrap();
        let event: serde_json::Value = serde_json::from_str(text.lines().last().unwrap()).unwrap();
        assert_eq!(event["details"]["through"], "32");
        assert_eq!(
            event["details"]["returned_through"], "64",
            "observation must preserve returned absolute knowledge rather than relabel the old request"
        );
        drop(store);
        let _ = std::fs::remove_dir_all(path);
    }
}
