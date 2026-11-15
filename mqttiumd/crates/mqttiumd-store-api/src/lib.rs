//! The interface of the store: `DurableStore`, the writer every store
//! realises, and the types it speaks in.
//!
//! Every method of `DurableStore` is one step in the single order of
//! authority (DEM-011). The deterministic store (`MemoryStore`, in
//! `mqttiumd-lab`) and the SQLite store (`mqttiumd-store-sqlite`) realise
//! it, and pass the same contract tests (`mqttiumd_lab::store::conformance`).
//! It lives in a crate of its own so that the bench can depend on both
//! stores, and the SQLite store on this interface alone.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use mqttiumd_contract::capacity::{
    BoundedVec, ExchangeSlots, PayloadBytes, ReceiptSlots, References,
};
use mqttiumd_contract::command::{Command, DeliveryCause, EpochLedger, OpenEpoch};
use mqttiumd_contract::effects::{InboundPhase, RecoveredFact};
use mqttiumd_contract::ids::{
    DeliveryId, ExchangeId, InboundPacketId, OperationId, PayloadId, PublicationId, Revision,
    RunEpoch, SessionId,
};
use mqttiumd_contract::outcome::{
    CommitFacts, OpeningOutcome, RejectReason, StoreOutcome, StoreReply,
};
use mqttiumd_contract::packet::Qos;

/// The longest ClientID MQTT can carry: a UTF-8 string with a two-byte
/// length. A session never holds a longer one, and a recovered session
/// fact is built within this budget.
pub const MAX_CLIENT_ID_BYTES: usize = 65_535;

/// The durable limits of a data set, fixed when it is created.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Limits {
    pub payload_bytes: PayloadBytes,
    pub references: References,
    pub exchange_slots: ExchangeSlots,
    pub receipt_slots: ReceiptSlots,
    pub max_targets_per_admission: usize,
    pub max_topic_bytes: usize,
    pub max_payload_bytes: usize,
    /// Active sessions at a time. Terminal ones are not counted: their
    /// identity is never used again, and they hold nothing.
    pub max_sessions: usize,
    pub max_route_targets: usize,
}

/// What the accounts currently charge. Never above the limits.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Accounts {
    pub payload_bytes: PayloadBytes,
    pub references: References,
    pub exchange_slots: ExchangeSlots,
    pub receipt_slots: ReceiptSlots,
}

impl Accounts {
    /// What one receipt or one fence holds.
    pub const RECEIPT: Self = Self::receipt();

    const fn receipt() -> Self {
        Self {
            payload_bytes: PayloadBytes::ZERO,
            references: References::ZERO,
            exchange_slots: ExchangeSlots::ZERO,
            receipt_slots: ReceiptSlots::new(1),
        }
    }
}

/// Life cycle of a session incarnation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SessionState {
    Active,
    /// The session was deleted. Its identity is never used again.
    Terminal,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SessionRecord {
    pub client_id: Vec<u8>,
    pub revision: Revision,
    pub state: SessionState,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RouteTarget {
    pub session: SessionId,
    pub qos: Qos,
}

/// The durable decision about one operation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Decision {
    Committed(CommitFacts),
    Rejected(RejectReason),
}

/// The receipt of an operation: its exact recipe and what was decided.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct OperationRecord {
    /// The canonical bytes of the recipe, compared byte for byte when the
    /// operation is presented again (DEM-009).
    pub recipe: Vec<u8>,
    pub decision: Decision,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PublicationRecord {
    pub operation: OperationId,
    pub source: SessionId,
    pub topic: Vec<u8>,
    pub qos: Qos,
    pub retain: bool,
    pub payload: PayloadId,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PayloadRecord {
    pub bytes: Vec<u8>,
    /// Live obligations that still refer to the payload. Never zero: a
    /// payload no obligation refers to is not kept, and its bytes are
    /// not charged.
    pub references: u64,
}

/// Phase of an obligation. S0 excludes emission towards recipients, so
/// an obligation stays pending until its destination is deleted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ObligationPhase {
    Pending,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ObligationRecord {
    pub target: SessionId,
    pub target_revision: Revision,
    pub qos: Qos,
    pub cause: DeliveryCause,
    pub phase: ObligationPhase,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct InboundRecord {
    pub exchange: ExchangeId,
    pub publication: PublicationId,
    pub phase: InboundPhase,
}

/// Everything that survives a crash. It is also the snapshot of a store:
/// two stores in the same durable state compare equal, and every
/// collection is ordered by its key.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DurableState {
    pub ledger: EpochLedger,
    /// Closed epochs up to this one had their receipts collected; zero
    /// when nothing was collected.
    pub collected_through: RunEpoch,
    /// Advances at every commit. A recovery cursor names the revision it
    /// reads, and refuses to continue on another one.
    pub revision: u64,
    pub limits: Limits,
    pub accounts: Accounts,
    pub next_session: SessionId,
    pub route_revision: Revision,
    pub sessions: BTreeMap<SessionId, SessionRecord>,
    pub routes: BTreeMap<Vec<u8>, Vec<RouteTarget>>,
    /// Every receipt, admitted or rejected, holds one receipt slot.
    pub operations: BTreeMap<OperationId, OperationRecord>,
    /// Identities closed by `ResolveAndFence` before any command arrived.
    /// Each holds one receipt slot, like a receipt.
    pub fences: BTreeSet<OperationId>,
    pub publications: BTreeMap<PublicationId, PublicationRecord>,
    pub payloads: BTreeMap<PayloadId, PayloadRecord>,
    pub obligations: BTreeMap<DeliveryId, ObligationRecord>,
    pub inbound: BTreeMap<(SessionId, InboundPacketId), InboundRecord>,
}

/// Commands that prepare or administer a data set. A fixture is set up
/// through these, never by writing state.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum AdminCommand {
    /// Creates a persistent session for a ClientID no active session has.
    CreateSession { client_id: Vec<u8> },
    /// Adds one destination to the exact-topic route of `topic`.
    AddRoute {
        topic: Vec<u8>,
        session: SessionId,
        qos: Qos,
    },
    /// Makes a session terminal. Its inbound markers go with it, and the
    /// obligations *towards* it end with their accounts released. The
    /// obligations of the publications it produced, towards other
    /// sessions, are not touched.
    DeleteSession { session: SessionId },
    /// Collects the receipts of closed epochs up to `through`.
    CollectClosedEpochs { through: RunEpoch },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AdminDone {
    SessionCreated {
        session: SessionId,
        revision: Revision,
    },
    RouteAdded {
        route_revision: Revision,
    },
    SessionDeleted,
    Collected,
}

/// Why an administration command changed nothing.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AdminRefused {
    DuplicateClientId,
    UnknownSession,
    DuplicateTarget,
    TableFull,
    TopicTooLong,
    /// Longer than `MAX_CLIENT_ID_BYTES`.
    ClientIdTooLong,
    /// A counter reached its end; nothing wraps.
    Exhausted,
    /// The epoch is open or was never opened: nothing to collect.
    EpochNotClosed,
    /// The durable medium refused the write.
    WriteFailed,
    /// The store found its own accounting broken. Nothing was committed.
    Inconsistent,
}

/// What applying a command did.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Applied {
    pub reply: StoreReply,
    /// Whether this application is the one that committed the effect of
    /// the operation. False for a repetition, a rejection or a violation.
    pub committed_now: bool,
    /// Whether this application made anything durable: the effect, or
    /// the receipt of a rejection. The point after a commit is passed
    /// exactly when this is true.
    pub wrote: bool,
}

/// What `ResolveAndFence` established.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Resolved {
    pub outcome: StoreOutcome,
    /// Whether this call is the one that made the fence durable. False
    /// when the outcome was already established, and when no fence
    /// could be written.
    pub fenced_now: bool,
}

/// A position in the recovery inventory of one state revision.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RecoveryCursor {
    revision: u64,
    position: usize,
}

impl RecoveryCursor {
    /// The position `position` of the inventory read at state revision
    /// `revision`. Built by a store for its next page; a cursor is only
    /// ever handed back to the store that issued it.
    #[must_use]
    pub const fn new(revision: u64, position: usize) -> Self {
        Self { revision, position }
    }

    /// The state revision the inventory was read at.
    #[must_use]
    pub const fn revision(self) -> u64 {
        self.revision
    }

    /// The position of the next fact in that inventory.
    #[must_use]
    pub const fn position(self) -> usize {
        self.position
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct RecoveryPage {
    pub facts: BoundedVec<RecoveredFact>,
    /// Where the next page starts; `None` when this page is the last.
    pub next: Option<RecoveryCursor>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RecoveryRefused {
    /// The state changed since the cursor was issued: the inventory is
    /// not coherent any more and is read again from the start.
    FrontierMoved,
    /// A page of zero facts makes no progress.
    EmptyPage,
    /// A page, or a fact on it, could not be built within its budget.
    /// Nothing is said about the facts after it: the inventory is not
    /// complete, and it is not presented as complete.
    AllocationFailed,
}

/// The writer of a store. Each method is one step in the single order
/// of authority; a method either commits entirely or changes nothing.
pub trait DurableStore {
    /// The incarnation of the data set and the last epoch opened in it:
    /// what start-up reads before it opens anything.
    fn ledger(&self) -> EpochLedger;

    /// Opens the epoch after the one the command expects and closes
    /// every older one, atomically, or changes nothing. The command
    /// that opened the last epoch, presented again, gets the same
    /// commit; see `EpochLedger::open` for every other case.
    fn open_epoch(&mut self, command: &OpenEpoch) -> OpeningOutcome;

    /// Applies a frozen command.
    fn apply(&mut self, command: &Command) -> Applied;

    /// What is established about `operation`, read and nothing written:
    /// `None` when it is absent from a space that can still execute, the
    /// case in which `resolve_and_fence` would write a fence.
    fn established(&self, operation: OperationId) -> Option<StoreOutcome>;

    /// Reports what is established about `operation`. When it is absent
    /// from a space that can still execute, a durable fence is
    /// established before a certain rejection is announced; when the
    /// fence cannot be established the outcome stays unknown.
    fn resolve_and_fence(&mut self, operation: OperationId) -> Resolved;

    /// Applies an administration command.
    fn admin(&mut self, command: AdminCommand) -> Result<AdminDone, AdminRefused>;

    /// Reads one bounded page of the recovery inventory, from durable
    /// state only.
    fn recover(
        &self,
        cursor: Option<RecoveryCursor>,
        limit: usize,
    ) -> Result<RecoveryPage, RecoveryRefused>;

    /// The durable state, for inspection.
    fn snapshot(&self) -> DurableState;
}

/// Whether the durable medium accepts writes. Controlled by the bench:
/// it is an availability, not a decision of the store (DEM-013).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Durability {
    Available,
    /// Every commit fails. Reads still work.
    Failing,
}
