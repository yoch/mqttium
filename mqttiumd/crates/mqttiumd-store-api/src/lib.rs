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
    DeliveryId, ExchangeId, InboundPacketId, OperationId, OutboundPacketId, PayloadId,
    PublicationId, Revision, RunEpoch, SessionId,
};
pub use mqttiumd_contract::message_expiry::ExpiredDelivery;
use mqttiumd_contract::message_expiry::{MessageExpiry, MessageTime};
use mqttiumd_contract::outcome::{
    CommitFacts, OpeningOutcome, RejectReason, StoreOutcome, StoreReply,
};
use mqttiumd_contract::packet::Qos;
use mqttiumd_contract::pubsub::WorkItem;
use mqttiumd_contract::receipts::{
    ClosedReceiptCollection, ClosedReceiptReply, ReceiptClosure, ReceiptCollection,
    ReceiptCollectionReply, ReceiptOrigin,
};

/// The longest ClientID MQTT can carry: a UTF-8 string with a two-byte
/// length. A session never holds a longer one, and a recovered session
/// fact is built within this budget.
pub const MAX_CLIENT_ID_BYTES: usize = 65_535;

/// Finite S0 ceiling for a recovery inventory, including its fixed facts.
/// Positional cursors can scan a prefix; they never enumerate an unbounded one.
pub const MAX_RECOVERY_INVENTORY_ROWS: usize = 65_536;

/// A sweep visits at most four publications and returns at most 128 deliveries.
pub const MESSAGE_EXPIRY_SWEEP_PUBLICATIONS: u32 = 4;
pub const MESSAGE_EXPIRY_SWEEP_DELIVERIES: usize = 128;

/// Absolute receiptless maintenance knowledge. A lost response can be retried
/// with the same observation and key cursor without granting credits twice.
#[derive(PartialEq, Eq, Debug)]
pub struct MessageExpirySweep {
    pub removed: BoundedVec<ExpiredDelivery>,
    pub retained_removed: u32,
    pub quarantined: u32,
    pub more: bool,
    pub next: Option<PublicationId>,
}

impl Default for MessageExpirySweep {
    fn default() -> Self {
        Self {
            // This default is an empty result and allocates no backing storage.
            // Writers reserve the bounded result they need fallibly instead.
            removed: BoundedVec::with_capacity(0).expect("zero capacity does not allocate"),
            retained_removed: 0,
            quarantined: 0,
            more: false,
            next: None,
        }
    }
}

/// Stable causes of a failed durable read. A failure establishes neither
/// absence nor a rejection and never authorises executing an uncertain command.
/// Driver details and offending durable values are deliberately not carried.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReadError {
    InvalidData,
    LimitExceeded,
    Unavailable,
    Io,
    AllocationFailed,
}

impl ReadError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidData => "InvalidData",
            Self::LimitExceeded => "LimitExceeded",
            Self::Unavailable => "Unavailable",
            Self::Io => "Io",
            Self::AllocationFailed => "AllocationFailed",
        }
    }
}

/// The read whose result was unavailable, including failures before Started.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReadPhase {
    Open,
    Ledger,
    Opening,
    Lookup,
    Snapshot,
    Inspect,
    Recovery,
    Work,
    Maintenance,
}

impl ReadPhase {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Open => "Open",
            Self::Ledger => "Ledger",
            Self::Opening => "Opening",
            Self::Lookup => "Lookup",
            Self::Snapshot => "Snapshot",
            Self::Inspect => "Inspect",
            Self::Recovery => "Recovery",
            Self::Work => "Work",
            Self::Maintenance => "Maintenance",
        }
    }
}

/// Independent limits on rows, copied field bytes and aggregate logical bytes.
/// Checks precede application-owned copies; field-specific limits still apply.
/// These bound the reader's work, not SQLite's internal page cache or allocator.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ReadBudget {
    pub max_rows: usize,
    pub max_bytes: usize,
    pub max_field_bytes: usize,
}

impl ReadBudget {
    pub const SNAPSHOT: Self = Self {
        max_rows: 65_536,
        max_bytes: 32 * 1024 * 1024,
        max_field_bytes: 8 * 1024 * 1024,
    };

    pub const PAGE: Self = Self {
        max_rows: 4_096,
        max_bytes: 1024 * 1024,
        max_field_bytes: 1024 * 1024,
    };

    /// A closure can validate 128 canonical subscription recipes, each with
    /// eight legal 1024-byte filters. Their aggregate exceeds a work page.
    /// Keep a separate finite budget rather than widening every page reader.
    pub const COLLECTION: Self = Self {
        max_rows: mqttiumd_contract::receipts::MAX_COLLECTION_ROWS as usize,
        max_bytes: 2 * 1024 * 1024,
        max_field_bytes: 1024 * 1024,
    };

    pub fn check_field(self, bytes: usize) -> Result<(), ReadError> {
        if bytes > self.max_field_bytes || bytes > self.max_bytes {
            Err(ReadError::LimitExceeded)
        } else {
            Ok(())
        }
    }

    pub fn check_totals(self, rows: usize, bytes: usize) -> Result<(), ReadError> {
        if rows > self.max_rows || bytes > self.max_bytes {
            Err(ReadError::LimitExceeded)
        } else {
            Ok(())
        }
    }
}

/// One observation in an explicitly identified monotonic boot-clock domain.
/// Ticks and logical progress are checked nanoseconds, never UTC or Instant.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SessionClockReading {
    pub domain: [u8; 32],
    pub tick: u64,
}

/// The one durable anchor. Checkpoints overwrite it, never append history.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SessionClockAnchor {
    pub reading: SessionClockReading,
    pub logical: u64,
}

impl SessionClockAnchor {
    /// Only a recognized identical domain may account uncommitted elapsed time.
    pub fn observe(self, reading: SessionClockReading) -> Result<u64, ReadError> {
        if reading.domain != self.reading.domain {
            return Err(ReadError::Unavailable);
        }
        let elapsed = reading
            .tick
            .checked_sub(self.reading.tick)
            .ok_or(ReadError::InvalidData)?;
        self.logical
            .checked_add(elapsed)
            .ok_or(ReadError::InvalidData)
    }

    pub fn advance(
        previous: Option<Self>,
        reading: SessionClockReading,
    ) -> Result<Self, ReadError> {
        let logical = match previous {
            Some(anchor) if anchor.reading.domain == reading.domain => anchor.observe(reading)?,
            Some(anchor) => anchor.logical,
            None => 0,
        };
        Ok(Self { reading, logical })
    }
}

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
    pub expiry: mqttiumd_contract::session::SessionExpiry,
    pub opened_by: Option<OperationId>,
    pub detached: Option<mqttiumd_contract::session::SessionDetachment>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RouteTarget {
    pub session: SessionId,
    pub qos: Qos,
    pub options: mqttiumd_contract::pubsub::SubscriptionOptions,
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
    /// Frozen retained version for a distinct replay occurrence.
    pub retained_version: Option<PublicationId>,
    pub operation: OperationId,
    pub source: SessionId,
    pub topic: Vec<u8>,
    pub qos: Qos,
    pub retain: bool,
    pub expiry: Option<MessageExpiry>,
    pub payload: PayloadId,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PayloadRecord {
    pub bytes: Vec<u8>,
    /// Live obligations plus the current retained entry. Never zero: a
    /// payload with no responsibility is not kept, and its bytes are
    /// not charged.
    pub references: u64,
}

/// Durable phase of an obligation. A guarded terminal operation removes it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ObligationPhase {
    Pending,
    /// A claimed QoS 2 delivery must send PUBREL and await PUBCOMP.
    AwaitPubcomp,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ObligationRecord {
    pub retain: bool,
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

/// A durable outgoing intent. Its claim operation also guards slot reuse.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct OutboundRecord {
    pub target: SessionId,
    pub target_revision: Revision,
    pub packet_id: OutboundPacketId,
    pub claim: OperationId,
    /// A durable positive-byte observation; an unmarked claim is uncertain.
    pub started: bool,
}

/// One coherent live key page, ordered by DeliveryId, with the admitted QoS
/// preserved independently of later subscription changes. A new scan starts
/// at None.
#[derive(PartialEq, Eq, Debug)]
pub struct WorkPage {
    pub items: BoundedVec<WorkItem>,
    pub next: Option<DeliveryId>,
}

#[derive(PartialEq, Eq, Debug)]
pub struct WillPage {
    pub items: BoundedVec<mqttiumd_contract::will::WillIntent>,
    pub next: Option<OperationId>,
}

/// Everything that survives a crash. It is also the snapshot of a store:
/// two stores in the same durable state compare equal, and every
/// collection is ordered by its key.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RetainedRecord {
    pub publication: PublicationId,
    pub producer_client_id: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DurableState {
    /// Independent intentions; Pending entries outlive their source session.
    pub wills: BTreeMap<OperationId, mqttiumd_contract::will::WillIntent>,
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
    pub session_clock: Option<SessionClockAnchor>,
    pub route_revision: Revision,
    pub sessions: BTreeMap<SessionId, SessionRecord>,
    pub routes: BTreeMap<Vec<u8>, Vec<RouteTarget>>,
    /// Every receipt, admitted or rejected, holds one receipt slot.
    pub operations: BTreeMap<OperationId, OperationRecord>,
    /// Identities closed by `ResolveAndFence` before any command arrived.
    /// Each holds one receipt slot, like a receipt.
    pub fences: BTreeSet<OperationId>,
    /// Compact non-execution guards, distinct from physical retained rows.
    pub receipt_frontiers: BTreeMap<ReceiptOrigin, ReceiptClosure>,
    pub retained: BTreeMap<Vec<u8>, RetainedRecord>,
    pub publications: BTreeMap<PublicationId, PublicationRecord>,
    pub payloads: BTreeMap<PayloadId, PayloadRecord>,
    pub obligations: BTreeMap<DeliveryId, ObligationRecord>,
    pub inbound: BTreeMap<(SessionId, InboundPacketId), InboundRecord>,
    pub outbound: BTreeMap<DeliveryId, OutboundRecord>,
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
    /// The inventory remains incomplete. The exact durable-read cause is
    /// preserved; a size refusal is not an allocation failure.
    Read(ReadError),
}

impl RecoveryRefused {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::FrontierMoved => "FrontierMoved",
            Self::EmptyPage => "EmptyPage",
            Self::Read(error) => error.code(),
        }
    }
}

/// The writer of a store. Each method is one step in the single order
/// of authority; a method either commits entirely or changes nothing.
pub trait DurableStore {
    /// Visit a bounded key range, removing only unclaimed expired copies.
    /// Any outbound claim preserves its copy. Unknown message domains are
    /// quarantined, never assigned an invented elapsed time or deleted here.
    /// Limits are 1..=MESSAGE_EXPIRY_SWEEP_PUBLICATIONS; result identities fit
    /// MESSAGE_EXPIRY_SWEEP_DELIVERIES. The caller advances `after` only after
    /// receiving the absolute result; no historical cursor table is stored.
    fn expire_messages(
        &mut self,
        _now: MessageTime,
        _after: Option<PublicationId>,
        _limit: u32,
    ) -> Result<MessageExpirySweep, ReadError> {
        Err(ReadError::Unavailable)
    }

    /// Compare-and-set the singleton clock anchor without creating a receipt.
    /// A repeated/lost reply is reconciled by its absolute returned anchor.
    /// A stale expectation returns the current anchor and performs no update.
    fn checkpoint_session_clock(
        &mut self,
        _expected: Option<SessionClockAnchor>,
        _reading: SessionClockReading,
    ) -> Result<SessionClockAnchor, ReadError> {
        Err(ReadError::Unavailable)
    }

    /// Confirm an origin's contiguous consumed prefix and sweep at most limit
    /// physical receipt/fence rows atomically. No receipt is created. A retry
    /// reports absolute durable knowledge, never a second credit grant.
    fn collect_receipts(
        &mut self,
        _collection: ReceiptCollection,
    ) -> Result<ReceiptCollectionReply, ReadError> {
        Err(ReadError::Unavailable)
    }

    /// Read one compact durable origin guard, including after a lost reply.
    fn receipt_frontier(
        &self,
        _origin: ReceiptOrigin,
    ) -> Result<Option<ReceiptClosure>, ReadError> {
        Err(ReadError::Unavailable)
    }

    /// Sweep old epochs only after durable startup closed their authority.
    /// Each call bounds rows and allocates no operation identity or receipt.
    fn collect_closed_receipts(
        &mut self,
        _collection: ClosedReceiptCollection,
    ) -> Result<ClosedReceiptReply, ReadError> {
        Err(ReadError::Unavailable)
    }

    /// The incarnation of the data set and the last epoch opened in it:
    /// what start-up reads before it opens anything.
    fn ledger(&self) -> Result<EpochLedger, ReadError>;

    /// Opens the epoch after the one the command expects and closes
    /// every older one, atomically, or changes nothing. The command
    /// that opened the last epoch, presented again, gets the same
    /// commit; see `EpochLedger::open` for every other case.
    fn open_epoch(&mut self, command: &OpenEpoch) -> Result<OpeningOutcome, ReadError>;

    /// Applies a frozen command.
    fn apply(&mut self, command: &Command) -> Applied;

    /// What is established about `operation`, read and nothing written:
    /// `None` when it is absent from a space that can still execute, the
    /// case in which `resolve_and_fence` would write a fence.
    fn established(&self, operation: OperationId) -> Result<Option<StoreOutcome>, ReadError>;

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
    ) -> Result<RecoveryPage, RecoveryRefused> {
        self.recover_with_budget(cursor, limit, ReadBudget::PAGE)
    }

    fn recover_with_budget(
        &self,
        cursor: Option<RecoveryCursor>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<RecoveryPage, RecoveryRefused>;

    /// Read only QoS 1 obligations for a target, in DeliveryId order. This is
    /// a live key cursor, not a claim or a proof of global absence. Each page
    /// is coherent and copied only within its field/aggregate byte budgets.
    fn read_work(
        &self,
        session: SessionId,
        after: Option<DeliveryId>,
        limit: usize,
    ) -> Result<WorkPage, ReadError> {
        self.read_work_with_budget(session, after, limit, ReadBudget::PAGE)
    }

    fn read_work_with_budget(
        &self,
        _session: SessionId,
        _after: Option<DeliveryId>,
        _limit: usize,
        _budget: ReadBudget,
    ) -> Result<WorkPage, ReadError> {
        Err(ReadError::Unavailable)
    }

    /// Bounded pending Will inventory. No notification is required for discovery.
    fn read_wills(
        &self,
        after: Option<OperationId>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<WillPage, ReadError> {
        let _ = (after, limit, budget);
        Err(ReadError::Unavailable)
    }

    /// The durable state, for inspection.
    fn snapshot(&self) -> Result<DurableState, ReadError> {
        self.snapshot_with_budget(ReadBudget::SNAPSHOT)
    }

    fn snapshot_with_budget(&self, budget: ReadBudget) -> Result<DurableState, ReadError>;
}

/// Whether the durable medium accepts writes. Controlled by the bench:
/// it is an availability, not a decision of the store (DEM-013).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Durability {
    Available,
    /// Every commit fails. Reads still work.
    Failing,
}
