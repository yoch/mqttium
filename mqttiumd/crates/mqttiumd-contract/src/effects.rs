//! Inputs, effects and the ownership of effects (DEM-007, DEM-008).
//!
//! Inputs and effects are closed unions of plain data: no future, no socket,
//! no closure, no mutable pointer to another owner. Each owner keeps a
//! bounded effect box that survives scheduler turns; nothing in it is dropped
//! because a downstream queue is full.
//!
//! The box is volatile: a crash destroys it. Safety after a crash rests on
//! durable facts and protocol replay, never on this memory.

use core::mem::size_of;
use core::num::{NonZeroU32, NonZeroUsize};

use crate::capacity::{
    AllocationFailed, BoundedQueue, BoundedVec, CapacityNeed, PayloadBytes, Resource,
    WakeCondition, WorkUnits,
};
use crate::command::Command;
use crate::ids::{
    ConnectionGeneration, ConnectionKey, ContinuationId, DeadlineToken, DeliveryId, EffectId,
    ExchangeId, InboundPacketId, LogicalDuration, LogicalOwnerId, LogicalTime, OperationId,
    OutboundPacketId, OutboxSerial, PublicationId, Revision, RunEpoch, SessionId, StoreIncarnation,
};
use crate::outcome::{EpochGrant, Knowledge, StoreReply};
use crate::packet::{InboundPacket, OutboundFrame, Qos, SharedBytes};
use crate::pubsub::{DeliveryClaim, PubackTarget, Qos0DropReason, Qos0Origin, WorkItem};

/// Durable phase of an inbound QoS 2 exchange.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InboundPhase {
    /// Admission committed; PUBREC is owed and PUBREL is awaited.
    AwaitPubrel,
}

/// One fact read from durable data during recovery. Identities, limits and
/// routes found here come from the store, never from the bench (DEM-012).
///
/// The inventory of a recovery is ordered: see [`FactRank`] and
/// [`RecoveryOrder`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RecoveredFact {
    /// Durable retirement does not mean that an old command was rejected.
    ReceiptFrontier(crate::receipts::ReceiptClosure),
    OutboundDelivery {
        delivery: DeliveryId,
        target: SessionId,
        target_revision: Revision,
        qos: Qos,
        claim: Option<DeliveryClaim>,
    },
    /// The data set and the run epoch opened for this run.
    Epoch {
        incarnation: StoreIncarnation,
        epoch: RunEpoch,
    },
    /// The limits the store enforces on an admission. They come before
    /// everything they bound.
    Limits {
        max_targets_per_admission: u32,
        max_topic_bytes: u32,
        max_payload_bytes: PayloadBytes,
    },
    /// The durable revision of the route table. The routes that follow
    /// belong to it.
    RouteTable { revision: Revision },
    /// One target of one topic, at the revision of the table that precedes
    /// it. Routes are sorted by the bytes of their topic; the targets of one
    /// topic keep the order in which they were added.
    Route {
        topic: SharedBytes,
        target: SessionId,
        target_revision: Revision,
        max_qos: Qos,
        options: crate::pubsub::SubscriptionOptions,
    },
    /// A persistent session.
    Session {
        session: SessionId,
        client_id: SharedBytes,
        revision: Revision,
        expiry: crate::session::SessionExpiry,
        opened_by: Option<OperationId>,
        detached: Option<crate::session::SessionDetachment>,
    },
    /// An inbound QoS 2 slot linking a client replay to its publication.
    InboundExchange {
        exchange: ExchangeId,
        packet_id: InboundPacketId,
        publication: PublicationId,
        phase: InboundPhase,
    },
    /// What the store establishes about an operation of an older epoch.
    Operation {
        operation: OperationId,
        knowledge: Knowledge,
    },
}

/// The place of a kind of fact in the inventory of a recovery, first to
/// last.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum FactRank {
    Epoch,
    Limits,
    RouteTable,
    Route,
    Session,
    InboundExchange,
    OutboundDelivery,
    ReceiptFrontier,
    Operation,
}

impl FactRank {
    /// Whether an inventory holds at most one fact of this rank.
    #[must_use]
    pub const fn at_most_once(self) -> bool {
        matches!(self, Self::Epoch | Self::Limits | Self::RouteTable)
    }
}

impl RecoveredFact {
    /// The rank of this fact in the inventory of a recovery.
    #[must_use]
    pub const fn rank(&self) -> FactRank {
        match self {
            Self::Epoch { .. } => FactRank::Epoch,
            Self::Limits { .. } => FactRank::Limits,
            Self::RouteTable { .. } => FactRank::RouteTable,
            Self::Route { .. } => FactRank::Route,
            Self::Session { .. } => FactRank::Session,
            Self::InboundExchange { .. } => FactRank::InboundExchange,
            Self::OutboundDelivery { .. } => FactRank::OutboundDelivery,
            Self::ReceiptFrontier(_) => FactRank::ReceiptFrontier,
            Self::Operation { .. } => FactRank::Operation,
        }
    }
}

/// Why a fact is out of place in an inventory.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OrderRefused {
    /// A fact of rank `found` follows a fact of the later rank `previous`.
    RankDecreased { previous: FactRank, found: FactRank },
    /// A second fact of a rank an inventory holds at most once.
    Repeated(FactRank),
    /// The topic of a route sorts before the topic of the route before it.
    RoutesUnsorted,
}

/// The order of the facts of a recovery, as one rule for the store that
/// writes them, the readers of a trace and the core that consumes them.
///
/// Ranks never decrease; `Epoch`, `Limits` and `RouteTable` appear at most
/// once; routes are sorted by the bytes of their topic. Equal topics are
/// accepted, since a topic has several targets and their order of addition
/// is not visible in the facts.
///
/// The same value checks one page, when it starts fresh, or a whole
/// inventory, when it is carried from page to page. It does not require
/// that a fact is present: whether an inventory may lack its epoch, its
/// limits or the table of its routes is for whoever reads the whole of it.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct RecoveryOrder {
    last: Option<FactRank>,
    last_topic: Option<SharedBytes>,
}

impl RecoveryOrder {
    /// Before the first fact.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            last: None,
            last_topic: None,
        }
    }

    /// Accepts `fact` as the next of the inventory, or says why it is out
    /// of place. A refusal changes nothing.
    pub fn admit(&mut self, fact: &RecoveredFact) -> Result<(), OrderRefused> {
        let found = fact.rank();
        if let Some(previous) = self.last {
            if found < previous {
                return Err(OrderRefused::RankDecreased { previous, found });
            }
            if found == previous && found.at_most_once() {
                return Err(OrderRefused::Repeated(found));
            }
        }
        if let RecoveredFact::Route { topic, .. } = fact {
            if let Some(previous) = &self.last_topic
                && topic.as_slice() < previous.as_slice()
            {
                return Err(OrderRefused::RoutesUnsorted);
            }
            self.last_topic = Some(topic.clone());
        }
        self.last = Some(found);
        Ok(())
    }
}

/// What an adapter confirms about a frame. Progress does not prove that the
/// peer received a complete packet.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FrameProgress {
    /// This many bytes of the frame are confirmed written, out of `total`.
    Written { confirmed: u64, total: u64 },
    /// The write failed; the connection is unusable.
    Failed,
}

/// An input of the core. Each variant states the authority it carries.
///
/// It is moved, never copied: a step either consumes it or hands it back.
#[derive(PartialEq, Eq, Debug)]
pub enum Input {
    OpenSession {
        connection: ConnectionGeneration,
        client_id: SharedBytes,
        clean_start: bool,
        expiry: crate::session::SessionExpiry,
    },
    OpenSessionAt {
        connection: ConnectionGeneration,
        client_id: SharedBytes,
        clean_start: bool,
        expiry: crate::session::SessionExpiry,
        now: u64,
    },
    DetachSession {
        connection: ConnectionKey,
        opened_by: Option<OperationId>,
        expiry: crate::session::SessionExpiry,
        observed_at: u64,
    },
    ExpireSession {
        guard: crate::session::SessionGuard,
        detached: crate::session::SessionDetachment,
        now: u64,
    },
    EndSession {
        connection: ConnectionKey,
        opened_by: Option<OperationId>,
        reason: crate::session::SessionEndReason,
    },
    /// Enable/pump bounded live receipt collection. Only product adapters that
    /// implement the control invoke it; ordinary S0 fixture runs do not.
    MaintainReceipts,
    ReceiptsCollected {
        effect: EffectId,
        collection: crate::receipts::ReceiptCollection,
        reply: crate::receipts::ReceiptCollectionReply,
    },
    /// Adapter refused or failed this exact volatile attempt. The owner decides its loss.
    Qos0EmitRefused {
        effect: EffectId,
        connection: ConnectionKey,
        reason: Qos0DropReason,
    },
    ConnectionSettings {
        connection: ConnectionGeneration,
        receive_maximum: u16,
        maximum_packet_size: u32,
    },
    WorkPage {
        effect: EffectId,
        session: SessionId,
        items: BoundedVec<WorkItem>,
        next: Option<DeliveryId>,
    },
    /// A session fact confirmed by the exclusive durable writer, outside
    /// recovery. The adapter correlates its request before constructing it.
    /// `None` installs a late result without binding a closed connection.
    SessionEstablished {
        connection: Option<ConnectionGeneration>,
        session: SessionId,
        client_id: SharedBytes,
        revision: Revision,
        session_present: bool,
    },
    /// The transport permanently closed this non-reused generation.
    ConnectionClosed { connection: ConnectionGeneration },
    /// A validated packet, its connection and its ingress order.
    Packet {
        connection: ConnectionGeneration,
        ingress_order: u64,
        packet: InboundPacket,
    },
    /// A PUBACK whose association was frozen by the adapter before entering
    /// its input queue. `None` is a completed observation without a target,
    /// never a request to look up the current occupant later. Product TCP
    /// observes after dequeue/validation of the reader channel, not at the
    /// earlier socket read. This carries no authority for unobserved bytes.
    ObservedPuback {
        connection: ConnectionKey,
        packet_id: OutboundPacketId,
        reason: u8,
        target: Option<PubackTarget>,
    },
    /// The store's answer to one effect, for one operation.
    StoreResult {
        effect: EffectId,
        operation: OperationId,
        reply: StoreReply,
    },
    /// Progress actually confirmed by the connection adapter.
    FrameResult {
        effect: EffectId,
        connection: ConnectionKey,
        progress: FrameProgress,
    },
    /// A wake-up. It carries no authority to create an obligation.
    WorkAvailable { session: SessionId },
    /// An armed deadline was reached at this explicit time.
    Deadline {
        token: DeadlineToken,
        now: LogicalTime,
    },
    /// Resume a counted continuation within this budget.
    Continue {
        continuation: ContinuationId,
        budget: WorkUnits,
    },
    /// A bounded page read from durable data. `last` closes the inventory.
    Recovery {
        facts: BoundedVec<RecoveredFact>,
        last: bool,
    },
}

/// Why the core closes a connection.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CloseReason {
    /// The packet is outside the implemented subset or violates the protocol.
    ProtocolError,
    /// A newer connection took the session over.
    SessionTakenOver,
    /// The outcome of a commit is unknown; the client must recover.
    OutcomeUnknown,
    /// A resource needed by the connection can never be granted.
    CapacityExceeded,
}

/// An effect: a responsibility transferred to an adapter.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Effect {
    /// A known failed session establishment has no invented SessionId.
    /// Reason is a valid MQTT 5 CONNACK failure code; transport then closes.
    SessionRefused {
        connection: ConnectionGeneration,
        reason: u8,
    },
    /// Install a correlated known lifecycle result in the transport/cache
    /// adapter before accepting subsequent frames for that binding.
    SessionChanged {
        operation: OperationId,
        connection: Option<ConnectionGeneration>,
        client_id: SharedBytes,
        change: crate::session::SessionChange,
    },
    /// A dedicated control lane; it never consumes a business receipt slot.
    CollectReceipts {
        collection: crate::receipts::ReceiptCollection,
    },
    /// One volatile attempt. No Packet Identifier, durable claim, or retry permission.
    EmitQos0 {
        connection: ConnectionKey,
        origin: Qos0Origin,
        retain: bool,
        topic: SharedBytes,
        payload: SharedBytes,
    },
    /// An explicit bounded owner decision, including drops before any adapter transfer.
    Qos0Dropped {
        origin: Qos0Origin,
        target: Option<SessionId>,
        reason: Qos0DropReason,
    },
    /// Propose a frozen command.
    Persist { command: Command },
    /// Reconcile an operation and, if possible, establish a safe fence. It
    /// has its own `EffectId` while targeting the same `OperationId`.
    ResolveAndFence { operation: OperationId },
    /// Read a coherent bounded page of existing obligations.
    LoadWork {
        session: SessionId,
        limit: u32,
        after: Option<DeliveryId>,
    },
    EmitPublish {
        connection: ConnectionKey,
        delivery: DeliveryId,
        packet_id: OutboundPacketId,
        claim: OperationId,
        topic: SharedBytes,
        payload: SharedBytes,
        dup: bool,
        retain: bool,
    },
    /// Emit one precise frame on one connection generation.
    EmitFrame {
        connection: ConnectionKey,
        frame: OutboundFrame,
        /// PUBLISH occurrence on this connection, frozen by the core when
        /// making the ACK eligible. Never inferred later from a reusable
        /// Packet Identifier. None for frames without a current PUBLISH.
        publish_order: Option<u64>,
    },
    /// Close the designated connection.
    CloseConnection {
        connection: ConnectionGeneration,
        reason: CloseReason,
    },
    /// Arm a deadline token. The delay is counted by the timer adapter from
    /// its acceptance of this effect. Published after the `Persist` it
    /// watches, it is dispatched only once that `Persist` was accepted, so
    /// the delay never starts before the operation was submitted (ADR 0001).
    ArmDeadline {
        token: DeadlineToken,
        after: LogicalDuration,
    },
    /// Run again a piece of work that is already counted.
    Continue { continuation: ContinuationId },
}

/// An effect together with its identity. It is moved, never copied: at any
/// time exactly one party owns it.
#[derive(PartialEq, Eq, Debug)]
pub struct OwnedEffect {
    id: EffectId,
    effect: Effect,
}

impl OwnedEffect {
    /// Binds an effect to its identity.
    #[must_use]
    pub fn new(id: EffectId, effect: Effect) -> Self {
        Self { id, effect }
    }

    /// The identity of this submission.
    #[must_use]
    pub fn id(&self) -> EffectId {
        self.id
    }

    /// The effect.
    #[must_use]
    pub fn effect(&self) -> &Effect {
        &self.effect
    }

    /// Takes the effect apart; only an adapter that accepted it does so.
    #[must_use]
    pub fn into_parts(self) -> (EffectId, Effect) {
        (self.id, self.effect)
    }
}

/// Why an adapter did not take an effect.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SubmitBlock {
    /// The adapter's queue is full.
    QueueFull,
    /// The bound on operations in flight is reached.
    InFlightLimit,
    /// The adapter no longer accepts work.
    Closed,
}

/// Result of a non-blocking submission.
#[must_use]
#[derive(PartialEq, Eq, Debug)]
pub enum SubmitResult {
    /// Ownership moved to the adapter, which tracks the effect until an
    /// outcome or a defined resolution. Afterwards a cancellation, a timeout
    /// or a closed connection does not mean the operation did not happen.
    Accepted,
    /// The same owned effect comes back; its reservations stay valid.
    NotSubmitted {
        effect: OwnedEffect,
        reason: SubmitBlock,
    },
}

/// A non-blocking adapter entry point, outside the core.
pub trait EffectSink {
    /// Takes the effect, or hands the same effect back.
    fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult;
}

/// Identity of an effect box: the data set and run epoch of the authority
/// that created it, and the serial that authority gave it (ADR 0009).
///
/// The serial alone tells apart every box of one authority, including the
/// successive boxes of one owner. The owner is kept beside it to say whose
/// box this is; it adds nothing to the uniqueness.
///
/// Only an [`OwnerAuthority`] builds an identity. Two authorities issued
/// for the same incarnation and epoch would give equal identities: what
/// prevents that is the issuance of the grant an authority consumes
/// ([`OwnerAuthority::from_grant`]), not this type.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct OutboxId {
    incarnation: StoreIncarnation,
    epoch: RunEpoch,
    owner: LogicalOwnerId,
    serial: OutboxSerial,
}

impl OutboxId {
    /// The data set of the authority that created the box.
    #[must_use]
    pub const fn incarnation(self) -> StoreIncarnation {
        self.incarnation
    }

    /// The run epoch of the authority that created the box.
    #[must_use]
    pub const fn epoch(self) -> RunEpoch {
        self.epoch
    }

    /// The owner the box was created for.
    #[must_use]
    pub const fn owner(self) -> LogicalOwnerId {
        self.owner
    }

    /// The serial of the box under its authority.
    #[must_use]
    pub const fn serial(self) -> OutboxSerial {
        self.serial
    }
}

/// Key of the slots reserved in one effect box.
///
/// The key carries no authority of its own. It names a box, an entry of
/// that box's reservation table and the generation of that entry; the box
/// checks all three before it changes anything. A key may be kept in owner
/// state across turns. Once its slots are all published, or released, the
/// entry moves to the next generation and the key is stale.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct EffectReservation {
    outbox: OutboxId,
    slot: u32,
    generation: u64,
}

/// Why a box refuses a reservation key. Nothing changed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReservationError {
    /// The key was issued by another box.
    ForeignOutbox,
    /// The key names no entry of this box's reservation table.
    UnknownSlot,
    /// The reservation was used up or released; the entry moved on.
    Stale,
    /// The box found its own accounting broken. This is a core fault.
    Inconsistent,
}

/// Why a box refuses a reservation. Nothing changed.
///
/// A temporary lack names a wake-up that can happen and after which a
/// retry is useful. The two other refusals are definitive for this box
/// and name none: waiting cannot end them. When several refusals hold at
/// once the first of this order is reported: `LargerThanBox`,
/// `TableExhausted`, then the needs, effect slots before table entries.
///
/// A wake-up names every event that can end the lack. Lack of effect
/// slots: `EffectAccepted` when no live reservation holds a slot, and
/// `EffectAcceptedOrResourceReleased` as soon as one does. Lack of table
/// entries: `ResourceReleased`, which is the end of a reservation, by
/// [`Outbox::release`] or by the publication of its last slot
/// ([`Outbox::publish`] returning zero).
///
/// A transition makes one reservation, for all the slots its plan needs
/// (ADR 0006). The box cannot tell whose reservation holds a slot: a plan
/// reserved in several calls whose sum exceeds the box would be answered
/// with a wait that its own release ends, again and again. Reserved in one
/// call, it is `LargerThanBox`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReserveRefused {
    /// A temporary lack: retry after the wake-up it names.
    Need(CapacityNeed),
    /// The request exceeds the whole capacity of the box. No dispatch can
    /// make room for it.
    LargerThanBox { requested: usize, capacity: usize },
    /// No entry of the reservation table can ever be taken again: every
    /// entry is spent, or is held at its last generation and will be spent
    /// when its reservation ends. Only a new box, created by the authority
    /// after this one is retired, reserves again; and this one is retired
    /// only once every reservation it still holds was published or
    /// released, and its effects accepted.
    TableExhausted,
}

/// A publication that was refused: the same effect and the reason.
#[derive(PartialEq, Eq, Debug)]
pub struct PublishRefused {
    pub effect: OwnedEffect,
    pub reason: ReservationError,
}

/// One entry of a reservation table.
#[derive(Clone, Copy, Debug)]
struct ReservationRecord {
    /// Generation of the reservation the entry holds or will hold next.
    generation: u64,
    /// Slots reserved and not yet published.
    remaining: usize,
    /// Whether a live reservation holds the entry.
    held: bool,
    /// The generation counter is exhausted; the entry is never reused.
    spent: bool,
}

/// What one dispatch pass did.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DispatchReport {
    /// Effects whose ownership moved to the adapter, in order.
    pub accepted: usize,
    /// Why the pass stopped early; the refused effect is back at the head.
    pub blocked: Option<SubmitBlock>,
}

/// The bounded effect box of one owner.
///
/// A transition reserves its slots, mutates, then publishes into the
/// reserved slots; publication cannot fail for lack of room. The dispatcher
/// moves effects to an adapter in order and puts a refused effect back in
/// its place without recreating it.
///
/// Invariant: effects held plus slots reserved never exceed the capacity,
/// and the slots reserved equal the sum over the live reservations.
#[derive(Debug)]
pub struct Outbox {
    id: OutboxId,
    queue: BoundedQueue<OwnedEffect>,
    records: BoundedVec<ReservationRecord>,
    reserved: usize,
}

impl Outbox {
    /// Preallocates a box of exactly `capacity` slots that can hold up to
    /// `max_reservations` reservations at a time.
    ///
    /// Private: a box is created by an [`OwnerAuthority`] and by nothing
    /// else (ADR 0009).
    fn with_capacity(
        id: OutboxId,
        capacity: usize,
        max_reservations: usize,
    ) -> Result<Self, AllocationFailed> {
        let mut records = BoundedVec::with_capacity(max_reservations)?;
        for _ in 0..max_reservations {
            let free = ReservationRecord {
                generation: 0,
                remaining: 0,
                held: false,
                spent: false,
            };
            records.try_push(free).map_err(|_| AllocationFailed)?;
        }
        Ok(Self {
            id,
            queue: BoundedQueue::with_capacity(capacity)?,
            records,
            reserved: 0,
        })
    }

    /// The identity of this box.
    #[must_use]
    pub fn id(&self) -> OutboxId {
        self.id
    }

    /// The slots the box was created with: filled, reserved and free
    /// together. It never changes. An owner compares it, once, with the
    /// largest reservation one of its transitions makes (ADR 0006,
    /// amendment).
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Slots neither filled nor reserved.
    #[must_use]
    pub fn free_slots(&self) -> usize {
        self.queue.capacity() - self.queue.len() - self.reserved
    }

    /// Effects waiting for an adapter.
    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Whether no effect waits.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Whether a reservation is live: slots reserved and not yet published
    /// or released.
    #[must_use]
    pub fn has_live_reservations(&self) -> bool {
        self.reserved > 0
    }

    /// The waiting effects, oldest first.
    pub fn pending(&self) -> impl Iterator<Item = &OwnedEffect> {
        self.queue.iter()
    }

    /// Reserves `slots` places, or says why not. Nothing changes on
    /// refusal. The refusals and their wake-ups are those of
    /// [`ReserveRefused`].
    pub fn reserve(&mut self, slots: NonZeroUsize) -> Result<EffectReservation, ReserveRefused> {
        let slots = slots.get();
        let capacity = self.queue.capacity();
        if slots > capacity {
            return Err(ReserveRefused::LargerThanBox {
                requested: slots,
                capacity,
            });
        }
        let records = self.records.as_slice();
        let entry = records
            .iter()
            .position(|record| !record.held && !record.spent);
        // An entry held at its last generation is spent when its
        // reservation ends: no end of it frees an entry. The table is
        // scanned for one that will be freed only when none is free now.
        if entry.is_none()
            && !records
                .iter()
                .any(|record| record.held && record.generation != u64::MAX)
        {
            return Err(ReserveRefused::TableExhausted);
        }
        let free = self.free_slots();
        if slots > free {
            let missing = slots - free;
            // Every event that can end the lack is named: the end of a
            // live reservation frees slots whatever the adapters do.
            let wake = if self.reserved == 0 {
                WakeCondition::EffectAccepted
            } else {
                WakeCondition::EffectAcceptedOrResourceReleased
            };
            return Err(ReserveRefused::Need(CapacityNeed {
                resource: Resource::EffectSlots,
                amount: missing as u64,
                wake,
            }));
        }
        let Some(index) = entry else {
            return Err(ReserveRefused::Need(CapacityNeed {
                resource: Resource::Reservations,
                amount: 1,
                wake: WakeCondition::ResourceReleased,
            }));
        };
        let record = &mut self.records.as_mut_slice()[index];
        record.held = true;
        record.remaining = slots;
        self.reserved += slots;
        // The table holds at most `u32::MAX` entries by construction.
        Ok(EffectReservation {
            outbox: self.id,
            slot: index as u32,
            generation: record.generation,
        })
    }

    /// Finds the live entry a key designates, changing nothing.
    fn locate(&self, key: EffectReservation) -> Result<usize, ReservationError> {
        if key.outbox != self.id {
            return Err(ReservationError::ForeignOutbox);
        }
        let index = usize::try_from(key.slot).map_err(|_| ReservationError::UnknownSlot)?;
        let record = self
            .records
            .as_slice()
            .get(index)
            .ok_or(ReservationError::UnknownSlot)?;
        if !record.held || record.generation != key.generation {
            return Err(ReservationError::Stale);
        }
        if record.remaining == 0 || record.remaining > self.reserved {
            return Err(ReservationError::Inconsistent);
        }
        Ok(index)
    }

    /// Ends the reservation held by an entry and moves it to its next
    /// generation, so that every key of the ended reservation is stale.
    fn retire(&mut self, index: usize) {
        let record = &mut self.records.as_mut_slice()[index];
        record.held = false;
        record.remaining = 0;
        match record.generation.checked_add(1) {
            Some(next) => record.generation = next,
            None => record.spent = true,
        }
    }

    /// Slots a live reservation still holds.
    pub fn remaining(&self, key: EffectReservation) -> Result<usize, ReservationError> {
        self.locate(key)
            .map(|index| self.records.as_slice()[index].remaining)
    }

    /// Puts an effect into a slot of the reservation `key`.
    ///
    /// The key is checked before anything changes. A refusal hands the same
    /// effect back with the reason and leaves the box untouched.
    ///
    /// Returns the slots the reservation still holds. Zero means that the
    /// reservation ended: its table entry is released, which is the
    /// `ResourceReleased` wake-up of a lack of table entries.
    #[expect(
        clippy::result_large_err,
        reason = "a refusal returns the owned effect intact without another allocation"
    )]
    pub fn publish(
        &mut self,
        key: EffectReservation,
        effect: OwnedEffect,
    ) -> Result<usize, PublishRefused> {
        let index = match self.locate(key) {
            Ok(index) => index,
            Err(reason) => return Err(PublishRefused { effect, reason }),
        };
        if let Err(effect) = self.queue.try_push_back(effect) {
            return Err(PublishRefused {
                effect,
                reason: ReservationError::Inconsistent,
            });
        }
        self.reserved -= 1;
        let record = &mut self.records.as_mut_slice()[index];
        record.remaining -= 1;
        let remaining = record.remaining;
        if remaining == 0 {
            self.retire(index);
        }
        Ok(remaining)
    }

    /// Gives back the unpublished slots of a reservation and returns how
    /// many they were. The key is checked before anything changes.
    pub fn release(&mut self, key: EffectReservation) -> Result<usize, ReservationError> {
        let index = self.locate(key)?;
        let returned = self.records.as_slice()[index].remaining;
        self.reserved -= returned;
        self.retire(index);
        Ok(returned)
    }

    /// Dispatch only one collector attempt and its deadline around blocked
    /// ordinary effects. One bounded rotation preserves every remaining
    /// effect's relative order; a refused attempt keeps its deadline held.
    pub fn dispatch_receipt_controls(
        &mut self,
        sink: &mut dyn EffectSink,
        collection: EffectId,
        deadline: DeadlineToken,
    ) -> DispatchReport {
        let mut waiting = self.queue.iter().any(|effect| effect.id() == collection);
        let mut accepted = 0;
        let mut blocked = None;
        let count = self.queue.len();
        for _ in 0..count {
            let Some(effect) = self.queue.pop_front() else {
                break;
            };
            let is_collection = effect.id() == collection
                && matches!(effect.effect(), Effect::CollectReceipts { .. });
            let is_deadline = !waiting
                && matches!(effect.effect(),Effect::ArmDeadline {token,..} if *token == deadline);
            let restore = if is_collection || is_deadline {
                match sink.try_submit(effect) {
                    SubmitResult::Accepted => {
                        accepted += 1;
                        if is_collection {
                            waiting = false;
                        }
                        None
                    }
                    SubmitResult::NotSubmitted { effect, reason } => {
                        blocked = Some(reason);
                        Some(effect)
                    }
                }
            } else {
                Some(effect)
            };
            if let Some(effect) = restore {
                let restored = self.queue.try_push_back(effect);
                debug_assert!(restored.is_ok(), "the scanned slot was taken");
            }
        }
        DispatchReport { accepted, blocked }
    }

    /// Bounded session controls can bypass blocked ordinary/network effects.
    /// Lifecycle cuts are explicitly ordered against old commands by durable
    /// SessionId/attachment guards, not by the ordinary dispatch lane.
    /// A blocked control fences later controls in this pass. The adapter also
    /// fences a pending physical close/cache installation before another send.
    pub fn dispatch_session_controls(
        &mut self,
        sink: &mut dyn EffectSink,
        flight: Option<(OperationId, DeadlineToken)>,
    ) -> DispatchReport {
        let mut waiting = flight.is_some_and(|(operation, _)| self.queue.iter().any(|e| {
            matches!(e.effect(), Effect::Persist {command} if command.recipe.operation() == operation)
        }));
        let mut accepted = 0;
        let mut blocked = None;
        let count = self.queue.len();
        for _ in 0..count {
            let Some(effect) = self.queue.pop_front() else {
                break;
            };
            let lifecycle_persist = matches!(effect.effect(), Effect::Persist {command} if matches!(command.recipe.get(), crate::command::Recipe::SessionLifecycle(_)));
            let lifecycle_resolve = !waiting
                && matches!(effect.effect(), Effect::ResolveAndFence {operation} if flight.is_some_and(|(current,_)| *operation == current));
            let lifecycle_deadline = !waiting
                && matches!(effect.effect(), Effect::ArmDeadline {token,..} if flight.is_some_and(|(_,current)| *token == current));
            let selected = lifecycle_persist
                || lifecycle_resolve
                || lifecycle_deadline
                || matches!(
                    effect.effect(),
                    Effect::CloseConnection { .. }
                        | Effect::SessionChanged { .. }
                        | Effect::SessionRefused { .. }
                );
            let restore = if selected && blocked.is_none() {
                match sink.try_submit(effect) {
                    SubmitResult::Accepted => {
                        accepted += 1;
                        if lifecycle_persist {
                            waiting = false;
                        }
                        None
                    }
                    SubmitResult::NotSubmitted { effect, reason } => {
                        blocked = Some(reason);
                        Some(effect)
                    }
                }
            } else {
                Some(effect)
            };
            if let Some(effect) = restore {
                let restored = self.queue.try_push_back(effect);
                debug_assert!(restored.is_ok(), "the scanned slot was taken");
            }
        }
        DispatchReport { accepted, blocked }
    }

    /// Moves effects to `sink` in order until one is refused. The refused
    /// effect returns to the head of the box with the same identity.
    pub fn dispatch(&mut self, sink: &mut dyn EffectSink) -> DispatchReport {
        let mut accepted = 0;
        while let Some(effect) = self.queue.pop_front() {
            match sink.try_submit(effect) {
                SubmitResult::Accepted => accepted += 1,
                SubmitResult::NotSubmitted { effect, reason } => {
                    // The slot freed by `pop_front` above is still free.
                    let restored = self.queue.try_push_front(effect);
                    debug_assert!(restored.is_ok(), "the popped slot was taken");
                    return DispatchReport {
                        accepted,
                        blocked: Some(reason),
                    };
                }
            }
        }
        DispatchReport {
            accepted,
            blocked: None,
        }
    }
}

/// Why an authority refuses to create a box. Nothing changed: the table of
/// active owners, the boxes that exist and the next serial are as before.
///
/// When several refusals hold at once the first of this order is reported:
/// `TooLarge`, `SerialsExhausted`, `OwnerAlreadyActive`, `TableFull`,
/// `AllocationFailed`. The definitive ones come first so that none is
/// reported as a wait that a retirement or a retry could end. A box of no
/// slot or of no reservation entry cannot be asked for: both dimensions
/// are non-zero by type.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CreateRefused {
    /// The dimensions exceed what any allocation can hold on this
    /// platform, whatever memory is free. Definitive for these dimensions.
    TooLarge,
    /// The serial counter reached its end. Definitive: nothing wraps, no
    /// retirement frees a serial, and this authority creates no box again.
    SerialsExhausted,
    /// The owner holds a live box. Retiring that box lifts the refusal.
    OwnerAlreadyActive,
    /// The bounded table of the authority has no free entry. The table
    /// has at least one entry, so a box exists whose retirement lifts the
    /// refusal.
    TableFull,
    /// Memory could not be obtained for dimensions that an allocation can
    /// hold. Recoverable: the caller may try again.
    AllocationFailed,
}

/// Whether `count` items of `T` fit the largest allocation there is.
fn fits_an_allocation<T>(count: usize) -> bool {
    count
        .checked_mul(size_of::<T>())
        .is_some_and(|bytes| bytes <= isize::MAX as usize)
}

/// Why an authority refuses to retire a box.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RetireRefusal {
    /// The box was created by an authority of another incarnation or epoch.
    ForeignAuthority,
    /// The box is not the exact active entry of its owner, serial included.
    NotActiveEntry,
    /// The box still holds an effect no adapter accepted.
    PendingEffects,
    /// The box still holds a live reservation.
    LiveReservations,
}

/// A retirement that was refused: the same box and the reason. The
/// authority is unchanged.
#[derive(Debug)]
pub struct RetireRefused {
    pub outbox: Outbox,
    pub reason: RetireRefusal,
}

/// Why an authority refuses to replace a box. Nothing changed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReplaceRefusal {
    /// The box cannot be retired: the reason of [`OwnerAuthority::retire`].
    Retire(RetireRefusal),
    /// The new box cannot be created: the reason of
    /// [`OwnerAuthority::create_outbox`]. `OwnerAlreadyActive` and
    /// `TableFull` never occur, since the new box takes the entry of the
    /// old one.
    Create(CreateRefused),
}

/// An authority that was not issued: the same grant comes back intact, so
/// that trying again needs neither a second hand-over nor a new epoch.
/// The one cause is that the table of active owners could not be
/// allocated.
#[derive(Debug)]
pub struct GrantRefused {
    pub grant: EpochGrant,
}

/// One entry of the table of active owners.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct ActiveBox {
    owner: LogicalOwnerId,
    serial: OutboxSerial,
}

/// The creation authority for effect boxes (ADR 0009): a construction
/// token held by the one component that creates owners.
///
/// It is neither `Clone` nor `Copy`. It gives each box a serial from a
/// checked counter that is never reused, and keeps, in a bounded table,
/// which owner holds which live box.
///
/// Invariant: at any time an owner has at most one live box, and no two
/// boxes ever created by this authority share an identity, so a
/// reservation key is accepted by the box that issued it and by no other.
///
/// Start-up precondition, which this type cannot check: no two grants
/// exist for one incarnation and epoch. One `EpochOpening` gives one
/// grant; that a process drives one opening per store it opened, and
/// that each committed opening names an epoch no earlier one named, is
/// established by the contract of opening an epoch and by the wiring of
/// start-up.
#[derive(Debug)]
pub struct OwnerAuthority {
    incarnation: StoreIncarnation,
    epoch: RunEpoch,
    /// Owners that hold a live box, in no particular order.
    active: BoundedVec<ActiveBox>,
    /// The serial of the next box, or `None` once the last one was used.
    next_serial: Option<OutboxSerial>,
}

impl OwnerAuthority {
    /// Issues the authority of the opened epoch `grant` belongs to, able to
    /// keep up to `max_owners` owners active at a time. The table has at
    /// least one entry: an authority that could create no box is not
    /// issued.
    ///
    /// This is the one function that constructs an authority (ADR 0009).
    /// The grant is consumed: an `EpochGrant` comes out of one transition
    /// of one `EpochOpening`, taken once, so one committed opening gives
    /// one authority. When the table cannot be allocated the same grant
    /// comes back and may be presented again.
    pub fn from_grant(grant: EpochGrant, max_owners: NonZeroU32) -> Result<Self, GrantRefused> {
        match usize::try_from(max_owners.get()) {
            Ok(table) => Self::with_table(grant, table),
            Err(_) => Err(GrantRefused { grant }),
        }
    }

    /// `from_grant`, with the bound of the table as the platform counts it.
    fn with_table(grant: EpochGrant, table: usize) -> Result<Self, GrantRefused> {
        match BoundedVec::with_capacity(table) {
            Ok(active) => Ok(Self {
                incarnation: grant.incarnation(),
                epoch: grant.epoch(),
                active,
                next_serial: Some(OutboxSerial::new(0)),
            }),
            Err(AllocationFailed) => Err(GrantRefused { grant }),
        }
    }

    /// The data set this authority was issued for.
    #[must_use]
    pub fn incarnation(&self) -> StoreIncarnation {
        self.incarnation
    }

    /// The run epoch this authority was issued for.
    #[must_use]
    pub fn epoch(&self) -> RunEpoch {
        self.epoch
    }

    /// Number of owners that hold a live box.
    #[must_use]
    pub fn active_owners(&self) -> usize {
        self.active.len()
    }

    /// The identity of the live box of `owner`, if it holds one.
    #[must_use]
    pub fn active_box(&self, owner: LogicalOwnerId) -> Option<OutboxId> {
        self.active
            .as_slice()
            .iter()
            .find(|entry| entry.owner == owner)
            .map(|entry| OutboxId {
                incarnation: self.incarnation,
                epoch: self.epoch,
                owner,
                serial: entry.serial,
            })
    }

    /// Creates the box of `owner`, with exactly `capacity` slots and up to
    /// `max_reservations` reservations at a time, and records it; or
    /// changes nothing and says why.
    ///
    /// Both dimensions are non-zero by type, like the slots of a
    /// reservation. The caller takes `capacity` from the effect slots of
    /// its profile (an `EffectSlots` quantity) and converts it once, when
    /// it creates the owner.
    pub fn create_outbox(
        &mut self,
        owner: LogicalOwnerId,
        capacity: NonZeroUsize,
        max_reservations: NonZeroU32,
    ) -> Result<Outbox, CreateRefused> {
        // Every check and every allocation comes before the entry is
        // published; nothing below the last `?` can fail.
        let capacity = capacity.get();
        let table = usize::try_from(max_reservations.get())
            .ok()
            .filter(|&table| fits_an_allocation::<ReservationRecord>(table))
            .ok_or(CreateRefused::TooLarge)?;
        if !fits_an_allocation::<OwnedEffect>(capacity) {
            return Err(CreateRefused::TooLarge);
        }
        let serial = self.next_serial.ok_or(CreateRefused::SerialsExhausted)?;
        if self.active.as_slice().iter().any(|e| e.owner == owner) {
            return Err(CreateRefused::OwnerAlreadyActive);
        }
        if self.active.is_full() {
            return Err(CreateRefused::TableFull);
        }
        let id = OutboxId {
            incarnation: self.incarnation,
            epoch: self.epoch,
            owner,
            serial,
        };
        let outbox = Outbox::with_capacity(id, capacity, table)
            .map_err(|_| CreateRefused::AllocationFailed)?;
        let published = self.active.try_push(ActiveBox { owner, serial });
        debug_assert!(published.is_ok(), "the table was checked not full");
        // The serial is spent whatever follows; the last one leaves `None`.
        self.next_serial = serial.checked_next().ok();
        Ok(outbox)
    }

    /// Replaces the box of an owner by a new one, with a new serial and
    /// the given dimensions, at once: the owner stays active throughout,
    /// and no other owner can take its identity in between.
    ///
    /// The box is checked as `retire` checks it, the new one as
    /// `create_outbox` does, and the new one is allocated, before anything
    /// changes. The box is replaced in place, so that its holder keeps a
    /// box at every moment: on any refusal, a failed allocation included,
    /// `outbox` and the authority are unchanged. On success the old box is
    /// dropped: it held no effect and no live reservation.
    pub fn replace_outbox(
        &mut self,
        outbox: &mut Outbox,
        capacity: NonZeroUsize,
        max_reservations: NonZeroU32,
    ) -> Result<(), ReplaceRefusal> {
        let id = outbox.id();
        let index = self.retirable(outbox).map_err(ReplaceRefusal::Retire)?;
        // Every check and every allocation comes before the entry changes;
        // nothing below the allocation can fail.
        let capacity = capacity.get();
        let refuse = |reason| Err(ReplaceRefusal::Create(reason));
        let Some(table) = usize::try_from(max_reservations.get())
            .ok()
            .filter(|&table| fits_an_allocation::<ReservationRecord>(table))
        else {
            return refuse(CreateRefused::TooLarge);
        };
        if !fits_an_allocation::<OwnedEffect>(capacity) {
            return refuse(CreateRefused::TooLarge);
        }
        let Some(serial) = self.next_serial else {
            return refuse(CreateRefused::SerialsExhausted);
        };
        let new_id = OutboxId { serial, ..id };
        let Ok(replacement) = Outbox::with_capacity(new_id, capacity, table) else {
            return refuse(CreateRefused::AllocationFailed);
        };
        let entry = &mut self.active.as_mut_slice()[index];
        entry.serial = serial;
        // The serial is spent; the last one leaves `None`.
        self.next_serial = serial.checked_next().ok();
        *outbox = replacement;
        Ok(())
    }

    /// The entry of the table `outbox` occupies, when the box may be
    /// retired; otherwise why not. Nothing changes.
    fn retirable(&self, outbox: &Outbox) -> Result<usize, RetireRefusal> {
        let id = outbox.id();
        if id.incarnation != self.incarnation || id.epoch != self.epoch {
            return Err(RetireRefusal::ForeignAuthority);
        }
        let entry = ActiveBox {
            owner: id.owner,
            serial: id.serial,
        };
        let Some(index) = self.active.as_slice().iter().position(|e| *e == entry) else {
            return Err(RetireRefusal::NotActiveEntry);
        };
        if !outbox.is_empty() {
            return Err(RetireRefusal::PendingEffects);
        }
        if outbox.has_live_reservations() {
            return Err(RetireRefusal::LiveReservations);
        }
        Ok(index)
    }

    /// Retires a box: its owner is no longer active and may be given a new
    /// box, with a new serial. On a refusal the same box comes back and the
    /// authority is unchanged.
    ///
    /// An empty box does not mean that its owner is at rest: replies to
    /// effects already accepted still belong to the owner, and retiring
    /// the box does not end its life cycle.
    pub fn retire(&mut self, outbox: Outbox) -> Result<(), RetireRefused> {
        let index = match self.retirable(&outbox) {
            Ok(index) => index,
            Err(reason) => return Err(RetireRefused { outbox, reason }),
        };
        let id = outbox.id();
        let entry = ActiveBox {
            owner: id.owner,
            serial: id.serial,
        };
        // Remove the entry: move the last one into its place, then drop
        // the last. The table keeps no order.
        let last = self.active.len() - 1;
        self.active.as_mut_slice().swap(index, last);
        let removed = self.active.pop();
        debug_assert!(removed == Some(entry), "another entry was removed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::EffectSeq;

    pub(super) fn effect(sequence: u64) -> OwnedEffect {
        OwnedEffect::new(
            EffectId {
                epoch: RunEpoch::new(1),
                owner: LogicalOwnerId::new(0),
                sequence: EffectSeq::new(sequence),
            },
            Effect::LoadWork {
                session: SessionId::new(sequence),
                limit: 8,
                after: None,
            },
        )
    }

    pub(super) fn slots(count: usize) -> NonZeroUsize {
        NonZeroUsize::new(count).expect("at least one slot")
    }

    /// An identity for the tests of the box as a primitive.
    fn box_id(owner: u64) -> OutboxId {
        OutboxId {
            incarnation: StoreIncarnation::new(1),
            epoch: RunEpoch::new(1),
            owner: LogicalOwnerId::new(owner),
            serial: OutboxSerial::new(owner),
        }
    }

    fn outbox_of(owner: u64, capacity: u64) -> Outbox {
        Outbox::with_capacity(box_id(owner), capacity as usize, 4).expect("small")
    }

    /// The accounting invariant, recomputed from the reservation table.
    fn assert_invariant(outbox: &Outbox) {
        let live: usize = outbox
            .records
            .as_slice()
            .iter()
            .filter(|record| record.held)
            .map(|record| record.remaining)
            .sum();
        assert_eq!(
            outbox.reserved, live,
            "reserved slots drifted from the table"
        );
        assert!(outbox.queue.len() + outbox.reserved <= outbox.queue.capacity());
        for record in outbox.records.as_slice() {
            assert!(record.held || record.remaining == 0);
        }
    }

    fn filled(capacity: u64, count: u64) -> Outbox {
        let mut outbox = outbox_of(0, capacity);
        if count > 0 {
            let key = outbox.reserve(slots(count as usize)).expect("room");
            for sequence in 0..count {
                outbox.publish(key, effect(sequence)).expect("reserved");
            }
        }
        assert_invariant(&outbox);
        outbox
    }

    fn sequences(outbox: &Outbox) -> Vec<u64> {
        outbox.pending().map(|e| e.id().sequence.get()).collect()
    }

    /// A snapshot of everything a refused operation must leave unchanged.
    fn snapshot(outbox: &Outbox) -> (Vec<u64>, usize, usize) {
        (sequences(outbox), outbox.free_slots(), outbox.reserved)
    }

    /// Accepts a fixed number of effects, then reports a full queue.
    pub(super) struct LimitedSink {
        pub(super) room: usize,
        pub(super) taken: Vec<OwnedEffect>,
    }

    impl EffectSink for LimitedSink {
        fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
            if self.taken.len() >= self.room {
                return SubmitResult::NotSubmitted {
                    effect,
                    reason: SubmitBlock::QueueFull,
                };
            }
            self.taken.push(effect);
            SubmitResult::Accepted
        }
    }

    #[test]
    fn a_full_sink_loses_no_effect_and_changes_no_identity() {
        for room in 0..=4 {
            let mut outbox = filled(4, 4);
            let mut sink = LimitedSink {
                room,
                taken: Vec::new(),
            };
            let report = outbox.dispatch(&mut sink);
            assert_eq!(report.accepted, room.min(4));
            assert_eq!(report.blocked.is_some(), room < 4);
            let mut seen: Vec<u64> = sink.taken.iter().map(|e| e.id().sequence.get()).collect();
            seen.extend(sequences(&outbox));
            assert_eq!(seen, vec![0, 1, 2, 3], "room {room}");
            for owned in sink.taken.iter().chain(outbox.pending()) {
                let sequence = owned.id().sequence.get();
                assert_eq!(*owned, effect(sequence), "content changed");
            }
            assert_invariant(&outbox);
        }
    }

    #[test]
    fn a_refused_effect_is_retried_first_once_room_returns() {
        let mut outbox = filled(3, 3);
        let mut sink = LimitedSink {
            room: 1,
            taken: Vec::new(),
        };
        assert_eq!(
            outbox.dispatch(&mut sink),
            DispatchReport {
                accepted: 1,
                blocked: Some(SubmitBlock::QueueFull)
            }
        );
        assert_eq!(sequences(&outbox), vec![1, 2]);
        sink.room = 3;
        assert_eq!(
            outbox.dispatch(&mut sink),
            DispatchReport {
                accepted: 2,
                blocked: None
            }
        );
        assert!(outbox.is_empty());
        let order: Vec<u64> = sink.taken.iter().map(|e| e.id().sequence.get()).collect();
        assert_eq!(order, vec![0, 1, 2]);
    }

    #[test]
    fn a_refused_reservation_changes_nothing() {
        let mut outbox = filled(3, 2);
        let before = snapshot(&outbox);
        let need = outbox.reserve(slots(2)).expect_err("only one slot is free");
        assert_eq!(
            need,
            ReserveRefused::Need(CapacityNeed {
                resource: Resource::EffectSlots,
                amount: 1,
                wake: WakeCondition::EffectAccepted,
            })
        );
        assert_eq!(snapshot(&outbox), before);
        assert_invariant(&outbox);
    }

    #[test]
    fn reserved_slots_cannot_be_taken_by_someone_else() {
        let mut outbox = outbox_of(0, 2);
        let first = outbox.reserve(slots(2)).expect("room");
        assert_eq!(outbox.free_slots(), 0);
        assert!(outbox.reserve(slots(1)).is_err());
        outbox.publish(first, effect(0)).expect("reserved");
        assert_eq!(outbox.remaining(first), Ok(1));
        assert_eq!(outbox.free_slots(), 0);
        assert_eq!(outbox.release(first), Ok(1));
        assert_eq!(outbox.free_slots(), 1);
        assert_invariant(&outbox);
    }

    #[test]
    fn a_reservation_cannot_publish_into_another_outbox() {
        let mut first = outbox_of(1, 2);
        let mut second = outbox_of(2, 2);
        // Give the second box a live reservation in the same table entry,
        // so that only the identity of the box tells the keys apart.
        let own = second.reserve(slots(1)).expect("room");
        let foreign = first.reserve(slots(1)).expect("room");
        let before = (snapshot(&first), snapshot(&second));

        let refused = second.publish(foreign, effect(0)).expect_err("foreign key");
        assert_eq!(
            refused,
            PublishRefused {
                effect: effect(0),
                reason: ReservationError::ForeignOutbox
            }
        );
        assert_eq!((snapshot(&first), snapshot(&second)), before);
        assert_eq!(second.remaining(own), Ok(1));
        assert_invariant(&first);
        assert_invariant(&second);

        first
            .publish(foreign, effect(0))
            .expect("its own box accepts the key");
        assert_eq!(sequences(&first), vec![0]);
    }

    #[test]
    fn a_reservation_cannot_be_released_into_another_outbox() {
        let mut first = outbox_of(1, 2);
        let mut second = outbox_of(2, 2);
        let own = second.reserve(slots(2)).expect("room");
        let foreign = first.reserve(slots(1)).expect("room");
        let before = (snapshot(&first), snapshot(&second));

        assert_eq!(
            second.release(foreign),
            Err(ReservationError::ForeignOutbox)
        );
        assert_eq!((snapshot(&first), snapshot(&second)), before);
        assert_eq!(second.remaining(own), Ok(2));
        assert_invariant(&first);
        assert_invariant(&second);
        assert_eq!(first.release(foreign), Ok(1));
    }

    #[test]
    fn a_key_is_stale_once_its_reservation_is_released() {
        let mut outbox = outbox_of(0, 2);
        let key = outbox.reserve(slots(2)).expect("room");
        assert_eq!(outbox.release(key), Ok(2));
        let before = snapshot(&outbox);

        let refused = outbox.publish(key, effect(0)).expect_err("stale key");
        assert_eq!(
            refused,
            PublishRefused {
                effect: effect(0),
                reason: ReservationError::Stale
            }
        );
        assert_eq!(
            outbox.release(key),
            Err(ReservationError::Stale),
            "double release"
        );
        assert_eq!(outbox.remaining(key), Err(ReservationError::Stale));
        assert_eq!(snapshot(&outbox), before);
        assert_invariant(&outbox);
    }

    #[test]
    fn a_stale_key_never_touches_the_reservation_that_reuses_its_entry() {
        let mut outbox = outbox_of(0, 2);
        let old = outbox.reserve(slots(1)).expect("room");
        assert_eq!(outbox.release(old), Ok(1));
        let new = outbox.reserve(slots(2)).expect("room");
        assert_ne!(old, new, "the entry moved to its next generation");
        let before = snapshot(&outbox);

        let refused = outbox.publish(old, effect(9)).expect_err("stale key");
        assert_eq!(refused.reason, ReservationError::Stale);
        assert_eq!(refused.effect, effect(9));
        assert_eq!(outbox.release(old), Err(ReservationError::Stale));
        assert_eq!(snapshot(&outbox), before);
        assert_eq!(outbox.remaining(new), Ok(2));
        assert_invariant(&outbox);
    }

    #[test]
    fn a_key_is_stale_once_its_reservation_is_used_up() {
        let mut outbox = outbox_of(0, 2);
        let key = outbox.reserve(slots(1)).expect("room");
        outbox.publish(key, effect(0)).expect("reserved");
        let before = snapshot(&outbox);

        let refused = outbox.publish(key, effect(1)).expect_err("used up");
        assert_eq!(
            refused,
            PublishRefused {
                effect: effect(1),
                reason: ReservationError::Stale
            }
        );
        assert_eq!(outbox.release(key), Err(ReservationError::Stale));
        assert_eq!(snapshot(&outbox), before);
        assert_eq!(sequences(&outbox), vec![0]);
        assert_eq!(outbox.free_slots(), 1);
        assert_invariant(&outbox);
    }

    #[test]
    fn a_key_naming_no_entry_is_refused() {
        let mut outbox = outbox_of(0, 2);
        let key = EffectReservation {
            outbox: outbox.id(),
            slot: 99,
            generation: 0,
        };
        let refused = outbox.publish(key, effect(0)).expect_err("no such entry");
        assert_eq!(
            refused,
            PublishRefused {
                effect: effect(0),
                reason: ReservationError::UnknownSlot
            }
        );
        assert_eq!(outbox.release(key), Err(ReservationError::UnknownSlot));
        assert_invariant(&outbox);
    }

    #[test]
    fn a_full_reservation_table_is_a_stated_need() {
        let mut outbox = Outbox::with_capacity(box_id(0), 8, 2).expect("small");
        let first = outbox.reserve(slots(1)).expect("room");
        let _second = outbox.reserve(slots(1)).expect("room");
        let before = snapshot(&outbox);
        assert_eq!(
            outbox.reserve(slots(1)),
            Err(ReserveRefused::Need(CapacityNeed {
                resource: Resource::Reservations,
                amount: 1,
                wake: WakeCondition::ResourceReleased,
            }))
        );
        assert_eq!(snapshot(&outbox), before);
        assert_eq!(outbox.release(first), Ok(1));
        assert!(
            outbox.reserve(slots(1)).is_ok(),
            "a released entry is reused"
        );
        assert_invariant(&outbox);
    }

    #[test]
    fn an_entry_whose_generation_is_exhausted_is_never_reused() {
        let mut outbox = outbox_of(0, 2);
        outbox.records.as_mut_slice()[0].generation = u64::MAX;
        let last = outbox.reserve(slots(1)).expect("room");
        assert_eq!(last.slot, 0);
        assert_eq!(outbox.release(last), Ok(1));
        assert!(outbox.records.as_slice()[0].spent);
        let next = outbox.reserve(slots(1)).expect("room");
        assert_ne!(next.slot, 0, "the spent entry was reused");
        assert_eq!(outbox.release(last), Err(ReservationError::Stale));
        assert_invariant(&outbox);
    }

    /// Spends every entry of the reservation table: each one is taken at
    /// its last generation and released.
    fn spend_every_entry(outbox: &mut Outbox) {
        for record in outbox.records.as_mut_slice() {
            record.generation = u64::MAX;
        }
        for _ in 0..outbox.records.len() {
            let key = outbox.reserve(slots(1)).expect("an entry is left");
            assert_eq!(outbox.release(key), Ok(1));
        }
        assert!(outbox.records.as_slice().iter().all(|record| record.spent));
    }

    // Open condition "Exhausted reservation table": no release can free an
    // entry of a table whose entries are all spent, so the refusal must
    // not name a wake-up.
    #[test]
    fn a_table_whose_entries_are_all_spent_is_not_reported_as_a_wait() {
        let mut outbox = outbox_of(0, 4);
        spend_every_entry(&mut outbox);
        assert_eq!(outbox.free_slots(), 4, "the slots themselves are free");
        let before = snapshot(&outbox);
        assert_eq!(
            outbox.reserve(slots(1)),
            Err(ReserveRefused::TableExhausted),
            "a definitive exhaustion was reported as a wait"
        );
        assert_eq!(snapshot(&outbox), before);
        assert_invariant(&outbox);
    }

    #[test]
    fn a_table_of_no_entry_is_exhausted_from_the_start() {
        let mut outbox = Outbox::with_capacity(box_id(0), 4, 0).expect("small");
        let before = snapshot(&outbox);
        assert_eq!(
            outbox.reserve(slots(1)),
            Err(ReserveRefused::TableExhausted)
        );
        assert_eq!(snapshot(&outbox), before);
    }

    #[test]
    fn a_spent_table_is_reported_before_a_lack_of_slots() {
        // The box is full as well: dispatching would free slots, and no
        // reservation could use them.
        let mut outbox = filled(2, 2);
        spend_every_entry_of_a_full_box(&mut outbox);
        assert_eq!(outbox.free_slots(), 0);
        let before = snapshot(&outbox);
        assert_eq!(
            outbox.reserve(slots(1)),
            Err(ReserveRefused::TableExhausted)
        );
        assert_eq!(snapshot(&outbox), before);
    }

    /// Marks every entry spent without reserving: the box has no free slot.
    fn spend_every_entry_of_a_full_box(outbox: &mut Outbox) {
        for record in outbox.records.as_mut_slice() {
            assert!(!record.held);
            record.spent = true;
        }
    }

    #[test]
    fn a_lack_of_entries_ends_when_a_reservation_ends_by_release_or_by_publication() {
        let need = Err(ReserveRefused::Need(CapacityNeed {
            resource: Resource::Reservations,
            amount: 1,
            wake: WakeCondition::ResourceReleased,
        }));
        for by_publication in [false, true] {
            let mut outbox = Outbox::with_capacity(box_id(0), 4, 2).expect("small");
            // One entry is spent; the other is held and has generations left.
            outbox.records.as_mut_slice()[0].spent = true;
            let held = outbox.reserve(slots(2)).expect("an entry is left");
            assert_eq!(held.slot, 1);
            let before = snapshot(&outbox);
            assert_eq!(outbox.reserve(slots(1)), need);
            assert_eq!(snapshot(&outbox), before);

            if by_publication {
                assert_eq!(outbox.publish(held, effect(0)), Ok(1));
                assert_eq!(outbox.reserve(slots(1)), need, "the reservation is live");
                assert_eq!(
                    outbox.publish(held, effect(1)),
                    Ok(0),
                    "the last slot ends the reservation"
                );
            } else {
                assert_eq!(outbox.release(held), Ok(2));
            }
            let next = outbox.reserve(slots(1)).expect("the entry was released");
            assert_eq!(next.slot, 1);
            assert_ne!(next.generation, held.generation);
            assert_invariant(&outbox);
        }
    }

    #[test]
    fn a_request_larger_than_the_box_is_not_reported_as_a_wait() {
        let mut outbox = filled(3, 1);
        let before = snapshot(&outbox);
        assert_eq!(
            outbox.reserve(slots(4)),
            Err(ReserveRefused::LargerThanBox {
                requested: 4,
                capacity: 3
            })
        );
        // The whole capacity is a wait while an effect holds a slot.
        assert_eq!(
            outbox.reserve(slots(3)),
            Err(ReserveRefused::Need(CapacityNeed {
                resource: Resource::EffectSlots,
                amount: 1,
                wake: WakeCondition::EffectAccepted,
            }))
        );
        assert_eq!(snapshot(&outbox), before);

        // Larger than the box comes first, even on a spent table.
        spend_every_entry_of_a_full_box(&mut outbox);
        assert_eq!(
            outbox.reserve(slots(4)),
            Err(ReserveRefused::LargerThanBox {
                requested: 4,
                capacity: 3
            })
        );
        assert_eq!(
            outbox.reserve(slots(1)),
            Err(ReserveRefused::TableExhausted)
        );
    }

    // Review of 437edc2, finding 5: slots held by live reservations are
    // not freed by the acceptance of an effect. A lack that the queued
    // effects alone cannot cover must not name `EffectAccepted` alone.
    #[test]
    fn a_lack_of_slots_held_by_reservations_does_not_wait_for_an_acceptance_alone() {
        let lack = |amount, wake| {
            Err(ReserveRefused::Need(CapacityNeed {
                resource: Resource::EffectSlots,
                amount,
                wake,
            }))
        };
        // Capacity 4: one effect queued, three slots reserved.
        let mut outbox = filled(4, 1);
        let held = outbox.reserve(slots(3)).expect("room");
        let before = snapshot(&outbox);
        assert_eq!(
            outbox.reserve(slots(2)),
            lack(2, WakeCondition::EffectAcceptedOrResourceReleased)
        );
        assert_eq!(snapshot(&outbox), before);

        // Each named event can happen, and after both the retry succeeds.
        let mut sink = LimitedSink {
            room: 1,
            taken: Vec::new(),
        };
        assert_eq!(outbox.dispatch(&mut sink).accepted, 1);
        assert_eq!(
            outbox.reserve(slots(2)),
            lack(1, WakeCondition::EffectAcceptedOrResourceReleased),
            "no effect waits any more; a reservation holds the rest"
        );
        assert_eq!(outbox.release(held), Ok(3));
        assert!(outbox.reserve(slots(2)).is_ok());

        // An empty queue, every slot reserved: no acceptance can happen
        // until a reservation publishes or is released.
        let mut outbox = outbox_of(0, 2);
        let held = outbox.reserve(slots(2)).expect("room");
        assert_eq!(
            outbox.reserve(slots(1)),
            lack(1, WakeCondition::EffectAcceptedOrResourceReleased)
        );
        // The reservation publishes its two slots and ends: only waiting
        // effects hold the box, and only an acceptance frees a slot.
        assert_eq!(outbox.publish(held, effect(0)), Ok(1));
        assert_eq!(
            outbox.reserve(slots(1)),
            lack(1, WakeCondition::EffectAcceptedOrResourceReleased),
            "the reservation still holds a slot"
        );
        assert_eq!(outbox.publish(held, effect(1)), Ok(0));
        assert_eq!(
            outbox.reserve(slots(1)),
            lack(1, WakeCondition::EffectAccepted)
        );
        assert_invariant(&outbox);
    }

    // Review of 6d603d0, finding 3: a transition makes one reservation,
    // for all the slots of its plan. The box cannot tell whose reservation
    // holds a slot, so a plan split over several calls is answered with a
    // wait that its own release ends, and the retry repeats.
    #[test]
    fn a_plan_larger_than_the_box_is_definitive_only_when_reserved_in_one_call() {
        let mut outbox = outbox_of(0, 4);
        // The plan needs five slots. Split in two calls:
        for _attempt in 0..3 {
            let provisional = outbox.reserve(slots(2)).expect("room");
            assert_eq!(
                outbox.reserve(slots(3)),
                Err(ReserveRefused::Need(CapacityNeed {
                    resource: Resource::EffectSlots,
                    amount: 1,
                    wake: WakeCondition::EffectAcceptedOrResourceReleased,
                })),
                "the box sees a wait"
            );
            // ADR 0006: the refused transition returns its provisional
            // reservation, which is the event it was told to wait for.
            assert_eq!(outbox.release(provisional), Ok(2));
        }
        // Reserved in one call, the same plan is refused for good.
        let before = snapshot(&outbox);
        assert_eq!(
            outbox.reserve(slots(5)),
            Err(ReserveRefused::LargerThanBox {
                requested: 5,
                capacity: 4
            })
        );
        assert_eq!(snapshot(&outbox), before);
    }

    // Review of 6d603d0, finding 1: a wake-up must name every event that
    // can end the lack. While a live reservation holds a slot, its release
    // can end a lack of slots even when no adapter accepts anything.
    #[test]
    fn a_lack_of_slots_names_the_release_of_a_live_reservation() {
        // Capacity 4: one effect queued, three slots reserved.
        let mut outbox = filled(4, 1);
        let held = outbox.reserve(slots(3)).expect("room");
        let refusal = outbox.reserve(slots(1)).expect_err("no slot is free");
        assert_eq!(
            refusal,
            ReserveRefused::Need(CapacityNeed {
                resource: Resource::EffectSlots,
                amount: 1,
                wake: WakeCondition::EffectAcceptedOrResourceReleased,
            }),
            "the release of the reservation was not named"
        );

        // The adapter is closed for good; the release alone makes room.
        struct Closed;
        impl EffectSink for Closed {
            fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
                SubmitResult::NotSubmitted {
                    effect,
                    reason: SubmitBlock::Closed,
                }
            }
        }
        assert_eq!(outbox.dispatch(&mut Closed).accepted, 0);
        assert_eq!(outbox.release(held), Ok(3));
        assert!(outbox.reserve(slots(1)).is_ok(), "the box has room");
        assert_invariant(&outbox);
    }

    // Review of 437edc2, finding 6: an entry held at its last generation
    // is spent when its reservation ends, however it ends. A table that
    // has nothing else is exhausted already.
    #[test]
    fn a_table_whose_only_entries_left_are_held_at_their_last_generation_is_exhausted() {
        let mut outbox = Outbox::with_capacity(box_id(0), 4, 2).expect("small");
        for record in outbox.records.as_mut_slice() {
            record.generation = u64::MAX;
        }
        let first = outbox.reserve(slots(1)).expect("an entry is left");
        let second = outbox.reserve(slots(1)).expect("an entry is left");
        let before = snapshot(&outbox);
        assert_eq!(
            outbox.reserve(slots(1)),
            Err(ReserveRefused::TableExhausted),
            "no end of these reservations frees an entry"
        );
        assert_eq!(snapshot(&outbox), before);
        // The live reservations are untouched.
        assert_eq!(outbox.remaining(first), Ok(1));
        assert_eq!(outbox.remaining(second), Ok(1));
    }

    #[test]
    fn a_deadline_is_dispatched_only_after_the_persist_it_watches() {
        use crate::ids::{DeadlineToken, OperationId, OperationSeq};

        // Refuses store effects and accepts everything else.
        struct StoreBusy(Vec<OwnedEffect>);
        impl EffectSink for StoreBusy {
            fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
                if matches!(effect.effect(), Effect::ResolveAndFence { .. }) {
                    return SubmitResult::NotSubmitted {
                        effect,
                        reason: SubmitBlock::InFlightLimit,
                    };
                }
                self.0.push(effect);
                SubmitResult::Accepted
            }
        }
        let id = |sequence| EffectId {
            epoch: RunEpoch::new(1),
            owner: LogicalOwnerId::new(0),
            sequence: EffectSeq::new(sequence),
        };
        let operation = OperationId::new(
            StoreIncarnation::new(1),
            RunEpoch::new(1),
            LogicalOwnerId::new(0),
            OperationSeq::new(1),
        );
        let mut outbox = outbox_of(0, 2);
        let key = outbox.reserve(slots(2)).expect("room");
        // A store effect first, then the deadline that watches it.
        let store = OwnedEffect::new(id(0), Effect::ResolveAndFence { operation });
        let deadline = OwnedEffect::new(
            id(1),
            Effect::ArmDeadline {
                token: DeadlineToken::new(1),
                after: LogicalDuration::new(5),
            },
        );
        outbox.publish(key, store).expect("reserved");
        outbox.publish(key, deadline).expect("reserved");

        let mut sink = StoreBusy(Vec::new());
        let report = outbox.dispatch(&mut sink);
        assert_eq!(
            report,
            DispatchReport {
                accepted: 0,
                blocked: Some(SubmitBlock::InFlightLimit)
            }
        );
        assert!(
            sink.0.is_empty(),
            "the deadline overtook the effect it watches"
        );
        assert_eq!(sequences(&outbox), vec![0, 1]);
    }

    #[test]
    fn a_reservation_held_across_dispatch_keeps_its_slots() {
        let mut outbox = filled(4, 2);
        let held = outbox.reserve(slots(2)).expect("room");
        let mut sink = LimitedSink {
            room: 8,
            taken: Vec::new(),
        };
        assert_eq!(
            outbox.dispatch(&mut sink),
            DispatchReport {
                accepted: 2,
                blocked: None
            }
        );
        assert_eq!(outbox.free_slots(), 2, "dispatch freed the held slots");
        outbox.publish(held, effect(7)).expect("still reserved");
        outbox.publish(held, effect(8)).expect("still reserved");
        assert_eq!(sequences(&outbox), vec![7, 8]);
        assert_invariant(&outbox);
    }

    #[test]
    fn effects_survive_between_turns_while_the_sink_stays_closed() {
        struct ClosedSink;
        impl EffectSink for ClosedSink {
            fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
                SubmitResult::NotSubmitted {
                    effect,
                    reason: SubmitBlock::Closed,
                }
            }
        }
        let mut outbox = filled(2, 2);
        for _ in 0..3 {
            assert_eq!(
                outbox.dispatch(&mut ClosedSink),
                DispatchReport {
                    accepted: 0,
                    blocked: Some(SubmitBlock::Closed)
                }
            );
            assert_eq!(sequences(&outbox), vec![0, 1]);
        }
    }
}

/// The tests named by ADR 0009 for the creation authority.
#[cfg(test)]
mod authority_tests {
    use super::tests::{LimitedSink, effect, slots};
    use super::*;
    use crate::command::StartupNonce;
    use crate::outcome::{
        EpochOpened, EpochOpening, HandOverRefused, Observed, OpeningOutcome, OpeningReply,
        OpeningStage,
    };

    const INCARNATION: u64 = 7;
    const EPOCH: u64 = 3;

    fn authority_of(incarnation: u64, epoch: u64, max_owners: u32) -> OwnerAuthority {
        let (_opening, grant) = opened(incarnation, epoch);
        OwnerAuthority::from_grant(grant, owners(max_owners)).expect("small")
    }

    fn authority(max_owners: u32) -> OwnerAuthority {
        authority_of(INCARNATION, EPOCH, max_owners)
    }

    fn owner(raw: u64) -> LogicalOwnerId {
        LogicalOwnerId::new(raw)
    }

    fn create(authority: &mut OwnerAuthority, raw: u64) -> Result<Outbox, CreateRefused> {
        authority.create_outbox(owner(raw), slots(4), entries(2))
    }

    /// Accepts every effect.
    fn taking() -> LimitedSink {
        LimitedSink {
            room: usize::MAX,
            taken: Vec::new(),
        }
    }

    /// The largest capacity an allocation can express. No allocator
    /// provides it, so creating such a box fails for lack of memory.
    fn unobtainable() -> NonZeroUsize {
        slots(isize::MAX as usize / size_of::<OwnedEffect>())
    }

    fn entries(count: u32) -> NonZeroU32 {
        NonZeroU32::new(count).expect("at least one entry")
    }

    /// Everything a refusal must leave unchanged in an authority: the
    /// table of active owners, in its order, and the next serial.
    fn authority_state(authority: &OwnerAuthority) -> (Vec<ActiveBox>, Option<OutboxSerial>) {
        (authority.active.as_slice().to_vec(), authority.next_serial)
    }

    /// Everything that identifies a box and its content.
    fn box_state(outbox: &Outbox) -> (OutboxId, Vec<u64>, usize, usize) {
        (
            outbox.id(),
            outbox.pending().map(|e| e.id().sequence.get()).collect(),
            outbox.free_slots(),
            outbox.reserved,
        )
    }

    /// The grant of a committed opening of `epoch`, obtained the way the
    /// product obtains it: submit, observe the committed reply, hand over.
    fn opened(incarnation: u64, epoch: u64) -> (EpochOpening, EpochGrant) {
        let incarnation = StoreIncarnation::new(incarnation);
        let last_opened = RunEpoch::new(epoch - 1);
        let mut opening = EpochOpening::new(incarnation, StartupNonce::new([1; 16]));
        let command = opening.submit(last_opened).expect("nothing in flight");
        let committed = EpochOpened {
            incarnation,
            epoch: RunEpoch::new(epoch),
        };
        let reply = OpeningReply {
            command,
            outcome: OpeningOutcome::Committed(committed),
        };
        assert_eq!(
            opening.observe(reply),
            Ok(Observed::Advanced(OpeningStage::Committed(committed))),
            "the reply of this attempt"
        );
        let grant = opening.hand_over().expect("committed");
        (opening, grant)
    }

    fn owners(count: u32) -> NonZeroU32 {
        NonZeroU32::new(count).expect("at least one owner")
    }

    // ADR 0009: the one constructor of an authority consumes the grant of
    // a committed opening, and takes its identity from it.
    #[test]
    fn an_authority_is_issued_from_the_grant_of_a_committed_opening() {
        let (mut opening, grant) = opened(INCARNATION, EPOCH);
        let mut authority = OwnerAuthority::from_grant(grant, owners(2)).expect("small");
        assert_eq!(authority.incarnation(), StoreIncarnation::new(INCARNATION));
        assert_eq!(authority.epoch(), RunEpoch::new(EPOCH));
        assert_eq!(authority.active_owners(), 0);
        let outbox = create(&mut authority, 1).expect("first box of the owner");
        assert_eq!(outbox.id().epoch(), RunEpoch::new(EPOCH));

        // The grant moved into the authority, and the opening gives no
        // second one: there is no way to a second authority.
        assert_eq!(
            opening.hand_over().map(|grant| grant.opened()),
            Err(HandOverRefused::AlreadyHandedOver)
        );
    }

    // Review of 437edc2, finding 4: a failed allocation hands the same
    // grant back, and the same grant then issues the authority.
    #[test]
    fn a_failed_allocation_hands_the_grant_back_and_the_same_grant_then_succeeds() {
        let (mut opening, grant) = opened(INCARNATION, EPOCH);
        let before = grant.opened();
        // The public bound is thirty-two bits wide and cannot ask for more
        // than an allocator may provide; the function `from_grant`
        // delegates to takes the largest table an allocation can express.
        let unobtainable = isize::MAX as usize / size_of::<ActiveBox>();
        let refused = OwnerAuthority::with_table(grant, unobtainable).expect_err("no memory");
        assert_eq!(refused.grant.opened(), before, "another grant came back");

        let authority = OwnerAuthority::from_grant(refused.grant, owners(2))
            .expect("the same grant, a table that fits");
        assert_eq!(authority.incarnation(), before.incarnation);
        assert_eq!(authority.epoch(), before.epoch);
        // The failure issued nothing and the retry asked for no new grant.
        assert_eq!(
            opening.hand_over().map(|grant| grant.opened()),
            Err(HandOverRefused::AlreadyHandedOver)
        );
    }

    #[test]
    fn a_created_box_carries_the_identity_of_its_authority() {
        let mut authority = authority(2);
        let outbox = create(&mut authority, 5).expect("first box of the owner");
        let id = outbox.id();
        assert_eq!(id.incarnation(), StoreIncarnation::new(INCARNATION));
        assert_eq!(id.epoch(), RunEpoch::new(EPOCH));
        assert_eq!(id.owner(), owner(5));
        assert_eq!(authority.active_box(owner(5)), Some(id));
        assert_eq!(authority.active_box(owner(6)), None);
        assert_eq!(authority.active_owners(), 1);
        assert_eq!(outbox.free_slots(), 4);
        assert!(outbox.is_empty());
        assert!(!outbox.has_live_reservations());
    }

    // ADR 0009, context: two creations for one owner in one incarnation.
    #[test]
    fn a_second_creation_for_an_active_owner_is_refused() {
        let mut authority = authority(4);
        let first = create(&mut authority, 1).expect("first box of the owner");
        let before = authority_state(&authority);
        assert_eq!(
            create(&mut authority, 1).map(|outbox| outbox.id()),
            Err(CreateRefused::OwnerAlreadyActive)
        );
        assert_eq!(authority_state(&authority), before);
        assert_eq!(authority.active_box(owner(1)), Some(first.id()));
    }

    // ADR 0009, context: destruction then recreation, with an old key kept.
    #[test]
    fn a_key_of_a_retired_box_is_foreign_to_the_box_that_replaces_it() {
        let mut authority = authority(4);
        let mut old = create(&mut authority, 1).expect("first box of the owner");
        let old_key = old.reserve(slots(1)).expect("room");
        assert_eq!(old.release(old_key), Ok(1));
        authority.retire(old).expect("empty and unreserved");

        let mut new = create(&mut authority, 1).expect("the owner is free again");
        // The new box holds a live reservation in the same table entry and
        // at the same generation: only the serial tells the keys apart.
        let own = new.reserve(slots(1)).expect("room");
        assert_eq!(
            (own.slot, own.generation),
            (old_key.slot, old_key.generation)
        );
        let before = box_state(&new);

        let refused = new.publish(old_key, effect(0)).expect_err("old key");
        assert_eq!(refused.reason, ReservationError::ForeignOutbox);
        assert_eq!(refused.effect, effect(0));
        assert_eq!(new.release(old_key), Err(ReservationError::ForeignOutbox));
        assert_eq!(new.remaining(old_key), Err(ReservationError::ForeignOutbox));
        assert_eq!(box_state(&new), before);
        assert_eq!(new.remaining(own), Ok(1));
    }

    // ADR 0009, context: no silent reuse of an identity.
    #[test]
    fn no_identity_is_given_twice() {
        let mut authority = authority(3);
        let mut seen: Vec<OutboxId> = Vec::new();
        let mut live: Vec<Outbox> = Vec::new();
        for round in 0..4u64 {
            for raw in 0..3u64 {
                let outbox = create(&mut authority, raw).expect("the owner is free");
                assert!(
                    !seen.contains(&outbox.id()),
                    "round {round}: identity {:?} was given before",
                    outbox.id()
                );
                assert!(
                    seen.iter().all(|id| id.serial() != outbox.id().serial()),
                    "round {round}: serial used again"
                );
                seen.push(outbox.id());
                live.push(outbox);
            }
            for outbox in live.drain(..) {
                authority.retire(outbox).expect("empty and unreserved");
            }
        }
        assert_eq!(seen.len(), 12);
    }

    #[test]
    fn a_serial_is_not_used_again_after_its_box_was_retired() {
        let mut authority = authority(1);
        let first = create(&mut authority, 1).expect("first box of the owner");
        let first_serial = first.id().serial();
        authority.retire(first).expect("empty and unreserved");
        let second = create(&mut authority, 1).expect("the owner is free again");
        assert_eq!(
            second.id().serial(),
            first_serial.checked_next().expect("far")
        );
        authority.retire(second).expect("empty and unreserved");
        let third = create(&mut authority, 2).expect("another owner");
        assert!(third.id().serial() > first_serial.checked_next().expect("far"));
    }

    // ADR 0009, context: contexts that could exchange keys by accident are
    // kept apart. The rule of the bench: two simulated stores have
    // distinct incarnations, so their authorities give distinct identities
    // even for the same epoch, owner and serial.
    #[test]
    fn authorities_of_distinct_incarnations_do_not_exchange_keys() {
        let mut here = authority_of(1, EPOCH, 2);
        let mut there = authority_of(2, EPOCH, 2);
        let mut ours = create(&mut here, 1).expect("first box");
        let mut theirs = create(&mut there, 1).expect("first box");
        assert_eq!(ours.id().serial(), theirs.id().serial());
        assert_eq!(ours.id().owner(), theirs.id().owner());
        assert_ne!(ours.id(), theirs.id());

        let own = ours.reserve(slots(1)).expect("room");
        let foreign = theirs.reserve(slots(1)).expect("room");
        assert_eq!(
            (own.slot, own.generation),
            (foreign.slot, foreign.generation)
        );
        let before = (box_state(&ours), box_state(&theirs));
        let refused = ours.publish(foreign, effect(0)).expect_err("foreign key");
        assert_eq!(refused.reason, ReservationError::ForeignOutbox);
        assert_eq!(ours.release(foreign), Err(ReservationError::ForeignOutbox));
        assert_eq!((box_state(&ours), box_state(&theirs)), before);

        // A box cannot be retired by the authority of another context.
        assert_eq!(theirs.release(foreign), Ok(1));
        let refused = here.retire(theirs).expect_err("foreign box");
        assert_eq!(refused.reason, RetireRefusal::ForeignAuthority);
        there.retire(refused.outbox).expect("its own authority");
    }

    // ADR 0009, context: explicit exhaustion of the identity domain.
    #[test]
    fn the_last_serial_creates_a_box_and_the_next_creation_is_exhausted() {
        let mut authority = authority(4);
        authority.next_serial = Some(OutboxSerial::new(u64::MAX - 1));
        let before_last = create(&mut authority, 1).expect("a serial is left");
        assert_eq!(before_last.id().serial(), OutboxSerial::new(u64::MAX - 1));
        let last = create(&mut authority, 2).expect("the last usable serial");
        assert_eq!(last.id().serial(), OutboxSerial::new(u64::MAX));
        assert_eq!(authority.next_serial, None);

        let before = authority_state(&authority);
        assert_eq!(
            create(&mut authority, 3).map(|outbox| outbox.id()),
            Err(CreateRefused::SerialsExhausted)
        );
        assert_eq!(authority_state(&authority), before);

        // Definitive: retiring every box frees no serial.
        authority.retire(before_last).expect("empty and unreserved");
        authority.retire(last).expect("empty and unreserved");
        for raw in 1..=3 {
            assert_eq!(
                create(&mut authority, raw).map(|outbox| outbox.id()),
                Err(CreateRefused::SerialsExhausted)
            );
        }
        assert_eq!(authority.active_owners(), 0);
        assert_eq!(authority.next_serial, None);
    }

    #[test]
    fn exhaustion_is_reported_before_a_refusal_that_a_retirement_could_lift() {
        let mut authority = authority(1);
        authority.next_serial = Some(OutboxSerial::new(u64::MAX));
        let _last = create(&mut authority, 1).expect("the last usable serial");
        // The owner is active and the table is full as well.
        assert_eq!(
            create(&mut authority, 1).map(|outbox| outbox.id()),
            Err(CreateRefused::SerialsExhausted)
        );
        assert_eq!(
            create(&mut authority, 2).map(|outbox| outbox.id()),
            Err(CreateRefused::SerialsExhausted)
        );
    }

    #[test]
    fn a_full_table_refuses_a_new_owner_until_a_box_is_retired() {
        let mut authority = authority(2);
        let first = create(&mut authority, 1).expect("room in the table");
        let _second = create(&mut authority, 2).expect("room in the table");
        let before = authority_state(&authority);
        assert_eq!(
            create(&mut authority, 3).map(|outbox| outbox.id()),
            Err(CreateRefused::TableFull)
        );
        assert_eq!(authority_state(&authority), before);

        authority.retire(first).expect("empty and unreserved");
        let third = create(&mut authority, 3).expect("an entry was freed");
        assert_eq!(authority.active_box(owner(3)), Some(third.id()));
        assert_eq!(authority.active_box(owner(1)), None);
        assert_eq!(authority.active_owners(), 2);
    }

    #[test]
    fn a_failed_allocation_is_recoverable_and_consumes_no_serial() {
        let mut authority = authority(2);
        let before = authority_state(&authority);
        // Dimensions an allocation can express and no allocator provides.
        assert_eq!(
            authority
                .create_outbox(owner(1), unobtainable(), entries(2))
                .map(|outbox| outbox.id()),
            Err(CreateRefused::AllocationFailed)
        );
        assert_eq!(authority_state(&authority), before);
        assert_eq!(authority.active_box(owner(1)), None);

        let outbox = create(&mut authority, 1).expect("the caller may try again");
        assert_eq!(Some(outbox.id().serial()), before.1, "a serial was skipped");
    }

    // Review of 437edc2, finding 1: a size that no allocation can ever
    // satisfy is not a recoverable failure the caller may retry.
    #[test]
    fn a_size_that_can_never_be_allocated_is_not_reported_as_recoverable() {
        let mut authority = authority(2);
        let before = authority_state(&authority);
        let one_more = slots(unobtainable().get() + 1);
        for capacity in [one_more, slots(usize::MAX)] {
            assert_eq!(
                authority
                    .create_outbox(owner(1), capacity, entries(2))
                    .map(|outbox| outbox.id()),
                Err(CreateRefused::TooLarge),
                "trying again cannot succeed"
            );
            assert_eq!(authority_state(&authority), before);
        }
        // The largest expressible size is a matter of memory, not of size.
        assert_eq!(
            authority
                .create_outbox(owner(1), unobtainable(), entries(2))
                .map(|outbox| outbox.id()),
            Err(CreateRefused::AllocationFailed)
        );
        assert_eq!(authority_state(&authority), before);
    }

    // ADR 0009: a refused creation leaves unchanged the table of active
    // owners, the boxes that exist and the next serial.
    #[test]
    fn a_refused_creation_changes_nothing() {
        let mut authority = authority(2);
        let mut first = create(&mut authority, 1).expect("room in the table");
        let mut second = create(&mut authority, 2).expect("room in the table");
        let key = first.reserve(slots(2)).expect("room");
        first.publish(key, effect(0)).expect("reserved");
        let other = second.reserve(slots(1)).expect("room");

        let unchanged = |authority: &OwnerAuthority, first: &Outbox, second: &Outbox| {
            (
                authority_state(authority),
                box_state(first),
                box_state(second),
            )
        };
        let before = unchanged(&authority, &first, &second);

        let attempts: [(u64, usize, CreateRefused); 5] = [
            (1, 4, CreateRefused::OwnerAlreadyActive),
            (3, 4, CreateRefused::TableFull),
            (2, unobtainable().get(), CreateRefused::OwnerAlreadyActive),
            // The refusal that depends on the dimensions alone comes first.
            (1, usize::MAX, CreateRefused::TooLarge),
            (3, usize::MAX, CreateRefused::TooLarge),
        ];
        for (raw, capacity, expected) in attempts {
            assert_eq!(
                authority
                    .create_outbox(owner(raw), slots(capacity), entries(2))
                    .map(|outbox| outbox.id()),
                Err(expected),
                "owner {raw}"
            );
            assert_eq!(
                unchanged(&authority, &first, &second),
                before,
                "owner {raw}"
            );
        }

        // Allocation failure, with a free entry so that it is reached.
        first.release(key).expect("live");
        let mut sink = taking();
        assert_eq!(first.dispatch(&mut sink).accepted, 1);
        let first_id = first.id();
        authority.retire(first).expect("empty and unreserved");
        let before = (authority_state(&authority), box_state(&second));
        assert_eq!(
            authority
                .create_outbox(owner(3), unobtainable(), entries(2))
                .map(|outbox| outbox.id()),
            Err(CreateRefused::AllocationFailed)
        );
        assert_eq!((authority_state(&authority), box_state(&second)), before);

        // The boxes that exist still accept their own keys, and the next
        // creation takes the serial the refusals did not consume.
        assert_eq!(second.remaining(other), Ok(1));
        let third = create(&mut authority, 3).expect("an entry is free");
        assert_eq!(Some(third.id().serial()), before.0.1);
        assert_ne!(third.id().serial(), first_id.serial());
    }

    // ADR 0009: on a refusal the same box comes back and the authority is
    // unchanged. One case per condition of retirement.
    #[test]
    fn a_box_with_a_pending_effect_is_not_retired() {
        let mut authority = authority(2);
        let mut outbox = create(&mut authority, 1).expect("first box of the owner");
        let key = outbox.reserve(slots(1)).expect("room");
        outbox.publish(key, effect(4)).expect("reserved");
        let before = (authority_state(&authority), box_state(&outbox));

        let refused = authority.retire(outbox).expect_err("an effect waits");
        assert_eq!(refused.reason, RetireRefusal::PendingEffects);
        let mut outbox = refused.outbox;
        assert_eq!((authority_state(&authority), box_state(&outbox)), before);
        let pending: Vec<&OwnedEffect> = outbox.pending().collect();
        assert_eq!(pending, vec![&effect(4)], "the effect changed");

        // Once an adapter accepted the effect the same box is retired.
        let mut sink = taking();
        assert_eq!(outbox.dispatch(&mut sink).accepted, 1);
        assert_eq!(sink.taken, vec![effect(4)]);
        authority.retire(outbox).expect("empty and unreserved");
        assert_eq!(authority.active_box(owner(1)), None);
    }

    #[test]
    fn a_box_with_a_live_reservation_is_not_retired() {
        let mut authority = authority(2);
        let mut outbox = create(&mut authority, 1).expect("first box of the owner");
        let key = outbox.reserve(slots(2)).expect("room");
        let before = (authority_state(&authority), box_state(&outbox));

        let refused = authority.retire(outbox).expect_err("a reservation is live");
        assert_eq!(refused.reason, RetireRefusal::LiveReservations);
        let mut outbox = refused.outbox;
        assert_eq!((authority_state(&authority), box_state(&outbox)), before);
        assert_eq!(outbox.remaining(key), Ok(2), "the key lost its slots");

        assert_eq!(outbox.release(key), Ok(2));
        authority.retire(outbox).expect("empty and unreserved");
    }

    #[test]
    fn a_box_of_another_epoch_is_not_retired() {
        let mut authority = authority(2);
        let mut older = authority_of(INCARNATION, EPOCH - 1, 2);
        let _own = create(&mut authority, 1).expect("first box of the owner");
        let foreign = create(&mut older, 1).expect("first box of the owner");
        let before = (authority_state(&authority), box_state(&foreign));

        let refused = authority.retire(foreign).expect_err("another epoch");
        assert_eq!(refused.reason, RetireRefusal::ForeignAuthority);
        assert_eq!(
            (authority_state(&authority), box_state(&refused.outbox)),
            before
        );
        older.retire(refused.outbox).expect("its own authority");
    }

    #[test]
    fn a_box_that_is_not_the_active_entry_of_its_owner_is_not_retired() {
        let mut authority = authority(2);
        let live = create(&mut authority, 1).expect("first box of the owner");
        // Boxes this authority never recorded, with its incarnation and
        // epoch: one for the active owner under another serial, one for an
        // owner that is not active. Only this module can build them.
        for (raw, serial) in [(1, 99), (2, live.id().serial().get())] {
            let stray_id = OutboxId {
                incarnation: authority.incarnation(),
                epoch: authority.epoch(),
                owner: owner(raw),
                serial: OutboxSerial::new(serial),
            };
            let stray = Outbox::with_capacity(stray_id, 4, 2).expect("small");
            let before = (authority_state(&authority), box_state(&stray));

            let refused = authority.retire(stray).expect_err("not the active entry");
            assert_eq!(refused.reason, RetireRefusal::NotActiveEntry, "owner {raw}");
            assert_eq!(
                (authority_state(&authority), box_state(&refused.outbox)),
                before
            );
        }
        assert_eq!(authority.active_box(owner(1)), Some(live.id()));
        authority.retire(live).expect("the exact active entry");
    }

    #[test]
    fn the_refusals_of_retirement_have_a_stated_order() {
        // A foreign box that also holds an effect and a reservation.
        let mut authority = authority(2);
        let mut other = authority_of(INCARNATION + 1, EPOCH, 2);
        let mut outbox = create(&mut other, 1).expect("first box of the owner");
        let key = outbox.reserve(slots(2)).expect("room");
        outbox.publish(key, effect(0)).expect("reserved");
        let refused = authority.retire(outbox).expect_err("foreign");
        assert_eq!(refused.reason, RetireRefusal::ForeignAuthority);

        // Its own authority names the effect before the reservation.
        let refused = other.retire(refused.outbox).expect_err("an effect waits");
        assert_eq!(refused.reason, RetireRefusal::PendingEffects);
    }

    // Review of 6d603d0, finding 2: a box whose table is exhausted while
    // reservations are still held at their last generation is renewed
    // only after those reservations ended.
    #[test]
    fn a_box_with_an_exhausted_table_is_renewed_once_its_reservations_ended() {
        let mut authority = authority(1);
        let mut outbox = create(&mut authority, 1).expect("first box of the owner");
        for record in outbox.records.as_mut_slice() {
            record.generation = u64::MAX;
        }
        let published = outbox.reserve(slots(1)).expect("an entry is left");
        let released = outbox.reserve(slots(1)).expect("an entry is left");
        assert_eq!(
            outbox.reserve(slots(1)),
            Err(ReserveRefused::TableExhausted)
        );
        let old_id = outbox.id();

        // The renewal is refused while a reservation lives...
        let refused = authority.retire(outbox).expect_err("two reservations live");
        assert_eq!(refused.reason, RetireRefusal::LiveReservations);
        let mut outbox = refused.outbox;
        assert_eq!(outbox.publish(published, effect(0)), Ok(0));
        let refused = authority.retire(outbox).expect_err("an effect waits");
        assert_eq!(refused.reason, RetireRefusal::PendingEffects);
        let mut outbox = refused.outbox;
        assert_eq!(outbox.dispatch(&mut taking()).accepted, 1);
        let refused = authority.retire(outbox).expect_err("one reservation lives");
        assert_eq!(refused.reason, RetireRefusal::LiveReservations);
        let mut outbox = refused.outbox;
        assert_eq!(outbox.id(), old_id, "another box came back");
        assert_eq!(outbox.release(released), Ok(1));
        assert_eq!(
            outbox.reserve(slots(1)),
            Err(ReserveRefused::TableExhausted)
        );

        // ...and succeeds once they ended: the new box reserves again.
        authority.retire(outbox).expect("empty and unreserved");
        let mut renewed = create(&mut authority, 1).expect("the owner is free again");
        assert_ne!(renewed.id().serial(), old_id.serial());
        assert!(renewed.reserve(slots(1)).is_ok());
    }

    // ADR 0009: dropping a box without retiring it does not free the entry
    // of its owner.
    #[test]
    fn a_box_dropped_without_retirement_leaves_its_owner_active() {
        let mut authority = authority(2);
        let outbox = create(&mut authority, 1).expect("first box of the owner");
        let id = outbox.id();
        let before = authority_state(&authority);
        drop(outbox);

        assert_eq!(authority_state(&authority), before);
        assert_eq!(authority.active_box(owner(1)), Some(id));
        assert_eq!(
            create(&mut authority, 1).map(|outbox| outbox.id()),
            Err(CreateRefused::OwnerAlreadyActive)
        );
        assert_eq!(authority_state(&authority), before);
    }

    #[test]
    fn retiring_a_box_frees_the_entry_of_its_owner_only() {
        let mut authority = authority(3);
        let first = create(&mut authority, 1).expect("room in the table");
        let second = create(&mut authority, 2).expect("room in the table");
        let third = create(&mut authority, 3).expect("room in the table");
        let (second_id, third_id) = (second.id(), third.id());

        authority.retire(first).expect("empty and unreserved");
        assert_eq!(authority.active_box(owner(1)), None);
        assert_eq!(authority.active_box(owner(2)), Some(second_id));
        assert_eq!(authority.active_box(owner(3)), Some(third_id));
        assert_eq!(authority.active_owners(), 2);

        authority.retire(third).expect("empty and unreserved");
        authority.retire(second).expect("empty and unreserved");
        assert_eq!(authority.active_owners(), 0);
    }

    // ADR 0006, amendment: the constructor of an owner compares the
    // capacity of its box with its largest reservation, whatever the box
    // holds at that moment.
    #[test]
    fn the_capacity_of_a_box_is_what_it_was_created_with_whatever_it_holds() {
        let mut authority = authority(1);
        let mut outbox = authority
            .create_outbox(owner(1), slots(4), entries(2))
            .expect("first box");
        assert_eq!(outbox.capacity(), 4);
        let key = outbox.reserve(slots(3)).expect("room");
        assert_eq!(outbox.capacity(), 4, "three slots reserved");
        assert_eq!(outbox.publish(key, effect(1)).map_err(|_| ()), Ok(2));
        assert_eq!(outbox.capacity(), 4, "one effect waits, two slots reserved");
        assert_eq!(outbox.release(key), Ok(2));
        let _ = outbox.dispatch(&mut taking());
        assert_eq!(outbox.capacity(), 4, "empty again");
        assert_eq!(outbox.free_slots(), 4);
    }

    // ----- Replacement of a box (review of the second wave, R2) -----

    #[test]
    fn replacing_a_box_keeps_its_owner_active_under_a_new_serial() {
        let mut authority = authority(2);
        let mut replacement = create(&mut authority, 1).expect("first box");
        let other = create(&mut authority, 2).expect("second box");
        let old = replacement.id();
        assert_eq!(
            authority.replace_outbox(&mut replacement, slots(6), entries(3)),
            Ok(())
        );
        let new = replacement.id();
        assert_eq!(new.owner(), old.owner());
        assert_eq!(new.incarnation(), old.incarnation());
        assert_eq!(new.epoch(), old.epoch());
        assert_ne!(new.serial(), old.serial());
        assert_ne!(new.serial(), other.id().serial());
        assert_eq!(replacement.capacity(), 6);
        assert_eq!(authority.active_box(owner(1)), Some(new));
        assert_eq!(authority.active_box(owner(2)), Some(other.id()));
        assert_eq!(authority.active_owners(), 2);
        // The identity stays taken throughout.
        assert_eq!(
            create(&mut authority, 1).err(),
            Some(CreateRefused::OwnerAlreadyActive)
        );
        // The replacement is the active entry: it retires, the old one
        // would not.
        assert_eq!(authority.retire(replacement).map_err(|_| ()), Ok(()));
        assert_eq!(authority.active_box(owner(1)), None);
        // The serial of the replacement was spent: the next box has another.
        let third = create(&mut authority, 3).expect("a free entry");
        assert_ne!(third.id().serial(), new.serial());
        assert_ne!(third.id().serial(), old.serial());
    }

    #[test]
    fn a_refused_replacement_changes_nothing_and_gives_the_box_back() {
        // The reasons of a creation, a failed allocation included.
        for (capacity, reason) in [
            (slots(usize::MAX), CreateRefused::TooLarge),
            (unobtainable(), CreateRefused::AllocationFailed),
        ] {
            let mut authority = self::authority(1);
            let mut outbox = create(&mut authority, 1).expect("first box");
            let before = (authority_state(&authority), box_state(&outbox));
            assert_eq!(
                authority.replace_outbox(&mut outbox, capacity, entries(1)),
                Err(ReplaceRefusal::Create(reason))
            );
            assert_eq!(
                (authority_state(&authority), box_state(&outbox)),
                before,
                "{reason:?}"
            );
            assert_eq!(
                create(&mut authority, 1).err(),
                Some(CreateRefused::OwnerAlreadyActive),
                "{reason:?}: the identity stays taken"
            );
            // The box kept is still the active entry.
            assert_eq!(
                authority.replace_outbox(&mut outbox, slots(4), entries(1)),
                Ok(())
            );
            assert_eq!(authority.active_box(owner(1)), Some(outbox.id()));
        }
        // No serial left.
        let mut authority = self::authority(1);
        let mut outbox = create(&mut authority, 1).expect("first box");
        authority.next_serial = None;
        let before = (authority_state(&authority), box_state(&outbox));
        assert_eq!(
            authority.replace_outbox(&mut outbox, slots(4), entries(1)),
            Err(ReplaceRefusal::Create(CreateRefused::SerialsExhausted))
        );
        assert_eq!((authority_state(&authority), box_state(&outbox)), before);

        // The reasons of a retirement.
        let mut authority = self::authority(2);
        let mut outbox = create(&mut authority, 1).expect("first box");
        let key = outbox.reserve(slots(1)).expect("room");
        let before = authority_state(&authority);
        assert_eq!(
            authority.replace_outbox(&mut outbox, slots(4), entries(1)),
            Err(ReplaceRefusal::Retire(RetireRefusal::LiveReservations))
        );
        assert_eq!(outbox.publish(key, effect(1)).map_err(|_| ()), Ok(0));
        assert_eq!(
            authority.replace_outbox(&mut outbox, slots(4), entries(1)),
            Err(ReplaceRefusal::Retire(RetireRefusal::PendingEffects))
        );
        assert_eq!(authority_state(&authority), before);
        let _ = outbox.dispatch(&mut taking());
        assert_eq!(
            authority.replace_outbox(&mut outbox, slots(4), entries(1)),
            Ok(())
        );
        let replacement = outbox;
        // The old box is no longer the active entry; a box of another
        // authority never was.
        let mut later = authority_of(INCARNATION, EPOCH + 1, 1);
        let mut foreign = create(&mut later, 1).expect("a box of another epoch");
        let before = authority_state(&authority);
        assert_eq!(
            authority.replace_outbox(&mut foreign, slots(4), entries(1)),
            Err(ReplaceRefusal::Retire(RetireRefusal::ForeignAuthority))
        );
        assert_eq!(authority_state(&authority), before);
        // A box of the same owner under another serial is not the active
        // entry. (Two authorities of one epoch exist only in a test.)
        let mut twin = authority_of(INCARNATION, EPOCH, 3);
        let _first = create(&mut twin, 2).expect("serial 0");
        let _second = create(&mut twin, 3).expect("serial 1");
        let mut stale = create(&mut twin, 1).expect("serial 2");
        assert_ne!(stale.id().serial(), replacement.id().serial());
        assert_eq!(
            authority.replace_outbox(&mut stale, slots(4), entries(1)),
            Err(ReplaceRefusal::Retire(RetireRefusal::NotActiveEntry))
        );
        assert_eq!(authority_state(&authority), before);
        drop(replacement);
    }
}

#[cfg(test)]
mod recovery_order_tests {
    use super::*;
    use crate::ids::{ExchangeGeneration, OperationSeq, PublicationSeq};
    use crate::outcome::Knowledge;

    fn route(topic: &[u8], target: u64) -> RecoveredFact {
        RecoveredFact::Route {
            options: crate::pubsub::SubscriptionOptions::DEFAULT,
            topic: SharedBytes::copy_from(topic, 16).expect("short topic"),
            target: SessionId::new(target),
            target_revision: Revision::new(1),
            max_qos: Qos::ExactlyOnce,
        }
    }

    /// One fact of each rank, first to last.
    fn one_of_each() -> [RecoveredFact; 7] {
        [
            RecoveredFact::Epoch {
                incarnation: StoreIncarnation::new(1),
                epoch: RunEpoch::new(2),
            },
            RecoveredFact::Limits {
                max_targets_per_admission: 4,
                max_topic_bytes: 64,
                max_payload_bytes: PayloadBytes::new(1024),
            },
            RecoveredFact::RouteTable {
                revision: Revision::new(3),
            },
            route(b"a/b", 7),
            RecoveredFact::Session {
                session: SessionId::new(7),
                client_id: SharedBytes::copy_from(b"client", 16).expect("short identifier"),
                revision: Revision::new(1),
                expiry: crate::session::SessionExpiry::Never,
                opened_by: None,
                detached: None,
            },
            RecoveredFact::InboundExchange {
                exchange: ExchangeId {
                    session: SessionId::new(7),
                    generation: ExchangeGeneration::new(1),
                },
                packet_id: InboundPacketId::new(5).expect("non-zero"),
                publication: PublicationId::new(
                    StoreIncarnation::new(1),
                    RunEpoch::new(1),
                    LogicalOwnerId::new(1),
                    PublicationSeq::new(9),
                ),
                phase: InboundPhase::AwaitPubrel,
            },
            RecoveredFact::Operation {
                operation: OperationId::new(
                    StoreIncarnation::new(1),
                    RunEpoch::new(1),
                    LogicalOwnerId::new(1),
                    OperationSeq::new(4),
                ),
                knowledge: Knowledge::Unknown,
            },
        ]
    }

    #[test]
    fn the_ranks_follow_the_order_of_the_inventory() {
        let ranks = one_of_each().map(|fact| fact.rank());
        assert_eq!(
            ranks,
            [
                FactRank::Epoch,
                FactRank::Limits,
                FactRank::RouteTable,
                FactRank::Route,
                FactRank::Session,
                FactRank::InboundExchange,
                FactRank::Operation,
            ]
        );
        assert!(ranks.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn an_inventory_in_order_is_accepted_whole_or_page_by_page() {
        let facts = one_of_each();
        let mut whole = RecoveryOrder::new();
        for fact in &facts {
            assert_eq!(whole.admit(fact), Ok(()));
        }
        // A later page starts anywhere: a fresh check accepts every suffix.
        for start in 0..facts.len() {
            let mut page = RecoveryOrder::new();
            for fact in &facts[start..] {
                assert_eq!(page.admit(fact), Ok(()));
            }
        }
    }

    #[test]
    fn a_fact_of_an_earlier_rank_is_refused_after_a_later_one() {
        let facts = one_of_each();
        for later in 1..facts.len() {
            for earlier in 0..later {
                let mut order = RecoveryOrder::new();
                assert_eq!(order.admit(&facts[later]), Ok(()));
                assert_eq!(
                    order.admit(&facts[earlier]),
                    Err(OrderRefused::RankDecreased {
                        previous: facts[later].rank(),
                        found: facts[earlier].rank(),
                    }),
                    "{:?} after {:?}",
                    facts[earlier].rank(),
                    facts[later].rank()
                );
            }
        }
    }

    #[test]
    fn the_epoch_the_limits_and_the_route_table_appear_at_most_once() {
        for fact in one_of_each() {
            let mut order = RecoveryOrder::new();
            assert_eq!(order.admit(&fact), Ok(()));
            let again = order.admit(&fact);
            if fact.rank().at_most_once() {
                assert_eq!(again, Err(OrderRefused::Repeated(fact.rank())));
            } else {
                assert_eq!(again, Ok(()), "{:?} may repeat", fact.rank());
            }
        }
        let once: [bool; 7] = one_of_each().map(|fact| fact.rank().at_most_once());
        assert_eq!(once, [true, true, true, false, false, false, false]);
    }

    #[test]
    fn routes_are_sorted_by_the_bytes_of_their_topic() {
        let mut order = RecoveryOrder::new();
        assert_eq!(order.admit(&route(b"a", 1)), Ok(()));
        // A topic has several targets.
        assert_eq!(order.admit(&route(b"a", 2)), Ok(()));
        // Bytes, not text: a prefix sorts first, and 0xff after any ASCII.
        assert_eq!(order.admit(&route(b"a/b", 1)), Ok(()));
        assert_eq!(order.admit(&route(b"b", 1)), Ok(()));
        assert_eq!(order.admit(&route(&[0xff], 1)), Ok(()));
        assert_eq!(
            order.admit(&route(b"b", 2)),
            Err(OrderRefused::RoutesUnsorted)
        );
        let mut prefix = RecoveryOrder::new();
        assert_eq!(prefix.admit(&route(b"a/b", 1)), Ok(()));
        assert_eq!(
            prefix.admit(&route(b"a", 1)),
            Err(OrderRefused::RoutesUnsorted)
        );
    }

    #[test]
    fn a_refused_fact_changes_nothing() {
        let facts = one_of_each();
        let mut order = RecoveryOrder::new();
        assert_eq!(order.admit(&route(b"m", 1)), Ok(()));
        let before = order.clone();
        assert!(order.admit(&facts[0]).is_err());
        assert!(order.admit(&route(b"a", 1)).is_err());
        assert_eq!(order, before);
        // The route that was refused did not lower the bar for the next.
        assert_eq!(
            order.admit(&route(b"b", 1)),
            Err(OrderRefused::RoutesUnsorted)
        );
        assert_eq!(order.admit(&route(b"m", 2)), Ok(()));
        assert_eq!(order.admit(&facts[4]), Ok(()));
    }
}
