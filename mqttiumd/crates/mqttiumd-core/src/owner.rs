//! The state of one logical owner and its transitions.
//!
//! Every input goes through the two functions of ADR 0006. `plan` reads
//! the state and computes everything the transition needs: the identities
//! it draws, the change of state, the effects, and nothing else. It
//! mutates nothing. `apply` consumes the plan: it checks the owner and the
//! revision the plan was computed against, then mutates and publishes
//! without a refusal left to meet.
//!
//! [`Owner::step`] orders the acquisitions so that a refusal restores
//! everything. Whatever can be checked without taking it is checked while
//! planning: the work limit, the identity counters, the tables, the
//! credits of the account. The one acquisition a plan cannot check is the
//! reservation of effect slots, and the step makes it last, in one call
//! for all the slots of the plan. When the box refuses, nothing was taken
//! and the input goes back as it came.
//!
//! An owner is built from its effect box, which it takes and keeps: a box
//! exists once, and the creation authority creates no second box for an
//! identity that is active (ADR 0009), so no path builds two owners of one
//! identity. The box is replaced through the owner, [`Owner::renew_box`],
//! and no counter or state of the owner starts again.

use core::num::{NonZeroU32, NonZeroUsize};

use crate::command::{CommandEvent, CommandPhase, OutcomeClass, Verdict};
use crate::contract::capacity::{
    AllocationFailed, BoundedQueue, BoundedVec, CapacityNeed, ExchangeSlots, PayloadBytes,
    ReceiptSlots, References, Resource, WakeCondition, WorkUnits,
};
use crate::contract::command::{
    AdmitPublication, Command, Costs, DeliveryCause, FrozenRecipe, PayloadRef, Recipe, RecipeError,
    ReleaseInbound, Source, Target,
};
use crate::contract::effects::{
    CloseReason, CreateRefused, DispatchReport, Effect, EffectReservation, EffectSink, FactRank,
    FrameProgress, Input, Outbox, OwnedEffect, OwnerAuthority, RecoveredFact, RecoveryOrder,
    RetireRefusal,
};
use crate::contract::ids::{
    AccountId, ConnectionGeneration, ConnectionKey, DeadlineToken, DeliveryId, EffectId, EffectSeq,
    ExchangeGeneration, ExchangeId, Exhausted, InboundPacketId, LeaseGeneration, LeaseKey,
    LogicalDuration, LogicalOwnerId, OperationId, OperationSeq, PayloadId, PayloadSeq,
    PublicationId, PublicationSeq, RequestId, RequestSeq, Revision, RunEpoch, SessionId,
    StoreIncarnation,
};
use crate::contract::outcome::{
    CommitFacts, RejectReason, StoreOutcome, StoreReply, StoreViolation,
};
use crate::contract::packet::{AckReason, InboundPacket, OutboundFrame, Qos, SharedBytes};
use crate::contract::step::{CoreFault, Exhaustion, PlanStamp, Progress, StepOutcome, TurnGrant};
use crate::credits::{Credits, ReleaseOutcome};
use crate::leases::{LeaseRefused, LeaseTable};

/// The largest reservation of effect slots one transition makes. The
/// constructor of an owner refuses a box that is smaller (ADR 0006,
/// amendment).
pub const MAX_TRANSITION_SLOTS: usize = 2;

/// How many times the durable termination of one exchange is prepared,
/// the first one included, while the store refuses it because a
/// resolution fenced the operation before it arrived.
pub const MAX_RELEASE_PREPARATIONS: u32 = 4;

/// The latest resolutions of one operation the owner retains.
pub const RECENT_RESOLUTIONS: usize = 2;

/// The latest landmark resolutions of one operation the owner retains: those
/// whose rank among the resolutions of the operation is a power of two.
pub const LANDMARK_RESOLUTIONS: usize = 4;

/// The bits of an exchange generation that count the exchanges of one run
/// epoch. The bits above them hold the epoch: see [`Counters`].
const GENERATION_BITS: u32 = 32;

/// The bounds of an owner. They are fixed at construction.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct OwnerConfig {
    /// Persistent sessions the owner can hold.
    pub max_sessions: NonZeroU32,
    /// Route targets the owner can hold.
    pub max_routes: NonZeroU32,
    /// Inbound QoS 2 exchanges the owner can hold at a time.
    pub max_exchanges: NonZeroU32,
    /// The account charged for the exchanges this owner holds.
    pub account: AccountId,
    /// Payload bytes the exchanges of this owner may have brought in and
    /// not yet finished with.
    pub payload_bytes: PayloadBytes,
    /// The delay after which an unanswered operation is resolved (ADR
    /// 0001). The timer adapter counts it from its acceptance of the
    /// deadline.
    pub resolution_delay: LogicalDuration,
    /// Operations that ended which the owner retains, the latest ones, so
    /// that a late result contradicting one of them is a fault. A result
    /// for an operation that ended before them is no longer seen: it is
    /// absorbed.
    pub retained_operations: NonZeroU32,
    /// Connections the owner ordered closed, or saw fail, which it
    /// retains, the latest ones: what such a connection still carries is
    /// absorbed and cannot resume a session. A connection closed before
    /// them is no longer known as closed.
    pub remembered_connections: NonZeroU32,
}

/// Why an owner was not constructed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OwnerRefusal {
    /// The box cannot hold the largest reservation of a transition.
    BoxTooSmall { capacity: usize, needed: usize },
    /// The tables could not be allocated.
    AllocationFailed,
}

/// An owner that was not constructed, and the box it was given back.
#[derive(Debug)]
pub struct OwnerRefused {
    pub outbox: Outbox,
    pub reason: OwnerRefusal,
}

/// Why a box was not renewed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RenewRefused {
    /// The authority is not the one of the owner's run epoch. Nothing
    /// changed.
    ForeignAuthority,
    /// The new box could not hold the largest reservation of a transition.
    /// Nothing changed.
    BoxTooSmall { capacity: usize, needed: usize },
    /// The box could not be retired; the owner keeps it. Nothing changed.
    Retire(RetireRefusal),
    /// The box was retired and the new one was not created: the owner has
    /// no box until a renewal succeeds. Its counters and state are kept.
    Create(CreateRefused),
}

/// The state of an inbound QoS 2 exchange the owner holds. A Packet
/// Identifier with no exchange is `Absent` in the registry.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ExchangePhase {
    /// The admission was submitted; its commit is not known.
    AdmissionPending,
    /// The admission is known committed; PUBREL is awaited.
    AwaitPubrel,
    /// The durable termination was submitted; its commit is not known.
    ReleasePending,
}

/// The next value of each identity the owner issues, or `None` once the
/// last one was drawn. They belong to the owner, not to its box.
///
/// An exchange generation must differ from every generation the session
/// ever used, in this run epoch and before it, and no durable fact says
/// how far an earlier epoch went once its exchanges are terminated. The
/// generation therefore carries the epoch above `GENERATION_BITS` and a
/// count of the exchanges of this epoch below. An epoch that does not
/// fit starts with the counter exhausted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Counters {
    operation: Option<OperationSeq>,
    request: Option<RequestSeq>,
    publication: Option<PublicationSeq>,
    payload: Option<PayloadSeq>,
    effect: Option<EffectSeq>,
    deadline: Option<DeadlineToken>,
    generation: Option<ExchangeGeneration>,
}

impl Counters {
    fn new(epoch: RunEpoch) -> Self {
        let generation = (epoch.get() >> (u64::BITS - GENERATION_BITS) == 0)
            .then(|| ExchangeGeneration::new((epoch.get() << GENERATION_BITS) | 1));
        Self {
            operation: Some(OperationSeq::new(1)),
            request: Some(RequestSeq::new(1)),
            publication: Some(PublicationSeq::new(1)),
            payload: Some(PayloadSeq::new(1)),
            effect: Some(EffectSeq::new(1)),
            deadline: Some(DeadlineToken::new(1)),
            generation,
        }
    }
}

/// The generation after `current` within its run epoch.
fn next_generation(current: ExchangeGeneration) -> Result<ExchangeGeneration, Exhausted> {
    let next = current.checked_next()?;
    if next.get() >> GENERATION_BITS == current.get() >> GENERATION_BITS {
        Ok(next)
    } else {
        Err(Exhausted)
    }
}

/// The run epoch a generation was issued in.
fn generation_epoch(generation: ExchangeGeneration) -> u64 {
    generation.get() >> GENERATION_BITS
}

/// Draws the next value of a counter of the plan.
fn draw<T: Copy>(counter: &mut Option<T>, next: fn(T) -> Result<T, Exhausted>) -> Result<T, Stop> {
    let value = counter.ok_or(Stop::Exhausted(Exhaustion::IdentityCounter))?;
    *counter = next(value).ok();
    Ok(value)
}

/// Whether `value` was issued by a counter whose next value is `next`.
fn issued<T: PartialOrd>(value: T, next: Option<T>) -> bool {
    next.is_none_or(|next| value < next)
}

#[derive(Clone, PartialEq, Eq, Debug)]
struct Session {
    id: SessionId,
    client_id: SharedBytes,
    revision: Revision,
    /// The connection that is current for the session, if one is.
    connection: Option<ConnectionGeneration>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
struct Route {
    topic: SharedBytes,
    target: SessionId,
    target_revision: Revision,
    max_qos: Qos,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Limits {
    max_targets: u32,
    max_topic_bytes: u32,
    max_payload_bytes: PayloadBytes,
}

/// The effects whose results the owner applies to one operation (ADR 0001,
/// audit F3). The invariants, for the correspondence with model M1:
///
/// - I1, bound. An operation in flight, and a terminated operation the
///   owner retains, keep at most `1 + RECENT_RESOLUTIONS +
///   LANDMARK_RESOLUTIONS` effect identifiers: the `Persist` that proposed
///   it, its `RECENT_RESOLUTIONS` latest resolutions, and its
///   `LANDMARK_RESOLUTIONS` latest landmarks, the resolutions whose rank
///   among those of the operation is a power of two.
/// - I2, nothing forged. A result is applied to an operation only when its
///   `EffectId` is of the owner and of its run epoch, was issued, and is
///   one the operation retains. Another effect (a frame, a deadline, a
///   resolution of another operation, a resolution no longer retained) is
///   absorbed and changes nothing; an effect of another epoch, or never
///   issued, is a fault.
/// - I3, progress. One resolution is emitted at each deadline of an
///   unresolved operation, whatever is retained. If the answer of each
///   resolution arrives at most `k` resolutions later, `k` finite and
///   unknown, in any order, and finitely many answers are lost, the first
///   landmark `p >= k` whose answer is not lost is still retained when it
///   arrives: it arrives by rank `p + k <= 2p`, and a landmark is retained
///   up to rank `p * 2^LANDMARK_RESOLUTIONS`.
/// - I4, monotonic knowledge. A repeated result changes nothing. A result
///   that contradicts what the operation established is a fault while the
///   operation is retained; beyond the retained terminated operations it
///   is absorbed (`OwnerConfig::retained_operations`).
/// - I5, consumed. A terminated operation never goes through the
///   application path again; its late results only meet the check of I4.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Answers {
    persist: EffectId,
    recent: [Option<EffectId>; RECENT_RESOLUTIONS],
    landmarks: [Option<EffectId>; LANDMARK_RESOLUTIONS],
    /// Resolutions emitted for the operation so far.
    emitted: u64,
}

impl Answers {
    fn new(persist: EffectId) -> Self {
        Self {
            persist,
            recent: [None; RECENT_RESOLUTIONS],
            landmarks: [None; LANDMARK_RESOLUTIONS],
            emitted: 0,
        }
    }

    fn names(&self, effect: EffectId) -> bool {
        self.persist == effect
            || self.recent.contains(&Some(effect))
            || self.landmarks.contains(&Some(effect))
    }

    /// Retains the next resolution of the operation.
    fn with(mut self, resolution: EffectId) -> Self {
        self.emitted = self.emitted.saturating_add(1);
        let rank = self.emitted;
        let place = |rank: u64, len: usize| {
            usize::try_from(rank % u64::try_from(len).unwrap_or(1)).unwrap_or(0)
        };
        if let Some(slot) = self.recent.get_mut(place(rank, RECENT_RESOLUTIONS)) {
            *slot = Some(resolution);
        }
        if rank.is_power_of_two() {
            let landmark = u64::from(rank.trailing_zeros());
            if let Some(slot) = self
                .landmarks
                .get_mut(place(landmark, LANDMARK_RESOLUTIONS))
            {
                *slot = Some(resolution);
            }
        }
        self
    }
}

/// The operation an exchange waits for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct InFlight {
    operation: OperationId,
    /// `Submitted` or `Indeterminate`.
    phase: CommandPhase,
    /// The deadline armed for its result. Each resolution arms another.
    deadline: DeadlineToken,
    answers: Answers,
}

/// An operation that ended, as the owner knows it: kept so that a late
/// result that contradicts it is a fault, not a stale answer.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Finished {
    operation: OperationId,
    answers: Answers,
    /// `Consumed` or `RejectedCertain`.
    phase: CommandPhase,
    /// What its commit established, when it committed.
    facts: Option<CommitFacts>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SlotState {
    AdmissionPending(InFlight),
    AwaitPubrel,
    ReleasePending(InFlight),
}

/// One inbound QoS 2 exchange.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Slot {
    session: SessionId,
    packet_id: InboundPacketId,
    exchange: ExchangeId,
    publication: PublicationId,
    /// The lease of the exchange in the account of the owner, held until
    /// the exchange ends.
    lease: LeaseKey,
    /// The admission of the exchange once it is known committed and its
    /// PUBREC was published: a consumed command. `None` for an exchange
    /// read from durable data, whose admission belongs to an older epoch.
    admission: Option<Finished>,
    /// The durable terminations of the exchange prepared so far.
    release_preparations: u32,
    state: SlotState,
}

impl Slot {
    fn in_flight(&self) -> Option<InFlight> {
        match self.state {
            SlotState::AdmissionPending(in_flight) | SlotState::ReleasePending(in_flight) => {
                Some(in_flight)
            }
            SlotState::AwaitPubrel => None,
        }
    }
}

/// Why a plan was not computed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Stop {
    Need(CapacityNeed),
    Exhausted(Exhaustion),
    Fatal(CoreFault),
}

/// No event the core can wait for gives memory back: a failed allocation
/// stops the owner.
impl From<AllocationFailed> for Stop {
    fn from(_: AllocationFailed) -> Self {
        Self::Fatal(CoreFault::AllocationFailed)
    }
}

/// An input the adapters cannot produce in this state.
const IMPOSSIBLE: Stop = Stop::Fatal(CoreFault::ImpossibleInput);

/// A refused plan, with the work its planning spent.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Refusal {
    stop: Stop,
    work_used: WorkUnits,
}

/// The change of state of a transition. Indices name entries of the
/// tables at the revision the plan was computed against.
#[expect(
    clippy::large_enum_variant,
    reason = "a plan lives once on the stack of a step; boxing would allocate while planning"
)]
#[derive(Debug)]
enum Change {
    /// The input is absorbed: late, repeated, or answered by effects only.
    Nothing,
    /// The facts of the recovery page in the input join the state.
    Recover,
    /// The connection becomes the current one of the session.
    Bind {
        session: usize,
        connection: ConnectionGeneration,
    },
    /// The session has no current connection any more.
    Unbind { session: usize },
    /// A new exchange, its admission submitted.
    Admit {
        session: SessionId,
        packet_id: InboundPacketId,
        exchange: ExchangeId,
        publication: PublicationId,
        cost: Credits,
        in_flight: InFlight,
    },
    /// The admission of the exchange is known committed.
    Committed { slot: usize, admission: Finished },
    /// The exchange ends with the operation it waited for, and gives its
    /// lease back.
    End { slot: usize, finished: Finished },
    /// The operation in flight changes phase, and, after a resolution,
    /// deadline and the effects that answer it.
    Await {
        slot: usize,
        phase: CommandPhase,
        resolution: Option<(DeadlineToken, EffectId)>,
    },
    /// The durable termination of the exchange is submitted, after the
    /// one the store refused if there was one.
    Release {
        slot: usize,
        in_flight: InFlight,
        refused: Option<Finished>,
    },
}

/// The effects of a plan: at most `MAX_TRANSITION_SLOTS`, in the order of
/// their publication.
#[derive(Debug)]
struct Effects {
    items: [Option<OwnedEffect>; MAX_TRANSITION_SLOTS],
    count: usize,
}

/// A transition computed and not yet applied.
///
/// It is private, it cannot be copied, and `apply` takes it by value: a
/// plan is used once (ADR 0006).
#[derive(Debug)]
struct Plan {
    stamp: PlanStamp,
    work: WorkUnits,
    /// The counters after the identities of the plan were drawn.
    counters: Counters,
    change: Change,
    effects: Effects,
    progress: Progress,
    /// The connection the transition closes, or saw fail.
    closed: Option<ConnectionGeneration>,
}

/// What a plan draws and publishes while it is computed.
struct Draft {
    epoch: RunEpoch,
    owner: LogicalOwnerId,
    counters: Counters,
    effects: Effects,
    progress: Progress,
    closed: Option<ConnectionGeneration>,
}

impl Draft {
    /// Adds an effect under the next effect identity.
    fn emit(&mut self, effect: Effect) -> Result<EffectId, Stop> {
        let Some(place) = self.effects.items.get_mut(self.effects.count) else {
            return Err(Stop::Fatal(CoreFault::ReservationOverrun));
        };
        let id = EffectId {
            epoch: self.epoch,
            owner: self.owner,
            sequence: draw(&mut self.counters.effect, EffectSeq::checked_next)?,
        };
        if matches!(
            effect,
            Effect::Persist { .. } | Effect::ResolveAndFence { .. }
        ) {
            self.progress = Progress::WaitingForEffects;
        }
        *place = Some(OwnedEffect::new(id, effect));
        self.effects.count += 1;
        Ok(id)
    }

    fn frame(&mut self, connection: ConnectionKey, frame: OutboundFrame) -> Result<(), Stop> {
        self.emit(Effect::EmitFrame { connection, frame })
            .map(|_| ())
    }

    fn close(&mut self, connection: ConnectionGeneration, reason: CloseReason) -> Result<(), Stop> {
        self.end(connection)?;
        self.emit(Effect::CloseConnection { connection, reason })
            .map(|_| ())
    }

    /// Records the one connection the transition ends.
    fn end(&mut self, connection: ConnectionGeneration) -> Result<(), Stop> {
        if self.closed.replace(connection).is_some() {
            return Err(IMPOSSIBLE);
        }
        Ok(())
    }
}

/// One logical owner: its sessions, its routes, its inbound exchanges and
/// the counters of the identities it issues.
///
/// The start-up path builds an owner from the authority of its run epoch
/// and the one box it creates for that owner:
///
/// ```
/// # use core::num::{NonZeroU32, NonZeroUsize};
/// # use mqttiumd_core::contract::capacity::PayloadBytes;
/// # use mqttiumd_core::contract::command::StartupNonce;
/// # use mqttiumd_core::contract::effects::OwnerAuthority;
/// # use mqttiumd_core::contract::ids::{
/// #     AccountId, LogicalDuration, LogicalOwnerId, RunEpoch, StoreIncarnation,
/// # };
/// # use mqttiumd_core::contract::outcome::{
/// #     EpochOpened, EpochOpening, OpeningOutcome, OpeningReply,
/// # };
/// # use mqttiumd_core::owner::{Owner, OwnerConfig};
/// # let one = NonZeroU32::new(1).unwrap();
/// # let incarnation = StoreIncarnation::new(1);
/// # let mut opening = EpochOpening::new(incarnation, StartupNonce::new([1; 16]));
/// # let command = opening.submit(RunEpoch::new(1)).unwrap();
/// # let committed = EpochOpened { incarnation, epoch: RunEpoch::new(2) };
/// # let outcome = OpeningOutcome::Committed(committed);
/// # let _ = opening.observe(OpeningReply { command, outcome }).unwrap();
/// # let grant = opening.hand_over().unwrap();
/// # let config = OwnerConfig {
/// #     max_sessions: one,
/// #     max_routes: one,
/// #     max_exchanges: one,
/// #     account: AccountId::new(1),
/// #     payload_bytes: PayloadBytes::new(16),
/// #     resolution_delay: LogicalDuration::new(5),
/// #     retained_operations: one,
/// #     remembered_connections: one,
/// # };
/// let mut authority = OwnerAuthority::from_grant(grant, one).unwrap();
/// let slots = NonZeroUsize::new(8).unwrap();
/// let outbox = authority.create_outbox(LogicalOwnerId::new(4), slots, one).unwrap();
/// let owner = Owner::new(config, outbox).unwrap();
/// # let _ = owner;
/// ```
///
/// The owner takes its box: no second owner can be built from it (audit
/// F4). This does not compile:
///
/// ```compile_fail,E0382
/// # use core::num::{NonZeroU32, NonZeroUsize};
/// # use mqttiumd_core::contract::capacity::PayloadBytes;
/// # use mqttiumd_core::contract::command::StartupNonce;
/// # use mqttiumd_core::contract::effects::OwnerAuthority;
/// # use mqttiumd_core::contract::ids::{
/// #     AccountId, LogicalDuration, LogicalOwnerId, RunEpoch, StoreIncarnation,
/// # };
/// # use mqttiumd_core::contract::outcome::{
/// #     EpochOpened, EpochOpening, OpeningOutcome, OpeningReply,
/// # };
/// # use mqttiumd_core::owner::{Owner, OwnerConfig};
/// # let one = NonZeroU32::new(1).unwrap();
/// # let incarnation = StoreIncarnation::new(1);
/// # let mut opening = EpochOpening::new(incarnation, StartupNonce::new([1; 16]));
/// # let command = opening.submit(RunEpoch::new(1)).unwrap();
/// # let committed = EpochOpened { incarnation, epoch: RunEpoch::new(2) };
/// # let outcome = OpeningOutcome::Committed(committed);
/// # let _ = opening.observe(OpeningReply { command, outcome }).unwrap();
/// # let grant = opening.hand_over().unwrap();
/// # let config = OwnerConfig {
/// #     max_sessions: one,
/// #     max_routes: one,
/// #     max_exchanges: one,
/// #     account: AccountId::new(1),
/// #     payload_bytes: PayloadBytes::new(16),
/// #     resolution_delay: LogicalDuration::new(5),
/// #     retained_operations: one,
/// #     remembered_connections: one,
/// # };
/// let mut authority = OwnerAuthority::from_grant(grant, one).unwrap();
/// let slots = NonZeroUsize::new(8).unwrap();
/// let outbox = authority.create_outbox(LogicalOwnerId::new(4), slots, one).unwrap();
/// let owner = Owner::new(config, outbox).unwrap();
/// let twin = Owner::new(config, outbox).unwrap();
/// ```
#[derive(Debug)]
pub struct Owner {
    id: LogicalOwnerId,
    incarnation: StoreIncarnation,
    epoch: RunEpoch,
    /// Advanced by every applied transition.
    revision: Revision,
    counters: Counters,
    /// The order of the inventory read so far, while the owner recovers.
    recovery: Option<RecoveryOrder>,
    epoch_recovered: bool,
    limits: Option<Limits>,
    route_revision: Option<Revision>,
    routes: BoundedVec<Route>,
    sessions: BoundedVec<Session>,
    slots: BoundedVec<Slot>,
    leases: LeaseTable,
    /// The latest operations that ended; see `OwnerConfig`.
    finished: BoundedQueue<Finished>,
    /// The latest connections that were closed; see `OwnerConfig`.
    closed: BoundedQueue<ConnectionGeneration>,
    resolution_delay: LogicalDuration,
    /// The one box of this owner; `None` after a renewal whose creation
    /// failed.
    outbox: Option<Outbox>,
}

fn table<T>(bound: NonZeroU32) -> Result<BoundedVec<T>, AllocationFailed> {
    BoundedVec::with_capacity(usize::try_from(bound.get()).map_err(|_| AllocationFailed)?)
}

fn ring<T>(bound: NonZeroU32) -> Result<BoundedQueue<T>, AllocationFailed> {
    BoundedQueue::with_capacity(usize::try_from(bound.get()).map_err(|_| AllocationFailed)?)
}

/// Keeps `item` as the latest of `ring`, forgetting the oldest when the
/// ring is full. The capacity is not zero, so it never refuses.
fn remember<T>(ring: &mut BoundedQueue<T>, item: T) -> Result<(), CoreFault> {
    if ring.len() == ring.capacity() {
        let _ = ring.pop_front();
    }
    ring.try_push_back(item)
        .map_err(|_| CoreFault::ReservationOverrun)
}

fn capacity_of(bound: usize) -> u64 {
    u64::try_from(bound).unwrap_or(u64::MAX)
}

/// The facts of one kind read so far on a page, before `upto`. The order
/// of an inventory puts the facts of one kind together: they start where
/// the first of them was seen and end where a later kind starts.
fn run<'a>(
    facts: &'a [RecoveredFact],
    starts: &[Option<usize>; 7],
    rank: FactRank,
    upto: usize,
) -> &'a [RecoveredFact] {
    let index = rank as usize;
    let Some(start) = starts.get(index).copied().flatten() else {
        return &[];
    };
    let end = starts
        .iter()
        .skip(index + 1)
        .find_map(|later| *later)
        .unwrap_or(upto)
        .min(upto);
    facts.get(start..end).unwrap_or(&[])
}

impl Owner {
    /// An owner that recovers: it accepts recovery pages and nothing else
    /// until the page that closes the inventory.
    ///
    /// The owner, the data set and the run epoch are those of `outbox`,
    /// which only a creation authority builds (ADR 0009). The owner takes
    /// the box and keeps it; a refusal gives it back.
    #[expect(
        clippy::result_large_err,
        reason = "a refusal hands the box back by value, as the retirement of a box does"
    )]
    pub fn new(config: OwnerConfig, outbox: Outbox) -> Result<Self, OwnerRefused> {
        if outbox.capacity() < MAX_TRANSITION_SLOTS {
            let reason = OwnerRefusal::BoxTooSmall {
                capacity: outbox.capacity(),
                needed: MAX_TRANSITION_SLOTS,
            };
            return Err(OwnerRefused { outbox, reason });
        }
        let id = outbox.id();
        let credit = Credits::new(
            config.payload_bytes,
            ExchangeSlots::new(u64::from(config.max_exchanges.get())),
        );
        let tables = (|| {
            Ok::<_, AllocationFailed>((
                table(config.max_routes)?,
                table(config.max_sessions)?,
                table(config.max_exchanges)?,
                LeaseTable::new(config.account, credit, config.max_exchanges)?,
                ring(config.retained_operations)?,
                ring(config.remembered_connections)?,
            ))
        })();
        let Ok((routes, sessions, slots, leases, finished, closed)) = tables else {
            let reason = OwnerRefusal::AllocationFailed;
            return Err(OwnerRefused { outbox, reason });
        };
        Ok(Self {
            id: id.owner(),
            incarnation: id.incarnation(),
            epoch: id.epoch(),
            revision: Revision::new(0),
            counters: Counters::new(id.epoch()),
            recovery: Some(RecoveryOrder::new()),
            epoch_recovered: false,
            limits: None,
            route_revision: None,
            routes,
            sessions,
            slots,
            leases,
            finished,
            closed,
            resolution_delay: config.resolution_delay,
            outbox: Some(outbox),
        })
    }

    /// The box of the owner, if it has one.
    #[must_use]
    pub fn outbox(&self) -> Option<&Outbox> {
        self.outbox.as_ref()
    }

    /// Hands the effects of the box to `sink`, in order (an owner without
    /// a box has none).
    pub fn dispatch(&mut self, sink: &mut dyn EffectSink) -> DispatchReport {
        match self.outbox.as_mut() {
            Some(outbox) => outbox.dispatch(sink),
            None => DispatchReport {
                accepted: 0,
                blocked: None,
            },
        }
    }

    /// Replaces the box of the owner: retires it under `authority`, then
    /// creates one for the same identity (ADR 0009). The counters and the
    /// state of the owner are kept. The retirement is refused while an
    /// effect waits or a reservation lives, and the owner keeps its box.
    pub fn renew_box(
        &mut self,
        authority: &mut OwnerAuthority,
        capacity: NonZeroUsize,
        max_reservations: NonZeroU32,
    ) -> Result<(), RenewRefused> {
        if authority.incarnation() != self.incarnation || authority.epoch() != self.epoch {
            return Err(RenewRefused::ForeignAuthority);
        }
        if capacity.get() < MAX_TRANSITION_SLOTS {
            return Err(RenewRefused::BoxTooSmall {
                capacity: capacity.get(),
                needed: MAX_TRANSITION_SLOTS,
            });
        }
        if let Some(outbox) = self.outbox.take()
            && let Err(refused) = authority.retire(outbox)
        {
            let reason = refused.reason;
            self.outbox = Some(refused.outbox);
            return Err(RenewRefused::Retire(reason));
        }
        let outbox = authority
            .create_outbox(self.id, capacity, max_reservations)
            .map_err(RenewRefused::Create)?;
        self.outbox = Some(outbox);
        Ok(())
    }

    /// The owner.
    #[must_use]
    pub fn id(&self) -> LogicalOwnerId {
        self.id
    }

    /// The revision of the state: the number of transitions applied.
    #[must_use]
    pub fn revision(&self) -> Revision {
        self.revision
    }

    /// Whether the inventory of the recovery is still open.
    #[must_use]
    pub fn is_recovering(&self) -> bool {
        self.recovery.is_some()
    }

    /// The current connection of a session, if the owner holds the
    /// session and it has one.
    #[must_use]
    pub fn connection_of(&self, session: SessionId) -> Option<ConnectionGeneration> {
        self.sessions
            .as_slice()
            .iter()
            .find(|held| held.id == session)
            .and_then(|held| held.connection)
    }

    /// The exchange the session holds under this Packet Identifier, and
    /// its state; `None` is the `Absent` of the registry.
    #[must_use]
    pub fn exchange(
        &self,
        session: SessionId,
        packet_id: InboundPacketId,
    ) -> Option<(ExchangeId, ExchangePhase)> {
        let slot = self
            .slots
            .as_slice()
            .get(self.slot_of(session, packet_id)?)?;
        let phase = match slot.state {
            SlotState::AdmissionPending(_) => ExchangePhase::AdmissionPending,
            SlotState::AwaitPubrel => ExchangePhase::AwaitPubrel,
            SlotState::ReleasePending(_) => ExchangePhase::ReleasePending,
        };
        Some((slot.exchange, phase))
    }

    /// Applies `input` entirely, or hands it back intact (DEM-006).
    ///
    /// A refusal keeps the input, the state, the identity counters, the
    /// account, the reservations held and the content of the box as they
    /// were. An owner without a box, after a renewal that could not create
    /// one, hands every input back as `Exhausted(ReservationTable)` until
    /// a renewal succeeds.
    pub fn step(&mut self, input: Input, grant: TurnGrant) -> StepOutcome {
        let Some(mut outbox) = self.outbox.take() else {
            return StepOutcome::Exhausted {
                input,
                exhaustion: Exhaustion::ReservationTable,
                work_used: WorkUnits::ZERO,
            };
        };
        let outcome = self.step_into(input, grant, &mut outbox);
        self.outbox = Some(outbox);
        outcome
    }

    fn step_into(&mut self, input: Input, grant: TurnGrant, outbox: &mut Outbox) -> StepOutcome {
        let plan = match self.plan(&input, grant) {
            Ok(plan) => plan,
            Err(Refusal { stop, work_used }) => {
                return match stop {
                    Stop::Need(need) => StepOutcome::NeedCapacity {
                        input,
                        need,
                        work_used,
                    },
                    Stop::Exhausted(exhaustion) => StepOutcome::Exhausted {
                        input,
                        exhaustion,
                        work_used,
                    },
                    Stop::Fatal(fault) => StepOutcome::Fatal(fault),
                };
            }
        };
        let work_used = plan.work;
        // The last acquisition, and the only one a plan cannot check: one
        // reservation for all the slots of the plan.
        let reservation = match NonZeroUsize::new(plan.effects.count) {
            None => None,
            Some(slots) => match outbox.reserve(slots) {
                Ok(key) => Some(key),
                Err(refusal) => return StepOutcome::reservation_refused(input, refusal, work_used),
            },
        };
        match self.apply(plan, input, reservation, outbox) {
            Ok(progress) => StepOutcome::Applied {
                work_used,
                progress,
            },
            Err(fault) => StepOutcome::Fatal(fault),
        }
    }

    /// The work an input costs: one unit, and for a recovery page a bound
    /// of the comparisons its checks make. A fact is compared with the
    /// table it joins: a route with the routes, a session with the
    /// sessions, an exchange with the sessions and the exchanges. The page
    /// that closes the inventory compares every route with every session.
    fn cost(&self, input: &Input) -> WorkUnits {
        let Input::Recovery { facts, last } = input else {
            return WorkUnits::new(1);
        };
        let routes = capacity_of(self.routes.capacity());
        let sessions = capacity_of(self.sessions.capacity());
        let slots = capacity_of(self.slots.capacity());
        let mut work: u64 = 0;
        for fact in facts.as_slice() {
            let compared = match fact.rank() {
                FactRank::Route => routes,
                FactRank::Session => sessions,
                FactRank::InboundExchange => sessions.saturating_add(slots),
                FactRank::Epoch | FactRank::Limits | FactRank::RouteTable | FactRank::Operation => {
                    0
                }
            };
            work = work.saturating_add(compared.saturating_add(1));
        }
        if *last {
            work = work.saturating_add(routes.saturating_mul(sessions).saturating_add(1));
        }
        WorkUnits::new(work)
    }

    /// Computes the transition of `input` and mutates nothing.
    fn plan(&self, input: &Input, grant: TurnGrant) -> Result<Plan, Refusal> {
        let work = self.cost(input);
        if let Some(missing) = work.checked_sub(grant.work_limit)
            && missing > WorkUnits::ZERO
        {
            // Refused before anything was read: no work was spent.
            return Err(Refusal {
                stop: Stop::Need(CapacityNeed {
                    resource: Resource::Work,
                    amount: missing.get(),
                    wake: WakeCondition::TurnGranted,
                }),
                work_used: WorkUnits::ZERO,
            });
        }
        let mut draft = Draft {
            epoch: self.epoch,
            owner: self.id,
            counters: self.counters,
            effects: Effects {
                items: [None, None],
                count: 0,
            },
            progress: Progress::Complete,
            closed: None,
        };
        let planned = self
            .revision
            .checked_next()
            .map_or(Err(Stop::Exhausted(Exhaustion::IdentityCounter)), |_| {
                self.plan_change(input, &mut draft)
            });
        match planned {
            Ok(change) => Ok(Plan {
                stamp: PlanStamp {
                    owner: self.id,
                    state_revision: self.revision,
                },
                work,
                counters: draft.counters,
                change,
                effects: draft.effects,
                progress: draft.progress,
                closed: draft.closed,
            }),
            Err(stop) => Err(Refusal {
                stop,
                work_used: work,
            }),
        }
    }

    fn plan_change(&self, input: &Input, draft: &mut Draft) -> Result<Change, Stop> {
        if self.recovery.is_some() {
            return match input {
                Input::Recovery { facts, last } => self.plan_recovery(facts.as_slice(), *last),
                _ => Err(IMPOSSIBLE),
            };
        }
        match input {
            Input::Recovery { .. } | Input::Continue { .. } => Err(IMPOSSIBLE),
            // A wake-up carries no authority, and this perimeter keeps no
            // work behind one.
            Input::WorkAvailable { .. } => Ok(Change::Nothing),
            Input::Packet {
                connection, packet, ..
            } => self.plan_packet(*connection, packet, draft),
            Input::StoreResult {
                effect,
                operation,
                reply,
            } => self.plan_result(*effect, *operation, reply, draft),
            Input::Deadline { token, .. } => self.plan_deadline(*token, draft),
            Input::FrameResult {
                connection,
                progress,
                ..
            } => self.plan_frame_result(*connection, *progress, draft),
        }
    }

    fn session_of(&self, connection: ConnectionGeneration) -> Option<usize> {
        self.sessions
            .as_slice()
            .iter()
            .position(|session| session.connection == Some(connection))
    }

    fn slot_of(&self, session: SessionId, packet_id: InboundPacketId) -> Option<usize> {
        self.slots
            .as_slice()
            .iter()
            .position(|slot| slot.session == session && slot.packet_id == packet_id)
    }

    /// Closes the connection of a packet the subset does not accept. The
    /// session, if the connection had one, has no current connection
    /// any more.
    fn plan_close(
        connection: ConnectionGeneration,
        session: Option<usize>,
        reason: CloseReason,
        draft: &mut Draft,
    ) -> Result<Change, Stop> {
        draft.close(connection, reason)?;
        Ok(session.map_or(Change::Nothing, |session| Change::Unbind { session }))
    }

    fn plan_packet(
        &self,
        connection: ConnectionGeneration,
        packet: &InboundPacket,
        draft: &mut Draft,
    ) -> Result<Change, Stop> {
        // The owner ordered this connection closed, or saw it fail: what
        // it still carries changes nothing and is not answered.
        if self.closed.iter().any(|closed| *closed == connection) {
            return Ok(Change::Nothing);
        }
        let bound = self.session_of(connection);
        let protocol_error = |draft: &mut Draft| {
            Self::plan_close(connection, bound, CloseReason::ProtocolError, draft)
        };
        if let InboundPacket::ConnectResume { client_id } = packet {
            if bound.is_some() {
                // A second CONNECT on one connection.
                return protocol_error(draft);
            }
            let known = self
                .sessions
                .as_slice()
                .iter()
                .position(|session| session.client_id == *client_id);
            let Some(index) = known else {
                // The subset resumes the sessions of the fixture only.
                return protocol_error(draft);
            };
            let session = &self.sessions.as_slice()[index];
            if let Some(previous) = session.connection {
                draft.close(previous, CloseReason::SessionTakenOver)?;
            }
            let key = ConnectionKey {
                session: session.id,
                generation: connection,
            };
            draft.frame(
                key,
                OutboundFrame::Connack {
                    session_present: true,
                },
            )?;
            return Ok(Change::Bind {
                session: index,
                connection,
            });
        }
        let Some(index) = bound else {
            // A packet before CONNECT, or on a connection that was replaced.
            return protocol_error(draft);
        };
        let session = &self.sessions.as_slice()[index];
        let key = ConnectionKey {
            session: session.id,
            generation: connection,
        };
        match packet {
            InboundPacket::ConnectResume { .. } => Err(IMPOSSIBLE),
            InboundPacket::Publish {
                topic,
                qos,
                retain,
                packet_id,
                payload,
                ..
            } => {
                if *qos != Qos::ExactlyOnce || *retain {
                    return protocol_error(draft);
                }
                let Some(packet_id) = *packet_id else {
                    return Err(IMPOSSIBLE);
                };
                let Some(slot) = self.slot_of(session.id, packet_id) else {
                    return self
                        .plan_admission(index, connection, packet_id, topic, payload, draft);
                };
                match self.slots.as_slice()[slot].state {
                    // The exchange owns its admission: the replay creates
                    // no command and no acknowledgement (ADR 0001).
                    SlotState::AdmissionPending(_) => Ok(Change::Nothing),
                    SlotState::AwaitPubrel => {
                        let frame = OutboundFrame::Pubrec {
                            packet_id,
                            reason: AckReason::Success,
                        };
                        draft.frame(key, frame)?;
                        Ok(Change::Nothing)
                    }
                    // After its PUBREL a client may only resend PUBREL.
                    SlotState::ReleasePending(_) => protocol_error(draft),
                }
            }
            InboundPacket::Pubrel { packet_id } => {
                let Some(slot) = self.slot_of(session.id, *packet_id) else {
                    let frame = OutboundFrame::Pubcomp {
                        packet_id: *packet_id,
                        reason: AckReason::PacketIdentifierNotFound,
                    };
                    draft.frame(key, frame)?;
                    return Ok(Change::Nothing);
                };
                match self.slots.as_slice()[slot].state {
                    // No PUBREC of this exchange was ever made eligible.
                    SlotState::AdmissionPending(_) => protocol_error(draft),
                    SlotState::AwaitPubrel => self.plan_release(slot, None, draft),
                    // No second release, and no PUBCOMP before the commit.
                    SlotState::ReleasePending(_) => Ok(Change::Nothing),
                }
            }
        }
    }

    /// Submits a frozen command and arms the deadline of its result, in
    /// that order: the box dispatches in order, so the delay starts only
    /// once the store adapter accepted the command (ADR 0001).
    fn plan_submission(
        &self,
        operation: OperationId,
        recipe: Recipe,
        payload: Option<SharedBytes>,
        draft: &mut Draft,
    ) -> Result<InFlight, Stop> {
        let Verdict::Transition(phase) = CommandPhase::Prepared.on(CommandEvent::Submit) else {
            return Err(IMPOSSIBLE);
        };
        let recipe = FrozenRecipe::freeze(recipe).map_err(|error| match error {
            RecipeError::AllocationFailed => Stop::from(AllocationFailed),
            _ => IMPOSSIBLE,
        })?;
        let persist = draft.emit(Effect::Persist {
            command: Command { recipe, payload },
        })?;
        let deadline = draw(&mut draft.counters.deadline, DeadlineToken::checked_next)?;
        draft.emit(Effect::ArmDeadline {
            token: deadline,
            after: self.resolution_delay,
        })?;
        Ok(InFlight {
            operation,
            phase,
            deadline,
            answers: Answers::new(persist),
        })
    }

    fn operation(&self, draft: &mut Draft) -> Result<OperationId, Stop> {
        let sequence = draw(&mut draft.counters.operation, OperationSeq::checked_next)?;
        Ok(OperationId::new(
            self.incarnation,
            self.epoch,
            self.id,
            sequence,
        ))
    }

    /// A new PUBLISH: identities, exact-topic routing from the recovered
    /// routes, a frozen recipe, `Persist`.
    fn plan_admission(
        &self,
        session: usize,
        connection: ConnectionGeneration,
        packet_id: InboundPacketId,
        topic: &SharedBytes,
        payload: &SharedBytes,
        draft: &mut Draft,
    ) -> Result<Change, Stop> {
        let (Some(limits), Some(route_revision)) = (self.limits, self.route_revision) else {
            return Err(IMPOSSIBLE);
        };
        let never = |draft: &mut Draft| {
            Self::plan_close(
                connection,
                Some(session),
                CloseReason::CapacityExceeded,
                draft,
            )
        };
        let source = &self.sessions.as_slice()[session];
        let length = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        let fits = |len: usize, max: u32| u32::try_from(len).is_ok_and(|len| len <= max);
        let routed = || {
            self.routes
                .as_slice()
                .iter()
                .filter(|route| route.topic == *topic)
        };
        let targets = routed().count();
        if !fits(topic.len(), limits.max_topic_bytes)
            || length > limits.max_payload_bytes.get()
            || !fits(targets, limits.max_targets)
        {
            return never(draft);
        }
        let cost = Credits::new(PayloadBytes::new(length), ExchangeSlots::new(1));
        let lacking = |resource, amount| {
            Stop::Need(CapacityNeed {
                resource,
                amount,
                wake: WakeCondition::ResourceReleased,
            })
        };
        // The lease table has one slot per entry of the exchange table and
        // every exchange holds a lease: an account that grants the lease
        // leaves room for the exchange.
        match self.leases.check(cost) {
            Ok(()) => {}
            Err(LeaseRefused::LargerThanAccount(_)) => return never(draft),
            Err(LeaseRefused::TableExhausted) => {
                return Err(Stop::Exhausted(Exhaustion::IdentityCounter));
            }
            Err(LeaseRefused::Limit { resource, missing }) => {
                return Err(lacking(resource, missing));
            }
            Err(LeaseRefused::TableFull) => return Err(lacking(Resource::ExchangeSlots, 1)),
        }
        let references = u64::try_from(targets).unwrap_or(u64::MAX);
        let mut list = BoundedVec::with_capacity(targets).map_err(Stop::from)?;

        let generation = draw(&mut draft.counters.generation, next_generation)?;
        let exchange = ExchangeId {
            session: source.id,
            generation,
        };
        let operation = self.operation(draft)?;
        let (incarnation, epoch, owner) = (self.incarnation, self.epoch, self.id);
        let request = RequestId::new(
            incarnation,
            epoch,
            owner,
            draw(&mut draft.counters.request, RequestSeq::checked_next)?,
        );
        let publication = PublicationId::new(
            incarnation,
            epoch,
            owner,
            draw(
                &mut draft.counters.publication,
                PublicationSeq::checked_next,
            )?,
        );
        let payload_id = PayloadId::new(
            incarnation,
            epoch,
            owner,
            draw(&mut draft.counters.payload, PayloadSeq::checked_next)?,
        );
        for (index, route) in routed().enumerate() {
            let target = Target {
                session: route.target,
                session_revision: route.target_revision,
                delivery: DeliveryId {
                    publication,
                    index: u32::try_from(index).map_err(|_| IMPOSSIBLE)?,
                },
                effective_qos: route.max_qos.min(Qos::ExactlyOnce),
                cause: DeliveryCause::LiveExact,
            };
            list.try_push(target).map_err(|_| IMPOSSIBLE)?;
        }
        let recipe = Recipe::AdmitPublication(AdmitPublication {
            operation,
            request,
            publication,
            source: Source {
                session: source.id,
                session_revision: source.revision,
                packet_id,
                exchange,
            },
            route_revision,
            qos: Qos::ExactlyOnce,
            retain: false,
            topic: topic.clone(),
            payload: PayloadRef {
                id: payload_id,
                length: PayloadBytes::new(length),
            },
            costs: Costs {
                payload_bytes: PayloadBytes::new(length),
                references: References::new(references),
                exchange_slots: ExchangeSlots::new(1),
                receipt_slots: ReceiptSlots::new(1),
            },
            targets: list,
        });
        let in_flight = self.plan_submission(operation, recipe, Some(payload.clone()), draft)?;
        Ok(Change::Admit {
            session: source.id,
            packet_id,
            exchange,
            publication,
            cost,
            in_flight,
        })
    }

    /// PUBREL, or a release the store certainly refused: the durable
    /// termination is submitted under a new operation.
    fn plan_release(
        &self,
        slot: usize,
        refused: Option<Finished>,
        draft: &mut Draft,
    ) -> Result<Change, Stop> {
        let held = &self.slots.as_slice()[slot];
        let operation = self.operation(draft)?;
        let recipe = Recipe::ReleaseInbound(ReleaseInbound {
            operation,
            source_session: held.session,
            packet_id: held.packet_id,
            exchange: held.exchange,
        });
        let in_flight = self.plan_submission(operation, recipe, None, draft)?;
        Ok(Change::Release {
            slot,
            in_flight,
            refused,
        })
    }

    /// The frame that answers an exchange goes to the connection that is
    /// current when its operation completes; without one nothing is
    /// emitted, and the commit is reconciled all the same (ADR 0001).
    fn plan_answer(
        &self,
        slot: &Slot,
        frame: OutboundFrame,
        draft: &mut Draft,
    ) -> Result<(), Stop> {
        match self.connection_of(slot.session) {
            None => Ok(()),
            Some(generation) => draft.frame(
                ConnectionKey {
                    session: slot.session,
                    generation,
                },
                frame,
            ),
        }
    }

    fn plan_result(
        &self,
        effect: EffectId,
        operation: OperationId,
        reply: &StoreReply,
        draft: &mut Draft,
    ) -> Result<Change, Stop> {
        // A result answers an effect this owner emitted in this run epoch.
        if effect.epoch != self.epoch
            || effect.owner != self.id
            || !issued(effect.sequence, self.counters.effect)
        {
            return Err(IMPOSSIBLE);
        }
        let contradiction = Stop::Fatal(CoreFault::ContradictoryOutcome { operation });
        let slots = self.slots.as_slice();
        let waiting = slots.iter().position(|slot| {
            slot.in_flight()
                .is_some_and(|held| held.operation == operation)
        });
        let Some(index) = waiting else {
            return self.plan_late_result(effect, operation, reply);
        };
        let slot = &slots[index];
        let Some(in_flight) = slot.in_flight() else {
            return Err(IMPOSSIBLE);
        };
        // Only the effects emitted for this operation answer it. The answer
        // to a resolution the owner no longer retains is absorbed: the next
        // resolution answers again (ADR 0001).
        if !in_flight.answers.names(effect) {
            return Ok(Change::Nothing);
        }
        let outcome = match reply {
            StoreReply::Violation(StoreViolation::RecipeMismatch) => {
                return Err(Stop::Fatal(CoreFault::RecipeMismatch { operation }));
            }
            StoreReply::Outcome(outcome) => *outcome,
        };
        let admission = matches!(slot.state, SlotState::AdmissionPending(_));
        let verdict = in_flight
            .phase
            .on(CommandEvent::StoreResult(OutcomeClass::of(&outcome)));
        let finished = |phase, facts| Finished {
            operation,
            answers: in_flight.answers,
            phase,
            facts,
        };
        match (verdict, outcome) {
            (Verdict::Transition(CommandPhase::Committed), StoreOutcome::Committed(facts)) => {
                let expected = if admission {
                    CommitFacts::PublicationAdmitted {
                        publication: slot.publication,
                        exchange: slot.exchange,
                    }
                } else {
                    CommitFacts::InboundReleased {
                        exchange: slot.exchange,
                    }
                };
                if facts != expected {
                    return Err(contradiction);
                }
                let packet_id = slot.packet_id;
                let reason = AckReason::Success;
                // The owner publishes what depends on the commit in this
                // transition: the command is consumed.
                let finished = finished(CommandPhase::Consumed, Some(facts));
                if admission {
                    // PUBREC becomes eligible here and nowhere earlier.
                    self.plan_answer(slot, OutboundFrame::Pubrec { packet_id, reason }, draft)?;
                    Ok(Change::Committed {
                        slot: index,
                        admission: finished,
                    })
                } else {
                    self.plan_answer(slot, OutboundFrame::Pubcomp { packet_id, reason }, draft)?;
                    Ok(Change::End {
                        slot: index,
                        finished,
                    })
                }
            }
            (
                Verdict::Transition(CommandPhase::RejectedCertain),
                StoreOutcome::RejectedCertain(rejection),
            ) => {
                let finished = finished(CommandPhase::RejectedCertain, None);
                if admission {
                    // Nothing was admitted: the exchange ends and the
                    // client is told, as the protocol says.
                    let frame = OutboundFrame::Pubrec {
                        packet_id: slot.packet_id,
                        reason: AckReason::refusing(rejection),
                    };
                    self.plan_answer(slot, frame, draft)?;
                    return Ok(Change::End {
                        slot: index,
                        finished,
                    });
                }
                // The exchange is still durably open and the client will
                // not send PUBREL again on a live connection. Only a fence
                // that a resolution of this owner set before the command
                // arrived is ended by preparing again, and only so many
                // times. Any other refusal would come back for the same
                // reason: the owner stops rather than prepare it again.
                match rejection {
                    RejectReason::Fenced
                        if slot.release_preparations < MAX_RELEASE_PREPARATIONS =>
                    {
                        self.plan_release(index, Some(finished), draft)
                    }
                    reason => Err(Stop::Fatal(CoreFault::ReleaseRefused { operation, reason })),
                }
            }
            (Verdict::Transition(CommandPhase::Indeterminate), _) => Ok(Change::Await {
                slot: index,
                phase: CommandPhase::Indeterminate,
                resolution: None,
            }),
            // S0 retires no identity space: the store cannot report that
            // the detail of a result the exchange waits for is gone.
            _ => Err(IMPOSSIBLE),
        }
    }

    /// A result for an operation no exchange waits for. When the owner
    /// retains the operation, the result must agree with what it knows;
    /// otherwise it ended before the retained ones, the owner no longer
    /// sees what it established, and the result is absorbed.
    fn plan_late_result(
        &self,
        effect: EffectId,
        operation: OperationId,
        reply: &StoreReply,
    ) -> Result<Change, Stop> {
        let known = self
            .slots
            .as_slice()
            .iter()
            .filter_map(|slot| slot.admission)
            .chain(self.finished.iter().copied())
            .find(|finished| finished.operation == operation);
        let Some(known) = known else {
            return Ok(Change::Nothing);
        };
        if !known.answers.names(effect) {
            return Ok(Change::Nothing);
        }
        let outcome = match reply {
            StoreReply::Violation(StoreViolation::RecipeMismatch) => {
                return Err(Stop::Fatal(CoreFault::RecipeMismatch { operation }));
            }
            StoreReply::Outcome(outcome) => *outcome,
        };
        let contradiction = Stop::Fatal(CoreFault::ContradictoryOutcome { operation });
        let event = CommandEvent::StoreResult(OutcomeClass::of(&outcome));
        if known.phase.on(event) == Verdict::Fault {
            return Err(contradiction);
        }
        if let (StoreOutcome::Committed(facts), Some(established)) = (outcome, known.facts)
            && facts != established
        {
            return Err(contradiction);
        }
        Ok(Change::Nothing)
    }

    fn plan_deadline(&self, token: DeadlineToken, draft: &mut Draft) -> Result<Change, Stop> {
        if !issued(token, self.counters.deadline) {
            return Err(IMPOSSIBLE);
        }
        let waiting = self
            .slots
            .as_slice()
            .iter()
            .enumerate()
            .find_map(|(index, slot)| {
                let held = slot.in_flight()?;
                (held.deadline == token).then_some((index, held))
            });
        // A deadline of an operation that is resolved, or that a later
        // deadline replaced, is stale.
        let Some((slot, in_flight)) = waiting else {
            return Ok(Change::Nothing);
        };
        let Verdict::Transition(phase) = in_flight.phase.on(CommandEvent::ResultTimeout) else {
            return Err(IMPOSSIBLE);
        };
        // Each resolution has its own effect identity and arms the next
        // deadline, so that a resolution whose answer is held or lost is
        // followed by another.
        let resolution = draft.emit(Effect::ResolveAndFence {
            operation: in_flight.operation,
        })?;
        let deadline = draw(&mut draft.counters.deadline, DeadlineToken::checked_next)?;
        draft.emit(Effect::ArmDeadline {
            token: deadline,
            after: self.resolution_delay,
        })?;
        Ok(Change::Await {
            slot,
            phase,
            resolution: Some((deadline, resolution)),
        })
    }

    fn plan_frame_result(
        &self,
        connection: ConnectionKey,
        progress: FrameProgress,
        draft: &mut Draft,
    ) -> Result<Change, Stop> {
        let failed = matches!(progress, FrameProgress::Failed);
        match self.session_of(connection.generation) {
            Some(session)
                if failed && self.sessions.as_slice()[session].id == connection.session =>
            {
                draft.end(connection.generation)?;
                Ok(Change::Unbind { session })
            }
            _ => Ok(Change::Nothing),
        }
    }

    /// Checks a recovery page against the state and against itself. The
    /// application then cannot be refused.
    ///
    /// Each check compares a fact with the table it joins, held and read
    /// so far, and the size of that table is checked first: the work of a
    /// page is bounded by `Owner::cost`.
    fn plan_recovery(&self, facts: &[RecoveredFact], last: bool) -> Result<Change, Stop> {
        let Some(mut order) = self.recovery.clone() else {
            return Err(IMPOSSIBLE);
        };
        let mut epoch_recovered = self.epoch_recovered;
        let mut limits = self.limits.is_some();
        let mut route_table = self.route_revision.is_some();
        let mut starts = [None; 7];
        let session_named = |sessions: &[RecoveredFact], id: SessionId| {
            self.sessions.as_slice().iter().any(|held| held.id == id)
                || sessions.iter().any(
                    |fact| matches!(fact, RecoveredFact::Session { session, .. } if *session == id),
                )
        };
        for (position, fact) in facts.iter().enumerate() {
            order.admit(fact).map_err(|_| IMPOSSIBLE)?;
            let rank = fact.rank();
            if let Some(start) = starts.get_mut(rank as usize)
                && start.is_none()
            {
                *start = Some(position);
            }
            // The earlier facts of this kind on this page.
            let same = run(facts, &starts, rank, position);
            let valid = match fact {
                RecoveredFact::Epoch { incarnation, epoch } => {
                    epoch_recovered = true;
                    *incarnation == self.incarnation && *epoch == self.epoch
                }
                RecoveredFact::Limits { .. } => {
                    limits = true;
                    true
                }
                RecoveredFact::RouteTable { .. } => {
                    route_table = true;
                    true
                }
                RecoveredFact::Route { topic, target, .. } => {
                    // A route twice would freeze a recipe with the same
                    // target twice: one delivery would be owed twice.
                    route_table
                        && self.routes.len() + same.len() < self.routes.capacity()
                        && !self
                            .routes
                            .as_slice()
                            .iter()
                            .any(|held| held.topic == *topic && held.target == *target)
                        && !same.iter().any(|other| {
                            matches!(other, RecoveredFact::Route { topic: name, target: to, .. }
                                if name == topic && to == target)
                        })
                }
                RecoveredFact::Session {
                    session, client_id, ..
                } => self.sessions.len() + same.len() < self.sessions.capacity()
                    && !self
                        .sessions
                        .as_slice()
                        .iter()
                        .any(|held| held.id == *session || held.client_id == *client_id)
                    && !same.iter().any(|other| {
                        matches!(other, RecoveredFact::Session { session: id, client_id: name, .. }
                                if id == session || name == client_id)
                    }),
                RecoveredFact::InboundExchange {
                    exchange,
                    packet_id,
                    ..
                } => {
                    let sessions = run(facts, &starts, FactRank::Session, position);
                    // A generation of this epoch would meet one the owner
                    // is about to issue.
                    let older = generation_epoch(exchange.generation) < self.epoch.get();
                    self.slots.len() + same.len() < self.slots.capacity()
                        && session_named(sessions, exchange.session)
                        && !self.slots.as_slice().iter().any(|held| {
                            held.exchange == *exchange
                                || (held.session == exchange.session && held.packet_id == *packet_id)
                        })
                        && !same.iter().any(|other| {
                            matches!(other, RecoveredFact::InboundExchange { exchange: id, packet_id: packet, .. }
                                if id == exchange
                                    || (id.session == exchange.session && packet == packet_id))
                        })
                        && older
                }
                RecoveredFact::Operation { operation, .. } => {
                    operation.incarnation() == self.incarnation && operation.epoch() < self.epoch
                }
            };
            if !valid {
                return Err(IMPOSSIBLE);
            }
        }
        if last {
            // An inventory is closed only when it named the run epoch, the
            // limits and the route table: an admission needs all three.
            if !(epoch_recovered && limits && route_table) {
                return Err(IMPOSSIBLE);
            }
            // Every route leads to a session of the inventory.
            let routes = run(facts, &starts, FactRank::Route, facts.len());
            let sessions = run(facts, &starts, FactRank::Session, facts.len());
            let targets = self
                .routes
                .as_slice()
                .iter()
                .map(|route| route.target)
                .chain(routes.iter().filter_map(|fact| match fact {
                    RecoveredFact::Route { target, .. } => Some(*target),
                    _ => None,
                }));
            for target in targets {
                if !session_named(sessions, target) {
                    return Err(IMPOSSIBLE);
                }
            }
        }
        Ok(Change::Recover)
    }

    /// Applies a plan. It fails only on a plan that is not of this owner
    /// at this revision, or on a reservation that is not the one of the
    /// plan: both are faults of the core.
    fn apply(
        &mut self,
        plan: Plan,
        input: Input,
        reservation: Option<EffectReservation>,
        outbox: &mut Outbox,
    ) -> Result<Progress, CoreFault> {
        plan.stamp.check(self.id, self.revision)?;
        let Plan {
            counters,
            change,
            effects,
            progress,
            closed,
            ..
        } = plan;
        let reserved = match reservation {
            Some(key) => outbox
                .remaining(key)
                .map_err(|_| CoreFault::ReservationOverrun)?,
            None => 0,
        };
        if reserved != effects.count {
            return Err(CoreFault::ReservationOverrun);
        }
        let revision = self
            .revision
            .checked_next()
            .map_err(|_| CoreFault::StalePlan)?;

        self.apply_change(change, input)?;
        if let Some(connection) = closed {
            remember(&mut self.closed, connection)?;
        }
        self.counters = counters;
        self.revision = revision;

        if let Some(key) = reservation {
            for effect in effects.items.into_iter().flatten() {
                outbox
                    .publish(key, effect)
                    .map_err(|_| CoreFault::ReservationOverrun)?;
            }
        }
        Ok(progress)
    }

    fn apply_change(&mut self, change: Change, input: Input) -> Result<(), CoreFault> {
        const STALE: CoreFault = CoreFault::StalePlan;
        match change {
            Change::Nothing => {}
            Change::Recover => {
                let Input::Recovery { facts, last } = input else {
                    return Err(STALE);
                };
                for fact in facts.as_slice() {
                    self.recover(fact)?;
                }
                if last {
                    self.recovery = None;
                }
            }
            Change::Bind {
                session,
                connection,
            } => {
                let held = self.sessions.as_mut_slice().get_mut(session).ok_or(STALE)?;
                held.connection = Some(connection);
            }
            Change::Unbind { session } => {
                let held = self.sessions.as_mut_slice().get_mut(session).ok_or(STALE)?;
                held.connection = None;
            }
            Change::Admit {
                session,
                packet_id,
                exchange,
                publication,
                cost,
                in_flight,
            } => {
                // The plan checked the account and the table.
                let lease = self.leases.reserve(cost).map_err(|_| STALE)?;
                let slot = Slot {
                    session,
                    packet_id,
                    exchange,
                    publication,
                    lease,
                    admission: None,
                    release_preparations: 0,
                    state: SlotState::AdmissionPending(in_flight),
                };
                self.slots.try_push(slot).map_err(|_| STALE)?;
            }
            Change::Committed { slot, admission } => {
                let held = self.slots.as_mut_slice().get_mut(slot).ok_or(STALE)?;
                if !matches!(held.state, SlotState::AdmissionPending(_)) {
                    return Err(STALE);
                }
                held.admission = Some(admission);
                held.state = SlotState::AwaitPubrel;
            }
            Change::End { slot, finished } => {
                let slots = self.slots.as_mut_slice();
                let last = slots.len().checked_sub(1).ok_or(STALE)?;
                let held = *slots.get(slot).ok_or(STALE)?;
                if self.leases.release(held.lease) != Ok(ReleaseOutcome::Released) {
                    return Err(STALE);
                }
                self.slots.as_mut_slice().swap(slot, last);
                let _ = self.slots.pop();
                // The operations of the exchange stay visible, the latest
                // last.
                if let Some(admission) = held.admission {
                    remember(&mut self.finished, admission)?;
                }
                remember(&mut self.finished, finished)?;
            }
            Change::Await {
                slot,
                phase,
                resolution,
            } => {
                let held = self.slots.as_mut_slice().get_mut(slot).ok_or(STALE)?;
                let (SlotState::AdmissionPending(in_flight) | SlotState::ReleasePending(in_flight)) =
                    &mut held.state
                else {
                    return Err(STALE);
                };
                in_flight.phase = phase;
                if let Some((deadline, effect)) = resolution {
                    in_flight.deadline = deadline;
                    in_flight.answers = in_flight.answers.with(effect);
                }
            }
            Change::Release {
                slot,
                in_flight,
                refused,
            } => {
                let held = self.slots.as_mut_slice().get_mut(slot).ok_or(STALE)?;
                held.state = SlotState::ReleasePending(in_flight);
                held.release_preparations = held.release_preparations.saturating_add(1);
                if let Some(refused) = refused {
                    remember(&mut self.finished, refused)?;
                }
            }
        }
        Ok(())
    }

    /// Adds one fact of a page the plan accepted.
    fn recover(&mut self, fact: &RecoveredFact) -> Result<(), CoreFault> {
        const STALE: CoreFault = CoreFault::StalePlan;
        if let Some(order) = self.recovery.as_mut() {
            order.admit(fact).map_err(|_| STALE)?;
        }
        match fact {
            RecoveredFact::Epoch { .. } => self.epoch_recovered = true,
            RecoveredFact::Limits {
                max_targets_per_admission,
                max_topic_bytes,
                max_payload_bytes,
            } => {
                self.limits = Some(Limits {
                    max_targets: *max_targets_per_admission,
                    max_topic_bytes: *max_topic_bytes,
                    max_payload_bytes: *max_payload_bytes,
                });
            }
            RecoveredFact::RouteTable { revision } => self.route_revision = Some(*revision),
            RecoveredFact::Route {
                topic,
                target,
                target_revision,
                max_qos,
            } => {
                let route = Route {
                    topic: topic.clone(),
                    target: *target,
                    target_revision: *target_revision,
                    max_qos: *max_qos,
                };
                self.routes.try_push(route).map_err(|_| STALE)?;
            }
            RecoveredFact::Session {
                session,
                client_id,
                revision,
            } => {
                let session = Session {
                    id: *session,
                    client_id: client_id.clone(),
                    revision: *revision,
                    connection: None,
                };
                self.sessions.try_push(session).map_err(|_| STALE)?;
            }
            RecoveredFact::InboundExchange {
                exchange,
                packet_id,
                publication,
                // The one durable phase: the admission is committed.
                phase: _,
            } => {
                // One lease per durable row, in the slot of its rank. The
                // payload of the exchange never entered this process.
                let lease = LeaseKey {
                    account: self.leases.account().id(),
                    slot: u32::try_from(self.slots.len()).map_err(|_| STALE)?,
                    generation: LeaseGeneration::new(0),
                };
                let cost = Credits::new(PayloadBytes::ZERO, ExchangeSlots::new(1));
                self.leases.restore(lease, cost).map_err(|_| STALE)?;
                let slot = Slot {
                    session: exchange.session,
                    packet_id: *packet_id,
                    exchange: *exchange,
                    publication: *publication,
                    lease,
                    admission: None,
                    release_preparations: 0,
                    state: SlotState::AwaitPubrel,
                };
                self.slots.try_push(slot).map_err(|_| STALE)?;
            }
            // What an older epoch established about its operations
            // changes nothing here: a committed admission is found through
            // its exchange, and no operation of a closed epoch executes.
            RecoveredFact::Operation { .. } => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::command::StartupNonce;
    use crate::contract::effects::{EffectSink, InboundPhase, OwnerAuthority, SubmitResult};
    use crate::contract::ids::LogicalTime;
    use crate::contract::outcome::{
        EpochOpened, EpochOpening, Knowledge, OpeningOutcome, OpeningReply, RejectReason,
    };

    const INCARNATION: u64 = 1;
    const EPOCH: u64 = 2;
    /// Slots and reservation entries of the box of a world.
    const CAPACITY: usize = 8;
    const ENTRIES: u32 = 2;
    /// The connection of the producer.
    const CONN: u64 = 10;
    const PRODUCER: u64 = 1;
    const GRANT: TurnGrant = TurnGrant {
        work_limit: WorkUnits::new(64),
    };

    /// Takes every effect.
    struct Taking(Vec<OwnedEffect>);

    impl EffectSink for Taking {
        fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
            self.0.push(effect);
            SubmitResult::Accepted
        }
    }

    /// A command the owner submitted, as the store adapter sees it.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    struct Flight {
        effect: EffectId,
        operation: OperationId,
        deadline: DeadlineToken,
        /// What its commit establishes.
        facts: CommitFacts,
    }

    struct World {
        authority: OwnerAuthority,
        owner: Owner,
        /// The commands submitted so far, in order.
        flights: Vec<Flight>,
    }

    /// Everything a refusal must leave unchanged: the state, the identity
    /// counters, the account and its leases, and the content of the box.
    #[derive(PartialEq, Debug)]
    struct Snapshot {
        revision: Revision,
        counters: Counters,
        recovery: Option<RecoveryOrder>,
        epoch_recovered: bool,
        limits: Option<Limits>,
        route_revision: Option<Revision>,
        routes: Vec<Route>,
        sessions: Vec<Session>,
        slots: Vec<Slot>,
        leases: (Credits, Vec<crate::leases::LeaseSlotView>),
        finished: Vec<Finished>,
        closed: Vec<ConnectionGeneration>,
        waiting: Vec<EffectId>,
        free_slots: usize,
        live_reservations: bool,
    }

    fn nonzero(count: usize) -> NonZeroUsize {
        NonZeroUsize::new(count).expect("at least one")
    }

    fn bound(count: u32) -> NonZeroU32 {
        NonZeroU32::new(count).expect("at least one")
    }

    fn bytes(content: &[u8]) -> SharedBytes {
        SharedBytes::copy_from(content, 64).expect("short")
    }

    fn packet_id(raw: u16) -> InboundPacketId {
        InboundPacketId::new(raw).expect("non-zero")
    }

    /// An authority for `owners` owners, of the same run epoch.
    fn authority_of_owners(owners: u32) -> OwnerAuthority {
        let incarnation = StoreIncarnation::new(INCARNATION);
        let mut opening = EpochOpening::new(incarnation, StartupNonce::new([2; 16]));
        let command = opening
            .submit(RunEpoch::new(EPOCH - 1))
            .expect("nothing in flight");
        let reply = OpeningReply {
            command,
            outcome: OpeningOutcome::Committed(EpochOpened {
                incarnation,
                epoch: RunEpoch::new(EPOCH),
            }),
        };
        assert!(opening.observe(reply).is_ok(), "the reply of this attempt");
        let grant = opening.hand_over().expect("committed");
        OwnerAuthority::from_grant(grant, bound(owners)).expect("small")
    }

    /// An authority obtained the way the product obtains it: the opening
    /// of the run epoch, committed and handed over.
    fn authority(incarnation: u64, epoch: u64) -> OwnerAuthority {
        let incarnation = StoreIncarnation::new(incarnation);
        let mut opening = EpochOpening::new(incarnation, StartupNonce::new([1; 16]));
        let command = opening
            .submit(RunEpoch::new(epoch - 1))
            .expect("nothing in flight");
        let reply = OpeningReply {
            command,
            outcome: OpeningOutcome::Committed(EpochOpened {
                incarnation,
                epoch: RunEpoch::new(epoch),
            }),
        };
        assert!(opening.observe(reply).is_ok(), "the reply of this attempt");
        let grant = opening.hand_over().expect("committed");
        OwnerAuthority::from_grant(grant, bound(2)).expect("small")
    }

    fn config() -> OwnerConfig {
        OwnerConfig {
            max_sessions: bound(3),
            max_routes: bound(2),
            max_exchanges: bound(2),
            account: AccountId::new(9),
            payload_bytes: PayloadBytes::new(16),
            resolution_delay: LogicalDuration::new(5),
            retained_operations: bound(4),
            remembered_connections: bound(4),
        }
    }

    fn session_fact(session: u64, client_id: &[u8]) -> RecoveredFact {
        RecoveredFact::Session {
            session: SessionId::new(session),
            client_id: bytes(client_id),
            revision: Revision::new(session + 10),
        }
    }

    fn route_fact(topic: &[u8], target: u64) -> RecoveredFact {
        RecoveredFact::Route {
            topic: bytes(topic),
            target: SessionId::new(target),
            target_revision: Revision::new(target + 10),
            max_qos: Qos::AtLeastOnce,
        }
    }

    fn epoch_fact(epoch: u64) -> RecoveredFact {
        RecoveredFact::Epoch {
            incarnation: StoreIncarnation::new(INCARNATION),
            epoch: RunEpoch::new(epoch),
        }
    }

    const LIMITS: RecoveredFact = RecoveredFact::Limits {
        max_targets_per_admission: 2,
        max_topic_bytes: 16,
        max_payload_bytes: PayloadBytes::new(8),
    };

    /// The fixture of P1-02: three persistent sessions, one topic routed
    /// to two of them.
    fn fixture(epoch: u64) -> Vec<RecoveredFact> {
        vec![
            epoch_fact(epoch),
            LIMITS,
            RecoveredFact::RouteTable {
                revision: Revision::new(3),
            },
            route_fact(b"p1/example", 2),
            route_fact(b"p1/example", 3),
            session_fact(PRODUCER, b"p1-producer"),
            session_fact(2, b"p1-b"),
            session_fact(3, b"p1-c"),
        ]
    }

    fn page(facts: &[RecoveredFact], last: bool) -> Input {
        Input::Recovery {
            facts: BoundedVec::from_slice(facts, facts.len()).expect("fits"),
            last,
        }
    }

    fn connect(connection: u64, client_id: &[u8]) -> Input {
        Input::Packet {
            connection: ConnectionGeneration::new(connection),
            ingress_order: 0,
            packet: InboundPacket::ConnectResume {
                client_id: bytes(client_id),
            },
        }
    }

    fn publish_with(connection: u64, raw: u16, qos: Qos, payload: &[u8]) -> Input {
        Input::Packet {
            connection: ConnectionGeneration::new(connection),
            ingress_order: 0,
            packet: InboundPacket::Publish {
                topic: bytes(b"p1/example"),
                qos,
                retain: false,
                dup: false,
                packet_id: Some(packet_id(raw)),
                payload: bytes(payload),
            },
        }
    }

    fn publish(raw: u16) -> Input {
        publish_with(CONN, raw, Qos::ExactlyOnce, b"payload")
    }

    fn pubrel(raw: u16) -> Input {
        Input::Packet {
            connection: ConnectionGeneration::new(CONN),
            ingress_order: 0,
            packet: InboundPacket::Pubrel {
                packet_id: packet_id(raw),
            },
        }
    }

    fn result(flight: &Flight, outcome: StoreOutcome) -> Input {
        Input::StoreResult {
            effect: flight.effect,
            operation: flight.operation,
            reply: StoreReply::Outcome(outcome),
        }
    }

    fn committed(flight: &Flight) -> Input {
        result(flight, StoreOutcome::Committed(flight.facts))
    }

    fn rejected(flight: &Flight) -> Input {
        let reason = RejectReason::CapacityExceeded(Resource::PayloadBytes);
        result(flight, StoreOutcome::RejectedCertain(reason))
    }

    fn unknown(flight: &Flight) -> Input {
        result(flight, StoreOutcome::PendingOrUnknown)
    }

    fn retired(flight: &Flight) -> Input {
        result(flight, StoreOutcome::RetiredOutcomeUnavailable)
    }

    fn deadline(token: DeadlineToken) -> Input {
        Input::Deadline {
            token,
            now: LogicalTime::new(100),
        }
    }

    /// The name the catalogue of the registry gives an effect.
    fn kind(effect: &Effect) -> &'static str {
        match effect {
            Effect::Persist { .. } => "Persist",
            Effect::ResolveAndFence { .. } => "ResolveAndFence",
            Effect::LoadWork { .. } => "LoadWork",
            Effect::CloseConnection { .. } => "CloseConnection",
            Effect::ArmDeadline { .. } => "ArmDeadline",
            Effect::Continue { .. } => "Continue",
            Effect::EmitFrame { frame, .. } => match frame {
                OutboundFrame::Connack { .. } => "EmitConnack",
                OutboundFrame::Pubrec {
                    reason: AckReason::Success,
                    ..
                } => "EmitPubrec",
                OutboundFrame::Pubrec { .. } => "EmitPubrecRefusal",
                OutboundFrame::Pubcomp {
                    reason: AckReason::Success,
                    ..
                } => "EmitPubcomp",
                OutboundFrame::Pubcomp {
                    reason: AckReason::PacketIdentifierNotFound,
                    ..
                } => "EmitPubcompNotFound",
                OutboundFrame::Pubcomp { .. } => "EmitPubcompRefusal",
            },
        }
    }

    fn kinds(effects: &[(EffectId, Effect)]) -> Vec<&'static str> {
        effects.iter().map(|(_, effect)| kind(effect)).collect()
    }

    impl World {
        /// An owner that still recovers, with an empty box.
        fn cold(epoch: u64, capacity: usize) -> Self {
            Self::with(config(), epoch, capacity)
        }

        fn with(config: OwnerConfig, epoch: u64, capacity: usize) -> Self {
            let mut authority = authority(INCARNATION, epoch);
            let outbox = authority
                .create_outbox(LogicalOwnerId::new(4), nonzero(capacity), bound(ENTRIES))
                .expect("first box");
            let owner = Owner::new(config, outbox).expect("large enough");
            Self {
                authority,
                owner,
                flights: Vec::new(),
            }
        }

        /// An owner that recovered the fixture.
        fn recovered() -> Self {
            let mut world = Self::cold(EPOCH, CAPACITY);
            world.applied(page(&fixture(EPOCH), true));
            world
        }

        /// The producer resumed its session on `CONN`.
        fn connected() -> Self {
            let mut world = Self::recovered();
            world.applied(connect(CONN, b"p1-producer"));
            assert_eq!(kinds(&world.drain()), ["EmitConnack"]);
            world
        }

        /// The exchange of Packet Identifier 7 waits for its admission.
        fn admission_pending() -> Self {
            let mut world = Self::connected();
            let _ = world.submit(publish(7));
            world
        }

        /// The admission of the exchange is known committed.
        fn await_pubrel() -> Self {
            let mut world = Self::admission_pending();
            world.applied(committed(&world.flights[0]));
            assert_eq!(kinds(&world.drain()), ["EmitPubrec"]);
            world
        }

        /// The release of the exchange was submitted.
        fn release_pending() -> Self {
            let mut world = Self::await_pubrel();
            let _ = world.submit(pubrel(7));
            world
        }

        /// The exchange of Packet Identifier 7 ran to its end.
        fn ended() -> Self {
            let mut world = Self::release_pending();
            world.applied(committed(&world.flights[1]));
            assert_eq!(kinds(&world.drain()), ["EmitPubcomp"]);
            world
        }

        /// The box of the owner.
        fn held(&self) -> &Outbox {
            self.owner.outbox().expect("a box")
        }

        /// The box of the owner, to squeeze it in a test.
        fn outbox(&mut self) -> &mut Outbox {
            self.owner.outbox.as_mut().expect("a box")
        }

        fn step(&mut self, input: Input) -> StepOutcome {
            self.owner.step(input, GRANT)
        }

        fn applied(&mut self, input: Input) {
            let outcome = self.step(input);
            assert!(
                matches!(outcome, StepOutcome::Applied { .. }),
                "{outcome:?}"
            );
        }

        /// Applies `input` and says what it published.
        fn sent(&mut self, input: Input) -> Vec<&'static str> {
            self.applied(input);
            kinds(&self.drain())
        }

        /// Hands every waiting effect to an adapter that takes it.
        fn drain(&mut self) -> Vec<(EffectId, Effect)> {
            let mut sink = Taking(Vec::new());
            let _ = self.owner.dispatch(&mut sink);
            sink.0.into_iter().map(OwnedEffect::into_parts).collect()
        }

        /// Applies an input that submits a command, and records the
        /// command: `Persist`, then the deadline of its result.
        fn submit(&mut self, input: Input) -> Flight {
            let outcome = self.step(input);
            assert_eq!(
                outcome,
                StepOutcome::Applied {
                    work_used: WorkUnits::new(1),
                    progress: Progress::WaitingForEffects,
                }
            );
            let effects = self.drain();
            assert_eq!(kinds(&effects), ["Persist", "ArmDeadline"]);
            let (effect, Effect::Persist { command }) = &effects[0] else {
                panic!("a command")
            };
            let (_, Effect::ArmDeadline { token, after }) = &effects[1] else {
                panic!("a deadline")
            };
            assert_eq!(*after, LogicalDuration::new(5));
            let facts = match command.recipe.get() {
                Recipe::AdmitPublication(admit) => {
                    assert!(command.payload.is_some());
                    CommitFacts::PublicationAdmitted {
                        publication: admit.publication,
                        exchange: admit.source.exchange,
                    }
                }
                Recipe::ReleaseInbound(release) => {
                    assert!(command.payload.is_none());
                    CommitFacts::InboundReleased {
                        exchange: release.exchange,
                    }
                }
            };
            let flight = Flight {
                effect: *effect,
                operation: command.recipe.operation(),
                deadline: *token,
                facts,
            };
            self.flights.push(flight);
            flight
        }

        /// The certain refusal the tests deliver for a command: `Fenced`
        /// for a release, which is prepared again, and a lack of capacity
        /// for an admission.
        fn refusal_of(&self, flight: &Flight) -> StoreOutcome {
            let reason = match flight.facts {
                CommitFacts::InboundReleased { .. } => RejectReason::Fenced,
                CommitFacts::PublicationAdmitted { .. } => {
                    RejectReason::CapacityExceeded(Resource::PayloadBytes)
                }
            };
            StoreOutcome::RejectedCertain(reason)
        }

        fn phase(&self, raw: u16) -> Option<ExchangePhase> {
            self.owner
                .exchange(SessionId::new(PRODUCER), packet_id(raw))
                .map(|(_, phase)| phase)
        }

        fn in_flight(&self, raw: u16) -> InFlight {
            let index = self
                .owner
                .slot_of(SessionId::new(PRODUCER), packet_id(raw))
                .expect("an exchange");
            self.owner.slots.as_slice()[index]
                .in_flight()
                .expect("an operation in flight")
        }

        fn snapshot(&self) -> Snapshot {
            let owner = &self.owner;
            Snapshot {
                revision: owner.revision,
                counters: owner.counters,
                recovery: owner.recovery.clone(),
                epoch_recovered: owner.epoch_recovered,
                limits: owner.limits,
                route_revision: owner.route_revision,
                routes: owner.routes.as_slice().to_vec(),
                sessions: owner.sessions.as_slice().to_vec(),
                slots: owner.slots.as_slice().to_vec(),
                leases: owner.leases.snapshot(),
                finished: owner.finished.iter().copied().collect(),
                closed: owner.closed.iter().copied().collect(),
                waiting: self.held().pending().map(OwnedEffect::id).collect(),
                free_slots: self.held().free_slots(),
                live_reservations: self.held().has_live_reservations(),
            }
        }
    }

    /// The reception of a transition at each boundary of capacity (P1-19,
    /// MU-25): the work limit, the effect slots, the reservation table.
    /// The same input comes back and nothing changed: state, identity
    /// counters, account, reservations held, content of the box. `slots`
    /// is what the transition publishes; one that publishes nothing meets
    /// the work limit only.
    fn handed_back(build: fn() -> World, input: &dyn Fn(&World) -> Input, slots: usize) {
        // The work limit.
        let mut world = build();
        let before = world.snapshot();
        let short = TurnGrant {
            work_limit: WorkUnits::ZERO,
        };
        let outcome = world.owner.step(input(&world), short);
        assert_eq!(
            outcome,
            StepOutcome::NeedCapacity {
                input: input(&world),
                need: CapacityNeed {
                    resource: Resource::Work,
                    amount: 1,
                    wake: WakeCondition::TurnGranted,
                },
                work_used: WorkUnits::ZERO,
            }
        );
        assert_eq!(world.snapshot(), before);
        if slots == 0 {
            // Nothing to reserve: a full box does not refuse it.
            let mut world = build();
            let all = nonzero(world.outbox().free_slots());
            let _held = world.outbox().reserve(all).expect("free");
            world.applied(input(&world));
            return;
        }

        // The effect slots: one is missing. A twin world is squeezed the
        // same way and not stepped.
        let mut world = build();
        let mut twin = build();
        let hold = nonzero(world.outbox().free_slots() - (slots - 1));
        let held = world.outbox().reserve(hold).expect("free");
        let twin_held = twin.outbox().reserve(hold).expect("free");
        let before = world.snapshot();
        let outcome = world.step(input(&world));
        assert_eq!(
            outcome,
            StepOutcome::NeedCapacity {
                input: input(&world),
                need: CapacityNeed {
                    resource: Resource::EffectSlots,
                    amount: 1,
                    wake: WakeCondition::EffectAcceptedOrResourceReleased,
                },
                work_used: WorkUnits::new(1),
            }
        );
        assert_eq!(world.snapshot(), before);
        // No entry of the reservation table was taken and given back: the
        // next reservation is the one the twin gets.
        assert_eq!(world.outbox().release(held), Ok(hold.get()));
        assert_eq!(twin.outbox().release(twin_held), Ok(hold.get()));
        let next = world.outbox().reserve(nonzero(1)).expect("free");
        assert_eq!(Ok(next), twin.outbox().reserve(nonzero(1)));
        assert_eq!(world.outbox().release(next), Ok(1));
        // With room, the input that came back is applied.
        world.applied(input(&world));

        // The reservation table: every entry is held.
        let mut world = build();
        for _ in 0..ENTRIES {
            let _held = world.outbox().reserve(nonzero(1)).expect("free");
        }
        let before = world.snapshot();
        let outcome = world.step(input(&world));
        let StepOutcome::NeedCapacity {
            input: back,
            need,
            work_used,
        } = outcome
        else {
            panic!("{outcome:?}")
        };
        assert_eq!(back, input(&world));
        assert_eq!(need.resource, Resource::Reservations);
        assert_eq!(need.wake, WakeCondition::ResourceReleased);
        assert_eq!(work_used, WorkUnits::new(1));
        assert_eq!(world.snapshot(), before);
    }

    /// An input that is absorbed: applied, nothing published, and nothing
    /// changed but the revision.
    fn absorbed(world: &mut World, input: Input) {
        let mut before = world.snapshot();
        assert_eq!(
            world.step(input),
            StepOutcome::Applied {
                work_used: WorkUnits::new(1),
                progress: Progress::Complete,
            }
        );
        before.revision = before.revision.checked_next().expect("far from the end");
        assert_eq!(world.snapshot(), before);
    }

    impl Owner {
        /// Applies a plan with the box the owner holds, as `step` does.
        fn apply_held(
            &mut self,
            plan: Plan,
            input: Input,
            reservation: Option<EffectReservation>,
        ) -> Result<Progress, CoreFault> {
            let mut outbox = self.outbox.take().expect("a box");
            let applied = self.apply(plan, input, reservation, &mut outbox);
            self.outbox = Some(outbox);
            applied
        }
    }

    // ----- Construction and the box of a step -----

    // ADR 0006, amendment: the constructor refuses a box smaller than the
    // largest reservation of a transition.
    #[test]
    fn the_constructor_refuses_a_box_smaller_than_the_largest_reservation() {
        let mut authority = authority(INCARNATION, EPOCH);
        let small = authority
            .create_outbox(
                LogicalOwnerId::new(4),
                nonzero(MAX_TRANSITION_SLOTS - 1),
                bound(1),
            )
            .expect("first box");
        let serial = small.id().serial();
        let refused = Owner::new(config(), small).expect_err("too small");
        assert_eq!(
            refused.reason,
            OwnerRefusal::BoxTooSmall {
                capacity: 1,
                needed: 2,
            }
        );
        // The box comes back, and it can still be retired.
        assert_eq!(refused.outbox.id().serial(), serial);
        assert_eq!(authority.retire(refused.outbox).map_err(|_| ()), Ok(()));
        let exact = authority
            .create_outbox(
                LogicalOwnerId::new(4),
                nonzero(MAX_TRANSITION_SLOTS),
                bound(1),
            )
            .expect("a box for the same owner, once the first is retired");
        let owner = Owner::new(config(), exact).expect("large enough");
        assert_eq!(owner.id(), LogicalOwnerId::new(4));
        assert!(owner.is_recovering());
    }

    // The largest reservation is really made: an admission publishes that
    // many effects into a box of exactly that size.
    #[test]
    fn a_box_of_the_largest_reservation_holds_every_transition() {
        let mut world = World::cold(EPOCH, MAX_TRANSITION_SLOTS);
        world.applied(page(&fixture(EPOCH), true));
        assert_eq!(world.sent(connect(CONN, b"p1-producer")), ["EmitConnack"]);
        let _ = world.submit(publish(7));
        assert_eq!(world.outbox().capacity(), MAX_TRANSITION_SLOTS);
    }

    #[test]
    fn a_box_is_renewed_only_under_the_authority_of_its_owner_and_with_room() {
        let mut world = World::connected();
        let before = world.snapshot();
        let mut later = authority(INCARNATION, EPOCH + 1);
        assert_eq!(
            world
                .owner
                .renew_box(&mut later, nonzero(CAPACITY), bound(ENTRIES)),
            Err(RenewRefused::ForeignAuthority)
        );
        assert_eq!(
            world.owner.renew_box(
                &mut world.authority,
                nonzero(MAX_TRANSITION_SLOTS - 1),
                bound(ENTRIES)
            ),
            Err(RenewRefused::BoxTooSmall {
                capacity: 1,
                needed: 2,
            })
        );
        assert_eq!(world.snapshot(), before);
        // A box that holds a waiting effect or a live reservation is kept.
        let _ = world.submit(publish(7));
        let key = world.outbox().reserve(nonzero(1)).expect("free");
        let serial = world.held().id().serial();
        assert_eq!(
            world
                .owner
                .renew_box(&mut world.authority, nonzero(CAPACITY), bound(ENTRIES)),
            Err(RenewRefused::Retire(RetireRefusal::LiveReservations))
        );
        assert_eq!(world.held().id().serial(), serial);
        assert_eq!(world.outbox().release(key), Ok(1));
    }

    // ADR 0009, audit F4: replacing the box of an owner resets none of its
    // counters and none of its state.
    #[test]
    fn replacing_the_box_resets_no_counter_of_the_owner() {
        let mut world = World::connected();
        let first = world.submit(publish(7));
        let mut before = world.snapshot();
        let serial = world.held().id().serial();
        assert_eq!(
            world
                .owner
                .renew_box(&mut world.authority, nonzero(CAPACITY), bound(ENTRIES)),
            Ok(())
        );
        assert_ne!(world.held().id().serial(), serial);
        assert_eq!(world.held().id().owner(), LogicalOwnerId::new(4));
        // Nothing but the box changed.
        let after = world.snapshot();
        before.free_slots = after.free_slots;
        assert_eq!(after, before);

        world.applied(committed(&first));
        world.applied(publish(8));
        let effects = world.drain();
        assert_eq!(kinds(&effects), ["EmitPubrec", "Persist", "ArmDeadline"]);
        // The sequences of `EffectId` and `OperationId` continue.
        let sequences: Vec<u64> = effects.iter().map(|(id, _)| id.sequence.get()).collect();
        let last = first.effect.sequence.get() + 1;
        assert_eq!(sequences, [last + 1, last + 2, last + 3]);
        let (_, Effect::Persist { command }) = &effects[1] else {
            panic!("a command")
        };
        assert_eq!(
            command.recipe.operation().sequence().get(),
            first.operation.sequence().get() + 1
        );
    }

    // A renewal whose creation fails leaves the owner without a box: every
    // input is handed back, nothing changes, and a later renewal resumes.
    #[test]
    fn an_owner_without_a_box_hands_every_input_back_until_a_renewal() {
        let mut world = World::connected();
        // No allocator provides a box this large.
        let unobtainable = nonzero(isize::MAX as usize / core::mem::size_of::<OwnedEffect>());
        assert_eq!(
            world
                .owner
                .renew_box(&mut world.authority, unobtainable, bound(1)),
            Err(RenewRefused::Create(CreateRefused::AllocationFailed))
        );
        assert!(world.owner.outbox().is_none());
        let counters = world.owner.counters;
        let revision = world.owner.revision();
        assert_eq!(
            world.owner.step(publish(7), GRANT),
            StepOutcome::Exhausted {
                input: publish(7),
                exhaustion: Exhaustion::ReservationTable,
                work_used: WorkUnits::ZERO,
            }
        );
        let mut sink = Taking(Vec::new());
        assert_eq!(
            world.owner.dispatch(&mut sink),
            DispatchReport {
                accepted: 0,
                blocked: None,
            }
        );
        assert_eq!(world.owner.counters, counters);
        assert_eq!(world.owner.revision(), revision);
        assert_eq!(world.phase(7), None);
        assert_eq!(
            world
                .owner
                .renew_box(&mut world.authority, nonzero(CAPACITY), bound(ENTRIES)),
            Ok(())
        );
        let _ = world.submit(publish(7));
    }

    // ----- The plan of ADR 0006 -----

    #[test]
    fn planning_changes_nothing() {
        for build in [
            World::connected,
            World::admission_pending,
            World::await_pubrel,
            World::release_pending,
        ] {
            let world = build();
            let before = world.snapshot();
            for input in [
                publish(7),
                publish(8),
                pubrel(7),
                connect(11, b"p1-producer"),
            ] {
                let _ = world.owner.plan(&input, GRANT);
            }
            for flight in &world.flights {
                for input in [committed(flight), rejected(flight), unknown(flight)] {
                    let _ = world.owner.plan(&input, GRANT);
                }
                let _ = world.owner.plan(&deadline(flight.deadline), GRANT);
            }
            assert_eq!(world.snapshot(), before);
        }
    }

    // A plan is taken by value: the compiler refuses a second application
    // of the same value. Two plans of one input computed against one state
    // are the same plan twice, and the second is refused.
    #[test]
    fn a_plan_is_not_applied_twice_or_after_the_state_moved() {
        let mut world = World::connected();
        let first = world.owner.plan(&publish(7), GRANT).expect("a plan");
        let second = world.owner.plan(&publish(7), GRANT).expect("the same plan");
        let other = world.owner.plan(&pubrel(9), GRANT).expect("another plan");
        let key = world.outbox().reserve(nonzero(2)).expect("free");
        assert_eq!(
            world.owner.apply_held(first, publish(7), Some(key)),
            Ok(Progress::WaitingForEffects)
        );
        let before = world.snapshot();
        let key = world.outbox().reserve(nonzero(2)).expect("free");
        assert_eq!(
            world.owner.apply_held(second, publish(7), Some(key)),
            Err(CoreFault::StalePlan)
        );
        assert_eq!(world.outbox().release(key), Ok(2));
        let key = world.outbox().reserve(nonzero(1)).expect("free");
        assert_eq!(
            world.owner.apply_held(other, pubrel(9), Some(key)),
            Err(CoreFault::StalePlan)
        );
        assert_eq!(world.outbox().release(key), Ok(1));
        assert_eq!(world.snapshot(), before);
    }

    #[test]
    fn a_plan_is_not_applied_to_another_owner() {
        let world = World::connected();
        let plan = world.owner.plan(&publish(7), GRANT).expect("a plan");
        // Another owner at the same revision, with its own box.
        let mut authority = authority(INCARNATION, EPOCH);
        let outbox = authority
            .create_outbox(LogicalOwnerId::new(5), nonzero(CAPACITY), bound(ENTRIES))
            .expect("first box");
        let mut owner = Owner::new(config(), outbox).expect("large enough");
        for input in [page(&fixture(EPOCH), true), connect(CONN, b"p1-producer")] {
            let outcome = owner.step(input, GRANT);
            assert!(
                matches!(outcome, StepOutcome::Applied { .. }),
                "{outcome:?}"
            );
        }
        assert_eq!(owner.revision(), world.owner.revision());
        let key = owner
            .outbox
            .as_mut()
            .expect("a box")
            .reserve(nonzero(2))
            .expect("free");
        assert_eq!(
            owner.apply_held(plan, publish(7), Some(key)),
            Err(CoreFault::StalePlan)
        );
        assert_eq!(owner.exchange(SessionId::new(PRODUCER), packet_id(7)), None);
    }

    // The reservation handed to an application is the one of its plan.
    #[test]
    fn a_plan_is_not_applied_on_a_reservation_that_is_not_its_own() {
        let mut world = World::connected();
        let before = world.snapshot();
        for wrong in [1, 3] {
            let plan = world.owner.plan(&publish(7), GRANT).expect("a plan");
            let key = world.outbox().reserve(nonzero(wrong)).expect("free");
            assert_eq!(
                world.owner.apply_held(plan, publish(7), Some(key)),
                Err(CoreFault::ReservationOverrun)
            );
            assert_eq!(world.outbox().release(key), Ok(wrong));
        }
        let plan = world.owner.plan(&publish(7), GRANT).expect("a plan");
        assert_eq!(
            world.owner.apply_held(plan, publish(7), None),
            Err(CoreFault::ReservationOverrun)
        );
        assert_eq!(world.snapshot(), before);
    }

    // ----- Exhaustion -----

    #[test]
    fn an_exhausted_identity_counter_hands_the_input_back_without_a_wake_up() {
        let exhaust: [fn(&mut Counters); 7] = [
            |counters| counters.operation = None,
            |counters| counters.request = None,
            |counters| counters.publication = None,
            |counters| counters.payload = None,
            |counters| counters.effect = None,
            |counters| counters.deadline = None,
            |counters| counters.generation = None,
        ];
        for one in exhaust {
            let mut world = World::connected();
            one(&mut world.owner.counters);
            let before = world.snapshot();
            assert_eq!(
                world.step(publish(7)),
                StepOutcome::Exhausted {
                    input: publish(7),
                    exhaustion: Exhaustion::IdentityCounter,
                    work_used: WorkUnits::new(1),
                }
            );
            assert_eq!(world.snapshot(), before);
        }
    }

    #[test]
    fn the_last_value_of_a_counter_is_issued_and_none_after_it() {
        let mut world = World::connected();
        world.owner.counters.operation = Some(OperationSeq::new(u64::MAX));
        let flight = world.submit(publish(7));
        assert_eq!(flight.operation.sequence(), OperationSeq::new(u64::MAX));
        assert_eq!(world.owner.counters.operation, None);
        let before = world.snapshot();
        assert_eq!(
            world.step(publish(8)),
            StepOutcome::Exhausted {
                input: publish(8),
                exhaustion: Exhaustion::IdentityCounter,
                work_used: WorkUnits::new(1),
            }
        );
        assert_eq!(world.snapshot(), before);
    }

    // The generation of an exchange carries its run epoch, so that no
    // exchange of a later epoch repeats it.
    #[test]
    fn an_exchange_generation_names_its_run_epoch_and_ends_with_it() {
        let mut world = World::connected();
        let _ = world.submit(publish(7));
        let (exchange, _) = world
            .owner
            .exchange(SessionId::new(PRODUCER), packet_id(7))
            .expect("an exchange");
        assert_eq!(exchange.generation.get(), (EPOCH << 32) | 1);
        assert_eq!(
            world.owner.counters.generation,
            Some(ExchangeGeneration::new((EPOCH << 32) | 2))
        );

        let last = ExchangeGeneration::new((EPOCH << 32) | u64::from(u32::MAX));
        assert_eq!(next_generation(last), Err(Exhausted));
        world.owner.counters.generation = Some(last);
        let _ = world.submit(publish(8));
        assert_eq!(world.owner.counters.generation, None);

        // An epoch that does not fit above the count issues no exchange.
        assert_eq!(Counters::new(RunEpoch::new(1 << 32)).generation, None);
        assert_eq!(
            Counters::new(RunEpoch::new((1 << 32) - 1)).generation,
            Some(ExchangeGeneration::new(u64::MAX - u64::from(u32::MAX) + 1))
        );
    }

    // ----- Recovery -----

    #[test]
    fn the_inventory_is_read_page_by_page_and_closed_by_its_last_page() {
        let facts = fixture(EPOCH);
        let mut whole = World::cold(EPOCH, CAPACITY);
        assert_eq!(
            whole.step(page(&facts, true)),
            StepOutcome::Applied {
                work_used: WorkUnits::new(FIXTURE_WORK),
                progress: Progress::Complete,
            }
        );
        assert!(!whole.owner.is_recovering());
        for cut in 0..=facts.len() {
            let mut world = World::cold(EPOCH, CAPACITY);
            world.applied(page(&facts[..cut], false));
            assert!(world.owner.is_recovering());
            world.applied(page(&facts[cut..], true));
            let mut expected = whole.snapshot();
            expected.revision = Revision::new(2);
            assert_eq!(world.snapshot(), expected, "cut at {cut}");
            assert!(world.outbox().is_empty());
        }
    }

    #[test]
    fn nothing_but_a_recovery_page_is_accepted_before_the_inventory_is_closed() {
        let mut world = World::cold(EPOCH, CAPACITY);
        world.applied(page(&fixture(EPOCH), false));
        let before = world.snapshot();
        for input in [
            connect(CONN, b"p1-producer"),
            publish(7),
            deadline(DeadlineToken::new(1)),
            Input::WorkAvailable {
                session: SessionId::new(PRODUCER),
            },
        ] {
            assert_eq!(
                world.step(input),
                StepOutcome::Fatal(CoreFault::ImpossibleInput)
            );
        }
        assert_eq!(world.snapshot(), before);
        world.applied(page(&[], true));
        // And no page after the one that closed it.
        assert_eq!(
            world.step(page(&[], true)),
            StepOutcome::Fatal(CoreFault::ImpossibleInput)
        );
    }

    fn exchange_fact(session: u64, raw: u16, generation: u64) -> RecoveredFact {
        RecoveredFact::InboundExchange {
            exchange: ExchangeId {
                session: SessionId::new(session),
                generation: ExchangeGeneration::new(generation),
            },
            packet_id: packet_id(raw),
            publication: PublicationId::new(
                StoreIncarnation::new(INCARNATION),
                RunEpoch::new(1),
                LogicalOwnerId::new(4),
                PublicationSeq::new(u64::from(raw)),
            ),
            phase: InboundPhase::AwaitPubrel,
        }
    }

    fn operation_fact(epoch: u64) -> RecoveredFact {
        RecoveredFact::Operation {
            operation: OperationId::new(
                StoreIncarnation::new(INCARNATION),
                RunEpoch::new(epoch),
                LogicalOwnerId::new(4),
                OperationSeq::new(1),
            ),
            knowledge: Knowledge::Unknown,
        }
    }

    #[test]
    fn a_page_the_owner_cannot_hold_or_trust_is_a_fault_and_changes_nothing() {
        let base = fixture(EPOCH);
        let with = |extra: &[RecoveredFact]| {
            let mut facts = base.clone();
            facts.extend_from_slice(extra);
            facts
        };
        let without = |rank: usize| {
            let mut facts = base.clone();
            let _ = facts.remove(rank);
            facts
        };
        let cases: Vec<(&str, Vec<RecoveredFact>)> = vec![
            ("out of order", {
                let mut facts = base.clone();
                facts.swap(0, 1);
                facts
            }),
            ("another run epoch", {
                let mut facts = base.clone();
                facts[0] = epoch_fact(EPOCH + 1);
                facts
            }),
            ("no run epoch", without(0)),
            ("no limits", without(1)),
            ("no route table", {
                let mut facts = base.clone();
                let _ = facts.drain(2..5);
                facts
            }),
            ("a route without its table", without(2)),
            ("more routes than the table holds", {
                let mut facts = base.clone();
                facts.insert(5, route_fact(b"p1/other", 2));
                facts
            }),
            (
                "more sessions than the table holds",
                with(&[session_fact(8, b"p1-d")]),
            ),
            ("a route twice", {
                let mut facts = base.clone();
                facts[4] = route_fact(b"p1/example", 2);
                facts
            }),
            ("a route to no session", {
                let mut facts = base.clone();
                facts[4] = route_fact(b"p1/example", 8);
                facts
            }),
            ("a session twice", {
                let mut facts = without(7);
                facts.push(session_fact(2, b"p1-other"));
                facts
            }),
            ("a client identifier twice", {
                let mut facts = without(7);
                facts.push(session_fact(8, b"p1-b"));
                facts
            }),
            ("an exchange of no session", with(&[exchange_fact(8, 7, 1)])),
            (
                "a Packet Identifier twice",
                with(&[exchange_fact(1, 7, 1), exchange_fact(1, 7, 2)]),
            ),
            (
                "an exchange twice",
                with(&[exchange_fact(1, 7, 1), exchange_fact(1, 8, 1)]),
            ),
            (
                "more exchanges than the table holds",
                with(&[
                    exchange_fact(1, 7, 1),
                    exchange_fact(1, 8, 2),
                    exchange_fact(1, 9, 3),
                ]),
            ),
            (
                "an exchange of this run epoch",
                with(&[exchange_fact(1, 7, EPOCH << 32)]),
            ),
            (
                "an operation of this run epoch",
                with(&[operation_fact(EPOCH)]),
            ),
        ];
        for (case, facts) in cases {
            let mut world = World::cold(EPOCH, CAPACITY);
            let before = world.snapshot();
            assert_eq!(
                world.step(page(&facts, true)),
                StepOutcome::Fatal(CoreFault::ImpossibleInput),
                "{case}"
            );
            assert_eq!(world.snapshot(), before, "{case}");
        }
        // A route without its table is refused on a page that does not
        // close the inventory, before the inventory could miss the table.
        let mut world = World::cold(EPOCH, CAPACITY);
        let before = world.snapshot();
        assert_eq!(
            world.step(page(&without(2)[..3], false)),
            StepOutcome::Fatal(CoreFault::ImpossibleInput)
        );
        assert_eq!(world.snapshot(), before);
        // The same checks hold across pages.
        let mut world = World::cold(EPOCH, CAPACITY);
        world.applied(page(&with(&[exchange_fact(1, 7, 1)]), false));
        for (case, fact) in [
            ("a Packet Identifier twice", exchange_fact(1, 7, 2)),
            ("an exchange twice", exchange_fact(1, 8, 1)),
            ("out of order", session_fact(8, b"p1-d")),
        ] {
            assert_eq!(
                world.step(page(&[fact], true)),
                StepOutcome::Fatal(CoreFault::ImpossibleInput),
                "{case}"
            );
        }
        // What is accepted: an exchange and an operation of an older epoch.
        world.applied(page(
            &[exchange_fact(1, 8, 2), operation_fact(EPOCH - 1)],
            true,
        ));
    }

    // Record 0010: one lease per durable row, restored once.
    #[test]
    fn a_recovered_exchange_awaits_its_pubrel_and_holds_one_lease() {
        let mut facts = fixture(EPOCH);
        facts.extend([
            exchange_fact(1, 7, 1),
            exchange_fact(3, 7, 1),
            operation_fact(1),
        ]);
        let mut world = World::cold(EPOCH, CAPACITY);
        world.applied(page(&facts, true));
        assert_eq!(world.phase(7), Some(ExchangePhase::AwaitPubrel));
        assert_eq!(world.owner.leases.held(), 2);
        assert_eq!(
            world.owner.leases.account().used(),
            Credits::new(PayloadBytes::ZERO, ExchangeSlots::new(2))
        );
        let leases: Vec<LeaseKey> = world
            .owner
            .slots
            .as_slice()
            .iter()
            .map(|s| s.lease)
            .collect();
        assert!(leases.iter().all(|lease| world.owner.leases.holds(*lease)));
        assert_ne!(leases[0], leases[1]);
        // The table is full: a new exchange waits for one to end.
        assert_eq!(world.sent(connect(CONN, b"p1-producer")), ["EmitConnack"]);
        assert_eq!(
            world.step(publish(8)),
            StepOutcome::NeedCapacity {
                input: publish(8),
                need: CapacityNeed {
                    resource: Resource::ExchangeSlots,
                    amount: 1,
                    wake: WakeCondition::ResourceReleased,
                },
                work_used: WorkUnits::new(1),
            }
        );
        // The recovered exchange ends through the one release function.
        let release = world.submit(pubrel(7));
        assert_eq!(world.sent(committed(&release)), ["EmitPubcomp"]);
        assert_eq!(world.owner.leases.held(), 1);
        assert!(!world.owner.leases.holds(leases[0]));
        let _ = world.submit(publish(8));
    }

    // ----- Session resume -----

    #[test]
    fn a_resumed_session_is_answered_with_session_present() {
        let mut world = World::recovered();
        world.applied(connect(CONN, b"p1-producer"));
        let effects = world.drain();
        assert_eq!(
            effects[0].1,
            Effect::EmitFrame {
                connection: ConnectionKey {
                    session: SessionId::new(PRODUCER),
                    generation: ConnectionGeneration::new(CONN),
                },
                frame: OutboundFrame::Connack {
                    session_present: true,
                },
            }
        );
        assert_eq!(effects.len(), 1);
        assert_eq!(
            world.owner.connection_of(SessionId::new(PRODUCER)),
            Some(ConnectionGeneration::new(CONN))
        );
    }

    #[test]
    fn resuming_a_session_is_handed_back_at_each_boundary_of_capacity() {
        handed_back(World::recovered, &|_| connect(CONN, b"p1-producer"), 1);
        // A take-over closes the previous connection in the same transition.
        handed_back(World::connected, &|_| connect(11, b"p1-producer"), 2);
    }

    #[test]
    fn a_newer_connection_takes_the_session_over() {
        let mut world = World::connected();
        world.applied(connect(11, b"p1-producer"));
        let effects = world.drain();
        assert_eq!(kinds(&effects), ["CloseConnection", "EmitConnack"]);
        assert_eq!(
            effects[0].1,
            Effect::CloseConnection {
                connection: ConnectionGeneration::new(CONN),
                reason: CloseReason::SessionTakenOver,
            }
        );
        assert_eq!(
            world.owner.connection_of(SessionId::new(PRODUCER)),
            Some(ConnectionGeneration::new(11))
        );
        // The connection that was replaced is closed: what it still
        // carries is absorbed.
        absorbed(&mut world, publish(7));
        assert_eq!(world.phase(7), None);
    }

    #[test]
    fn a_packet_outside_the_subset_closes_its_connection() {
        let close = |connection: u64| Effect::CloseConnection {
            connection: ConnectionGeneration::new(connection),
            reason: CloseReason::ProtocolError,
        };
        // An unknown client, and a packet before CONNECT.
        let mut world = World::recovered();
        let pubrel_on = |connection: u64| Input::Packet {
            connection: ConnectionGeneration::new(connection),
            ingress_order: 0,
            packet: InboundPacket::Pubrel {
                packet_id: packet_id(7),
            },
        };
        for (connection, input) in [
            (CONN, connect(CONN, b"stranger")),
            (12, publish_with(12, 7, Qos::ExactlyOnce, b"payload")),
            (13, pubrel_on(13)),
        ] {
            world.applied(input);
            assert_eq!(world.drain()[0].1, close(connection));
        }
        assert_eq!(world.owner.connection_of(SessionId::new(PRODUCER)), None);
        // A second CONNECT, a QoS below 2, a retained message: the session
        // loses its connection and keeps everything else.
        let retained = || Input::Packet {
            connection: ConnectionGeneration::new(CONN),
            ingress_order: 0,
            packet: InboundPacket::Publish {
                topic: bytes(b"p1/example"),
                qos: Qos::ExactlyOnce,
                retain: true,
                dup: false,
                packet_id: Some(packet_id(7)),
                payload: bytes(b"payload"),
            },
        };
        for input in [
            connect(CONN, b"p1-producer"),
            connect(CONN, b"p1-b"),
            publish_with(CONN, 7, Qos::AtLeastOnce, b"payload"),
            publish_with(CONN, 7, Qos::AtMostOnce, b"payload"),
            retained(),
        ] {
            let mut world = World::connected();
            world.applied(input);
            let effects = world.drain();
            assert_eq!(effects.len(), 1);
            assert_eq!(effects[0].1, close(CONN));
            assert_eq!(world.owner.connection_of(SessionId::new(PRODUCER)), None);
            assert_eq!(world.phase(7), None);
        }
    }

    #[test]
    fn a_failed_frame_ends_the_connection_of_its_session() {
        let frame = |session: u64, generation: u64, progress| Input::FrameResult {
            effect: EffectId {
                epoch: RunEpoch::new(EPOCH),
                owner: LogicalOwnerId::new(4),
                sequence: EffectSeq::new(1),
            },
            connection: ConnectionKey {
                session: SessionId::new(session),
                generation: ConnectionGeneration::new(generation),
            },
            progress,
        };
        let written = FrameProgress::Written {
            confirmed: 4,
            total: 4,
        };
        let mut world = World::connected();
        for stale in [
            frame(PRODUCER, CONN, written),
            frame(PRODUCER, 9, FrameProgress::Failed),
            frame(2, CONN, FrameProgress::Failed),
        ] {
            absorbed(&mut world, stale);
        }
        world.applied(frame(PRODUCER, CONN, FrameProgress::Failed));
        assert_eq!(world.owner.connection_of(SessionId::new(PRODUCER)), None);
        assert!(world.outbox().is_empty());
        // The connection is gone: what it still carries is absorbed.
        absorbed(&mut world, publish(7));
        absorbed(&mut world, connect(CONN, b"p1-producer"));
        assert_eq!(world.owner.connection_of(SessionId::new(PRODUCER)), None);
    }

    // ----- The machine `inbound_qos2`: one test per rule, named after it -----

    const PENDING: Option<ExchangePhase> = Some(ExchangePhase::AdmissionPending);
    const AWAIT: Option<ExchangePhase> = Some(ExchangePhase::AwaitPubrel);
    const RELEASING: Option<ExchangePhase> = Some(ExchangePhase::ReleasePending);

    fn used(world: &World) -> (u64, u64) {
        let used = world.owner.leases.account().used();
        (used.bytes.get(), used.slots.get())
    }

    fn frame_to(connection: u64, frame: OutboundFrame) -> Effect {
        Effect::EmitFrame {
            connection: ConnectionKey {
                session: SessionId::new(PRODUCER),
                generation: ConnectionGeneration::new(connection),
            },
            frame,
        }
    }

    fn pubrec(reason: AckReason) -> OutboundFrame {
        OutboundFrame::Pubrec {
            packet_id: packet_id(7),
            reason,
        }
    }

    /// The results of an operation no exchange waits for that agree with
    /// what is known of it are absorbed: an unknown or retired answer, and
    /// the repetition of its terminal result. A contradiction is a fault:
    /// see `a_late_result_that_contradicts_an_operation_that_ended_is_a_fault`.
    fn every_result_is_absorbed(world: &mut World, flight: &Flight, committed_known: bool) {
        let repeated = if committed_known {
            committed(flight)
        } else {
            result(flight, world.refusal_of(flight))
        };
        for input in [repeated, unknown(flight), retired(flight)] {
            absorbed(world, input);
        }
    }

    fn fenced(flight: &Flight) -> Input {
        result(flight, StoreOutcome::RejectedCertain(RejectReason::Fenced))
    }

    fn mismatch(flight: &Flight) -> Input {
        Input::StoreResult {
            effect: flight.effect,
            operation: flight.operation,
            reply: StoreReply::Violation(StoreViolation::RecipeMismatch),
        }
    }

    /// A fault stops the owner and changes nothing.
    fn faulted(world: &mut World, input: Input, fault: CoreFault) {
        let before = world.snapshot();
        assert_eq!(world.step(input), StepOutcome::Fatal(fault));
        assert_eq!(world.snapshot(), before);
    }

    /// A world whose first admission was certainly refused: Packet
    /// Identifier 7 holds no exchange and `flights[0]` is stale.
    fn refused() -> World {
        let mut world = World::admission_pending();
        assert_eq!(
            world.sent(rejected(&world.flights[0])),
            ["EmitPubrecRefusal"]
        );
        world
    }

    #[test]
    fn rule_inbound_qos2_absent_publish_admit() {
        let mut world = World::connected();
        let flight = world.submit(publish(7));
        assert_eq!(world.phase(7), PENDING);
        assert_eq!(
            world.in_flight(7),
            InFlight {
                operation: flight.operation,
                phase: CommandPhase::Submitted,
                deadline: flight.deadline,
                answers: Answers::new(flight.effect),
            }
        );
        // The exchange is charged to the account of the owner.
        assert_eq!(used(&world), (7, 1));
    }

    #[test]
    fn rule_inbound_qos2_absent_publish_defer() {
        handed_back(World::connected, &|_| publish(7), 2);
    }

    #[test]
    fn rule_inbound_qos2_admission_pending_publish_replay_after_reconnect() {
        let mut world = World::admission_pending();
        assert_eq!(
            world.sent(connect(11, b"p1-producer")),
            ["CloseConnection", "EmitConnack"]
        );
        absorbed(
            &mut world,
            publish_with(11, 7, Qos::ExactlyOnce, b"payload"),
        );
        assert_eq!(world.flights.len(), 1);
        // PUBREC goes to the connection that is current at the commit.
        world.applied(committed(&world.flights[0]));
        assert_eq!(world.drain()[0].1, frame_to(11, pubrec(AckReason::Success)));
    }

    #[test]
    fn rule_inbound_qos2_admission_pending_publish_repeat_on_live_connection() {
        let mut world = World::admission_pending();
        absorbed(&mut world, publish(7));
        // Whatever the repeated packet carries: the exchange owns its
        // admission and its recipe.
        absorbed(
            &mut world,
            publish_with(CONN, 7, Qos::ExactlyOnce, b"other"),
        );
        assert_eq!(world.in_flight(7).operation, world.flights[0].operation);
    }

    #[test]
    fn rule_inbound_qos2_admission_pending_committed_authorize() {
        let mut world = World::admission_pending();
        let admission = world.flights[0];
        world.applied(committed(&admission));
        let effects = world.drain();
        assert_eq!(kinds(&effects), ["EmitPubrec"]);
        assert_eq!(effects[0].1, frame_to(CONN, pubrec(AckReason::Success)));
        assert_eq!(world.phase(7), AWAIT);
        // The lease of the exchange stays: its charge is now durable.
        assert_eq!(used(&world), (7, 1));
    }

    #[test]
    fn rule_inbound_qos2_admission_pending_committed_defer() {
        handed_back(World::admission_pending, &|w| committed(&w.flights[0]), 1);
    }

    #[test]
    fn rule_inbound_qos2_await_pubrel_publish_replay() {
        let mut world = World::await_pubrel();
        let counters = world.owner.counters.operation;
        world.applied(publish(7));
        let effects = world.drain();
        assert_eq!(kinds(&effects), ["EmitPubrec"]);
        assert_eq!(effects[0].1, frame_to(CONN, pubrec(AckReason::Success)));
        assert_eq!(world.phase(7), AWAIT);
        assert_eq!(world.owner.counters.operation, counters);
    }

    #[test]
    fn rule_inbound_qos2_await_pubrel_publish_defer() {
        handed_back(World::await_pubrel, &|_| publish(7), 1);
    }

    #[test]
    fn rule_inbound_qos2_await_pubrel_pubrel_release() {
        let mut world = World::await_pubrel();
        let (exchange, _) = world
            .owner
            .exchange(SessionId::new(PRODUCER), packet_id(7))
            .expect("an exchange");
        let release = world.submit(pubrel(7));
        assert_eq!(release.facts, CommitFacts::InboundReleased { exchange });
        assert_ne!(release.operation, world.flights[0].operation);
        assert_eq!(world.phase(7), RELEASING);
        assert_eq!(world.in_flight(7).operation, release.operation);
    }

    #[test]
    fn rule_inbound_qos2_await_pubrel_pubrel_defer() {
        handed_back(World::await_pubrel, &|_| pubrel(7), 2);
    }

    #[test]
    fn rule_inbound_qos2_await_pubrel_late_unknown_ignore() {
        let mut world = World::await_pubrel();
        let admission = world.flights[0];
        absorbed(&mut world, unknown(&admission));
        assert_eq!(world.phase(7), AWAIT);
    }

    #[test]
    fn rule_inbound_qos2_release_pending_committed_complete() {
        let mut world = World::release_pending();
        world.applied(committed(&world.flights[1]));
        let effects = world.drain();
        assert_eq!(kinds(&effects), ["EmitPubcomp"]);
        let frame = OutboundFrame::Pubcomp {
            packet_id: packet_id(7),
            reason: AckReason::Success,
        };
        assert_eq!(effects[0].1, frame_to(CONN, frame));
        assert_eq!(world.phase(7), None);
        // The exchange gave its lease back.
        assert_eq!(used(&world), (0, 0));
        assert_eq!(world.owner.leases.held(), 0);
    }

    #[test]
    fn rule_inbound_qos2_release_pending_committed_defer() {
        handed_back(World::release_pending, &|w| committed(&w.flights[1]), 1);
    }

    #[test]
    fn rule_inbound_qos2_absent_pubrel_not_found() {
        let mut world = World::connected();
        assert_eq!(world.sent(pubrel(7)), ["EmitPubcompNotFound"]);
        assert_eq!(world.phase(7), None);
        assert_eq!(used(&world), (0, 0));
    }

    #[test]
    fn rule_inbound_qos2_absent_pubrel_defer() {
        handed_back(World::connected, &|_| pubrel(7), 1);
    }

    #[test]
    fn rule_inbound_qos2_absent_admission_result_stale() {
        let mut world = World::ended();
        let admission = world.flights[0];
        every_result_is_absorbed(&mut world, &admission, true);
        let mut world = refused();
        let admission = world.flights[0];
        every_result_is_absorbed(&mut world, &admission, false);
    }

    // Unreachable: a Packet Identifier with no exchange has no operation a
    // result could belong to. Whatever is delivered is stale, also under
    // the identities of the exchange that ended, and creates nothing.
    #[test]
    fn rule_inbound_qos2_absent_admission_result_current() {
        let mut world = refused();
        assert_eq!(world.phase(7), None);
        assert!(world.owner.slots.is_empty());
        let admission = world.flights[0];
        every_result_is_absorbed(&mut world, &admission, false);
        assert_eq!(world.phase(7), None);
    }

    #[test]
    fn rule_inbound_qos2_absent_release_result_stale() {
        let mut world = World::ended();
        let release = world.flights[1];
        every_result_is_absorbed(&mut world, &release, true);
    }

    // Unreachable, for the same reason as the admission result.
    #[test]
    fn rule_inbound_qos2_absent_release_result_current() {
        let mut world = World::ended();
        assert_eq!(world.phase(7), None);
        assert!(world.owner.slots.is_empty());
        let release = world.flights[1];
        every_result_is_absorbed(&mut world, &release, true);
        assert_eq!(world.phase(7), None);
    }

    #[test]
    fn rule_inbound_qos2_admission_pending_pubrel_close() {
        let mut world = World::admission_pending();
        let in_flight = world.in_flight(7);
        world.applied(pubrel(7));
        assert_eq!(
            world.drain()[0].1,
            Effect::CloseConnection {
                connection: ConnectionGeneration::new(CONN),
                reason: CloseReason::ProtocolError,
            }
        );
        // The admission keeps its operation and its resolution continues.
        assert_eq!(world.phase(7), PENDING);
        assert_eq!(world.in_flight(7), in_flight);
        assert_eq!(world.owner.connection_of(SessionId::new(PRODUCER)), None);
    }

    #[test]
    fn rule_inbound_qos2_admission_pending_pubrel_defer() {
        handed_back(World::admission_pending, &|_| pubrel(7), 1);
    }

    #[test]
    fn rule_inbound_qos2_admission_pending_rejected_refuse() {
        let reasons = [
            (
                RejectReason::CapacityExceeded(Resource::PayloadBytes),
                AckReason::QuotaExceeded,
            ),
            (RejectReason::Fenced, AckReason::ImplementationSpecificError),
        ];
        for (rejection, reason) in reasons {
            let mut world = World::admission_pending();
            let admission = world.flights[0];
            world.applied(result(&admission, StoreOutcome::RejectedCertain(rejection)));
            let effects = world.drain();
            assert_eq!(kinds(&effects), ["EmitPubrecRefusal"]);
            assert_eq!(effects[0].1, frame_to(CONN, pubrec(reason)));
            // Nothing was admitted: the Packet Identifier is free again and
            // the lease is given back.
            assert_eq!(world.phase(7), None);
            assert_eq!(used(&world), (0, 0));
        }
    }

    #[test]
    fn rule_inbound_qos2_admission_pending_rejected_defer() {
        handed_back(World::admission_pending, &|w| rejected(&w.flights[0]), 1);
    }

    #[test]
    fn rule_inbound_qos2_admission_pending_unknown_keep() {
        let mut world = World::admission_pending();
        let admission = world.flights[0];
        world.applied(unknown(&admission));
        assert!(world.outbox().is_empty());
        assert_eq!(world.phase(7), PENDING);
        assert_eq!(
            world.in_flight(7),
            InFlight {
                operation: admission.operation,
                phase: CommandPhase::Indeterminate,
                deadline: admission.deadline,
                answers: Answers::new(admission.effect),
            }
        );
    }

    // Unreachable in S0, which retires no identity space: the owner stops.
    #[test]
    fn rule_inbound_qos2_admission_pending_retired_unreachable() {
        let mut world = World::admission_pending();
        let admission = world.flights[0];
        faulted(&mut world, retired(&admission), CoreFault::ImpossibleInput);
    }

    #[test]
    fn rule_inbound_qos2_admission_pending_admission_result_stale() {
        let mut world = refused();
        let current = world.submit(publish(7));
        let stale = world.flights[0];
        every_result_is_absorbed(&mut world, &stale, false);
        assert_eq!(world.in_flight(7).operation, current.operation);
    }

    #[test]
    fn rule_inbound_qos2_admission_pending_release_result_stale() {
        let mut world = World::ended();
        let current = world.submit(publish(7));
        let stale = world.flights[1];
        every_result_is_absorbed(&mut world, &stale, true);
        assert_eq!(world.in_flight(7).operation, current.operation);
    }

    // Unreachable: an exchange whose admission is pending submitted no
    // release. A commit that claims a release under the operation of the
    // admission contradicts its recipe.
    #[test]
    fn rule_inbound_qos2_admission_pending_release_result_current() {
        let mut world = World::admission_pending();
        assert_eq!(world.flights.len(), 1);
        let admission = world.flights[0];
        let CommitFacts::PublicationAdmitted { exchange, .. } = admission.facts else {
            panic!("an admission")
        };
        let claim = StoreOutcome::Committed(CommitFacts::InboundReleased { exchange });
        let fault = CoreFault::ContradictoryOutcome {
            operation: admission.operation,
        };
        faulted(&mut world, result(&admission, claim), fault);
    }

    #[test]
    fn rule_inbound_qos2_await_pubrel_release_result_stale() {
        let mut world = World::ended();
        let current = world.submit(publish(7));
        assert_eq!(world.sent(committed(&current)), ["EmitPubrec"]);
        let stale = world.flights[1];
        every_result_is_absorbed(&mut world, &stale, true);
        assert_eq!(world.phase(7), AWAIT);
    }

    // Unreachable: an exchange that awaits its PUBREL has no operation in
    // flight, so no release result is current.
    #[test]
    fn rule_inbound_qos2_await_pubrel_release_result_current() {
        let mut world = World::await_pubrel();
        let slots = world.owner.slots.as_slice();
        assert_eq!(slots.len(), 1);
        assert_eq!(slots[0].in_flight(), None);
        let admission = world.flights[0];
        let CommitFacts::PublicationAdmitted { exchange, .. } = admission.facts else {
            panic!("an admission")
        };
        // A release claimed under the operation of the admission
        // contradicts what that operation committed.
        let claim = StoreOutcome::Committed(CommitFacts::InboundReleased { exchange });
        let fault = CoreFault::ContradictoryOutcome {
            operation: admission.operation,
        };
        faulted(&mut world, result(&admission, claim), fault);
        assert_eq!(world.phase(7), AWAIT);
    }

    #[test]
    fn rule_inbound_qos2_await_pubrel_admission_result_repeat() {
        let mut world = World::await_pubrel();
        let admission = world.flights[0];
        absorbed(&mut world, committed(&admission));
        absorbed(&mut world, retired(&admission));
        assert_eq!(world.phase(7), AWAIT);
    }

    #[test]
    fn rule_inbound_qos2_await_pubrel_admission_result_rejected_stale() {
        let mut world = refused();
        let current = world.submit(publish(7));
        assert_eq!(world.sent(committed(&current)), ["EmitPubrec"]);
        let stale = world.flights[0];
        absorbed(&mut world, rejected(&stale));
        assert_eq!(world.phase(7), AWAIT);
    }

    #[test]
    fn rule_inbound_qos2_await_pubrel_admission_result_contradiction() {
        let mut world = World::await_pubrel();
        let admission = world.flights[0];
        let fault = CoreFault::ContradictoryOutcome {
            operation: admission.operation,
        };
        faulted(&mut world, rejected(&admission), fault);
    }

    #[test]
    fn rule_inbound_qos2_release_pending_publish_close() {
        let mut world = World::release_pending();
        let in_flight = world.in_flight(7);
        world.applied(publish(7));
        assert_eq!(
            world.drain()[0].1,
            Effect::CloseConnection {
                connection: ConnectionGeneration::new(CONN),
                reason: CloseReason::ProtocolError,
            }
        );
        // The release continues.
        assert_eq!(world.phase(7), RELEASING);
        assert_eq!(world.in_flight(7), in_flight);
        assert_eq!(world.flights.len(), 2);
    }

    #[test]
    fn rule_inbound_qos2_release_pending_publish_defer() {
        handed_back(World::release_pending, &|_| publish(7), 1);
    }

    #[test]
    fn rule_inbound_qos2_release_pending_pubrel_replay() {
        let mut world = World::release_pending();
        absorbed(&mut world, pubrel(7));
        assert_eq!(world.in_flight(7).operation, world.flights[1].operation);
    }

    #[test]
    fn rule_inbound_qos2_release_pending_admission_result_repeat() {
        let mut world = World::release_pending();
        let admission = world.flights[0];
        for input in [
            committed(&admission),
            unknown(&admission),
            retired(&admission),
        ] {
            absorbed(&mut world, input);
        }
        assert_eq!(world.phase(7), RELEASING);
    }

    #[test]
    fn rule_inbound_qos2_release_pending_admission_result_rejected_stale() {
        let mut world = refused();
        let current = world.submit(publish(7));
        assert_eq!(world.sent(committed(&current)), ["EmitPubrec"]);
        let _ = world.submit(pubrel(7));
        let stale = world.flights[0];
        absorbed(&mut world, rejected(&stale));
        assert_eq!(world.phase(7), RELEASING);
    }

    #[test]
    fn rule_inbound_qos2_release_pending_admission_result_contradiction() {
        let mut world = World::release_pending();
        let admission = world.flights[0];
        let fault = CoreFault::ContradictoryOutcome {
            operation: admission.operation,
        };
        faulted(&mut world, rejected(&admission), fault);
    }

    #[test]
    fn rule_inbound_qos2_release_pending_rejected_prepare_again() {
        let mut world = World::release_pending();
        let refused = world.flights[1];
        let again = world.submit(fenced(&refused));
        // Another operation for the same exchange.
        assert_ne!(again.operation, refused.operation);
        assert_eq!(again.facts, refused.facts);
        assert_eq!(world.phase(7), RELEASING);
        assert_eq!(world.in_flight(7).operation, again.operation);
        assert_eq!(used(&world), (7, 1));
        assert_eq!(world.sent(committed(&again)), ["EmitPubcomp"]);
    }

    #[test]
    fn rule_inbound_qos2_release_pending_rejected_defer() {
        handed_back(World::release_pending, &|w| fenced(&w.flights[1]), 2);
    }

    #[test]
    fn rule_inbound_qos2_release_pending_unknown_keep() {
        let mut world = World::release_pending();
        let release = world.flights[1];
        world.applied(unknown(&release));
        assert!(world.outbox().is_empty());
        assert_eq!(world.phase(7), RELEASING);
        assert_eq!(
            world.in_flight(7),
            InFlight {
                operation: release.operation,
                phase: CommandPhase::Indeterminate,
                deadline: release.deadline,
                answers: Answers::new(release.effect),
            }
        );
    }

    // Unreachable in S0, which retires no identity space: the owner stops.
    #[test]
    fn rule_inbound_qos2_release_pending_retired_unreachable() {
        let mut world = World::release_pending();
        let release = world.flights[1];
        faulted(&mut world, retired(&release), CoreFault::ImpossibleInput);
    }

    #[test]
    fn rule_inbound_qos2_release_pending_release_result_stale() {
        let mut world = World::release_pending();
        let stale = world.flights[1];
        let current = world.submit(fenced(&stale));
        every_result_is_absorbed(&mut world, &stale, false);
        assert_eq!(world.in_flight(7).operation, current.operation);
    }

    // ----- The unknown outcome on a live process (ADR 0001) -----

    #[test]
    fn a_deadline_starts_a_resolution_and_arms_the_next_deadline() {
        let mut world = World::admission_pending();
        let admission = world.flights[0];
        assert_eq!(
            world.step(deadline(admission.deadline)),
            StepOutcome::Applied {
                work_used: WorkUnits::new(1),
                progress: Progress::WaitingForEffects,
            }
        );
        let first = world.drain();
        assert_eq!(kinds(&first), ["ResolveAndFence", "ArmDeadline"]);
        assert_eq!(
            first[0].1,
            Effect::ResolveAndFence {
                operation: admission.operation,
            }
        );
        let Effect::ArmDeadline { token, .. } = first[1].1 else {
            panic!("a deadline")
        };
        assert_ne!(token, admission.deadline);
        assert_eq!(
            world.in_flight(7),
            InFlight {
                operation: admission.operation,
                phase: CommandPhase::Indeterminate,
                deadline: token,
                answers: Answers::new(admission.effect).with(first[0].0),
            }
        );
        // The deadline that was replaced is stale.
        absorbed(&mut world, deadline(admission.deadline));
        // The next one starts another resolution, under a new effect
        // identity, and arms a third.
        world.applied(deadline(token));
        let second = world.drain();
        assert_eq!(kinds(&second), ["ResolveAndFence", "ArmDeadline"]);
        assert_eq!(second[0].1, first[0].1);
        assert_ne!(second[0].0, first[0].0);
        assert_ne!(second[1].1, first[1].1);
    }

    // command.prepared.submit.defer: a submission whose slots are missing
    // is handed back before the command exists.
    #[test]
    fn rule_command_prepared_submit_defer() {
        handed_back(World::connected, &|_| publish(7), 2);
        handed_back(World::await_pubrel, &|_| pubrel(7), 2);
    }

    // command.submitted.timeout.defer
    #[test]
    fn rule_command_submitted_timeout_defer() {
        handed_back(
            World::admission_pending,
            &|w| deadline(w.flights[0].deadline),
            2,
        );
        handed_back(
            World::release_pending,
            &|w| deadline(w.flights[1].deadline),
            2,
        );
    }

    // command.indeterminate.timeout.defer
    #[test]
    fn rule_command_indeterminate_timeout_defer() {
        let indeterminate = || {
            let mut world = World::admission_pending();
            world.applied(unknown(&world.flights[0]));
            world
        };
        handed_back(indeterminate, &|w| deadline(w.flights[0].deadline), 2);
    }

    #[test]
    fn a_deadline_of_a_resolved_operation_is_stale() {
        let mut world = World::await_pubrel();
        let admission = world.flights[0];
        absorbed(&mut world, deadline(admission.deadline));
        let mut world = World::ended();
        for flight in world.flights.clone() {
            absorbed(&mut world, deadline(flight.deadline));
        }
    }

    #[test]
    fn a_deadline_that_was_never_armed_is_a_fault() {
        let mut world = World::admission_pending();
        let next = world.owner.counters.deadline.expect("far from the end");
        faulted(&mut world, deadline(next), CoreFault::ImpossibleInput);
    }

    // P1-02b: the result is lost and the process lives. The second
    // resolution finds the commit, and the unknown answer of the first,
    // delivered afterwards, replaces nothing (DEM-MU-05).
    #[test]
    fn p1_02b_an_unknown_outcome_is_resolved_without_a_crash() {
        let mut world = World::connected();
        let admission = world.submit(publish(7));
        // The first deadline fires; the answer of its resolution is held.
        world.applied(deadline(admission.deadline));
        let first = world.drain();
        let held = Input::StoreResult {
            effect: first[0].0,
            operation: admission.operation,
            reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
        };
        let Effect::ArmDeadline { token: second, .. } = first[1].1 else {
            panic!("a deadline")
        };
        // The command commits and its result is dropped. The second
        // deadline fires and its resolution reports the commit.
        world.applied(deadline(second));
        let resolution = world.drain();
        assert_eq!(kinds(&resolution), ["ResolveAndFence", "ArmDeadline"]);
        world.applied(Input::StoreResult {
            effect: resolution[0].0,
            operation: admission.operation,
            reply: StoreReply::Outcome(StoreOutcome::Committed(admission.facts)),
        });
        let answer = world.drain();
        assert_eq!(kinds(&answer), ["EmitPubrec"]);
        assert_eq!(answer[0].1, frame_to(CONN, pubrec(AckReason::Success)));
        assert_eq!(world.phase(7), AWAIT);
        // The held answer arrives: knowledge stays committed, no effect
        // and no frame follow. The third deadline is stale.
        absorbed(&mut world, held);
        let Effect::ArmDeadline { token: third, .. } = resolution[1].1 else {
            panic!("a deadline")
        };
        absorbed(&mut world, deadline(third));
        // PUBREL, release, PUBCOMP.
        let release = world.submit(pubrel(7));
        assert_eq!(world.sent(committed(&release)), ["EmitPubcomp"]);
        assert_eq!(world.phase(7), None);
        assert_eq!(world.flights.len(), 2, "one admission, one release");
    }

    // P1-02: the admission commits, its result is lost and the broker
    // crashes. The next run finds the exchange in the durable data,
    // answers the replayed PUBLISH with PUBREC without a second
    // admission, and terminates the exchange.
    #[test]
    fn p1_02_an_admission_whose_result_was_lost_is_found_after_a_crash() {
        let mut before = World::connected();
        let admission = before.submit(publish(7));
        // No PUBREC before the crash: the commit was never reported.
        assert!(before.outbox().is_empty());
        assert_eq!(before.phase(7), PENDING);
        let CommitFacts::PublicationAdmitted {
            publication,
            exchange,
        } = admission.facts
        else {
            panic!("an admission")
        };

        // The next run epoch reads what the store kept.
        let mut facts = fixture(EPOCH + 1);
        facts.push(RecoveredFact::InboundExchange {
            exchange,
            packet_id: packet_id(7),
            publication,
            phase: InboundPhase::AwaitPubrel,
        });
        facts.push(RecoveredFact::Operation {
            operation: admission.operation,
            knowledge: Knowledge::Committed(admission.facts),
        });
        let mut world = World::cold(EPOCH + 1, CAPACITY);
        world.applied(page(&facts, true));
        assert_eq!(world.sent(connect(CONN, b"p1-producer")), ["EmitConnack"]);
        assert_eq!(world.phase(7), AWAIT);
        assert_eq!(
            world.owner.exchange(SessionId::new(PRODUCER), packet_id(7)),
            Some((exchange, ExchangePhase::AwaitPubrel))
        );

        // The client resumes the exchange: PUBREC, and no admission.
        world.applied(publish(7));
        let answer = world.drain();
        assert_eq!(kinds(&answer), ["EmitPubrec"]);
        assert_eq!(answer[0].1, frame_to(CONN, pubrec(AckReason::Success)));
        let release = world.submit(pubrel(7));
        assert_eq!(release.facts, CommitFacts::InboundReleased { exchange });
        assert_eq!(release.operation.epoch(), RunEpoch::new(EPOCH + 1));
        assert_eq!(world.sent(committed(&release)), ["EmitPubcomp"]);
        assert_eq!(world.phase(7), None);
        assert_eq!(world.flights, [release], "no admission in the second run");
        // A new exchange of the session never repeats the generation.
        let _ = world.submit(publish(7));
        let (next, _) = world
            .owner
            .exchange(SessionId::new(PRODUCER), packet_id(7))
            .expect("an exchange");
        assert!(next.generation > exchange.generation);
    }

    // ----- Admission: recipe, limits and credits -----

    #[test]
    fn an_admission_freezes_a_recipe_from_the_recovered_routes_and_limits() {
        let mut world = World::connected();
        world.applied(publish(7));
        let effects = world.drain();
        let (_, Effect::Persist { command }) = &effects[0] else {
            panic!("a command")
        };
        let incarnation = StoreIncarnation::new(INCARNATION);
        let epoch = RunEpoch::new(EPOCH);
        let owner = LogicalOwnerId::new(4);
        let publication = PublicationId::new(incarnation, epoch, owner, PublicationSeq::new(1));
        let target = |session: u64, index: u32| Target {
            session: SessionId::new(session),
            session_revision: Revision::new(session + 10),
            delivery: DeliveryId { publication, index },
            effective_qos: Qos::AtLeastOnce,
            cause: DeliveryCause::LiveExact,
        };
        let expected = Recipe::AdmitPublication(AdmitPublication {
            operation: OperationId::new(incarnation, epoch, owner, OperationSeq::new(1)),
            request: RequestId::new(incarnation, epoch, owner, RequestSeq::new(1)),
            publication,
            source: Source {
                session: SessionId::new(PRODUCER),
                session_revision: Revision::new(11),
                packet_id: packet_id(7),
                exchange: ExchangeId {
                    session: SessionId::new(PRODUCER),
                    generation: ExchangeGeneration::new((EPOCH << 32) | 1),
                },
            },
            route_revision: Revision::new(3),
            qos: Qos::ExactlyOnce,
            retain: false,
            topic: bytes(b"p1/example"),
            payload: PayloadRef {
                id: PayloadId::new(incarnation, epoch, owner, PayloadSeq::new(1)),
                length: PayloadBytes::new(7),
            },
            costs: Costs {
                payload_bytes: PayloadBytes::new(7),
                references: References::new(2),
                exchange_slots: ExchangeSlots::new(1),
                receipt_slots: ReceiptSlots::new(1),
            },
            targets: BoundedVec::from_slice(&[target(2, 0), target(3, 1)], 2).expect("fits"),
        });
        assert_eq!(command.recipe.get(), &expected);
        assert_eq!(command.payload, Some(bytes(b"payload")));
    }

    #[test]
    fn a_topic_without_a_route_is_admitted_with_no_target() {
        let mut world = World::connected();
        world.applied(Input::Packet {
            connection: ConnectionGeneration::new(CONN),
            ingress_order: 0,
            packet: InboundPacket::Publish {
                topic: bytes(b"p1/exampl"),
                qos: Qos::ExactlyOnce,
                retain: false,
                dup: true,
                packet_id: Some(packet_id(7)),
                payload: bytes(b""),
            },
        });
        let effects = world.drain();
        let (_, Effect::Persist { command }) = &effects[0] else {
            panic!("a command")
        };
        let Recipe::AdmitPublication(admit) = command.recipe.get() else {
            panic!("an admission")
        };
        assert!(admit.targets.is_empty());
        assert_eq!(admit.costs.references, References::ZERO);
        assert_eq!(admit.costs.payload_bytes, PayloadBytes::ZERO);
        assert_eq!(used(&world), (0, 1));
    }

    #[test]
    fn an_admission_waits_for_the_credits_of_its_owner() {
        let need = |resource, amount| CapacityNeed {
            resource,
            amount,
            wake: WakeCondition::ResourceReleased,
        };
        // Payload bytes: 16 for the owner, 7 for each exchange.
        let mut world = World::connected();
        let first = world.submit(publish(7));
        let _ = world.submit(publish_with(CONN, 8, Qos::ExactlyOnce, b"abc"));
        let before = world.snapshot();
        assert_eq!(
            world.step(publish(9)),
            StepOutcome::NeedCapacity {
                input: publish(9),
                need: need(Resource::PayloadBytes, 1),
                work_used: WorkUnits::new(1),
            }
        );
        assert_eq!(world.snapshot(), before);
        // Exchange slots: two for the owner.
        let small = || publish_with(CONN, 9, Qos::ExactlyOnce, b"a");
        assert_eq!(
            world.step(small()),
            StepOutcome::NeedCapacity {
                input: small(),
                need: need(Resource::ExchangeSlots, 1),
                work_used: WorkUnits::new(1),
            }
        );
        assert_eq!(world.snapshot(), before);
        // The end of an exchange is the release that was waited for.
        assert_eq!(world.sent(rejected(&first)), ["EmitPubrecRefusal"]);
        let _ = world.submit(publish(9));
        assert_eq!(used(&world), (10, 2));
    }

    #[test]
    fn a_publish_no_credit_can_ever_cover_closes_its_connection() {
        let never = Effect::CloseConnection {
            connection: ConnectionGeneration::new(CONN),
            reason: CloseReason::CapacityExceeded,
        };
        let topic = |topic: &[u8]| Input::Packet {
            connection: ConnectionGeneration::new(CONN),
            ingress_order: 0,
            packet: InboundPacket::Publish {
                topic: bytes(topic),
                qos: Qos::ExactlyOnce,
                retain: false,
                dup: false,
                packet_id: Some(packet_id(7)),
                payload: bytes(b"payload"),
            },
        };
        let cases: [(&str, OwnerConfig, Input); 3] = [
            (
                "a payload above the limit of the store",
                config(),
                publish_with(CONN, 7, Qos::ExactlyOnce, b"123456789"),
            ),
            (
                "a topic above the limit of the store",
                config(),
                topic(b"p1/example/longer"),
            ),
            (
                "a payload above the account of the owner",
                OwnerConfig {
                    payload_bytes: PayloadBytes::new(6),
                    ..config()
                },
                publish(7),
            ),
        ];
        for (case, config, input) in cases {
            let mut world = World::with(config, EPOCH, CAPACITY);
            world.applied(page(&fixture(EPOCH), true));
            assert_eq!(world.sent(connect(CONN, b"p1-producer")), ["EmitConnack"]);
            world.applied(input);
            let effects = world.drain();
            assert_eq!(effects.len(), 1, "{case}");
            assert_eq!(effects[0].1, never, "{case}");
            assert_eq!(world.phase(7), None, "{case}");
            assert_eq!(used(&world), (0, 0), "{case}");
            assert_eq!(world.owner.connection_of(SessionId::new(PRODUCER)), None);
        }
        // The limits themselves are admitted.
        let mut world = World::connected();
        let _ = world.submit(publish_with(CONN, 7, Qos::ExactlyOnce, b"12345678"));
        // More targets than an admission may have.
        let mut facts = fixture(EPOCH);
        facts[1] = RecoveredFact::Limits {
            max_targets_per_admission: 1,
            max_topic_bytes: 16,
            max_payload_bytes: PayloadBytes::new(8),
        };
        let mut world = World::cold(EPOCH, CAPACITY);
        world.applied(page(&facts, true));
        assert_eq!(world.sent(connect(CONN, b"p1-producer")), ["EmitConnack"]);
        world.applied(publish(7));
        assert_eq!(world.drain()[0].1, never);
    }

    // ----- Results the store cannot give -----

    #[test]
    fn a_commit_with_the_facts_of_another_exchange_is_a_fault() {
        let mut world = World::admission_pending();
        let admission = world.flights[0];
        let CommitFacts::PublicationAdmitted {
            publication,
            exchange,
        } = admission.facts
        else {
            panic!("an admission")
        };
        let other = ExchangeId {
            session: exchange.session,
            generation: exchange
                .generation
                .checked_next()
                .expect("far from the end"),
        };
        let fault = CoreFault::ContradictoryOutcome {
            operation: admission.operation,
        };
        let claim = CommitFacts::PublicationAdmitted {
            publication,
            exchange: other,
        };
        faulted(
            &mut world,
            result(&admission, StoreOutcome::Committed(claim)),
            fault,
        );
        let mut world = World::release_pending();
        let release = world.flights[1];
        let fault = CoreFault::ContradictoryOutcome {
            operation: release.operation,
        };
        let claim = CommitFacts::InboundReleased { exchange: other };
        faulted(
            &mut world,
            result(&release, StoreOutcome::Committed(claim)),
            fault,
        );
        faulted(
            &mut world,
            result(&release, StoreOutcome::Committed(admission.facts)),
            fault,
        );
    }

    // MU-03: one identity, one recipe.
    #[test]
    fn a_recipe_mismatch_reported_by_the_store_is_a_fault() {
        let fault = |flight: &Flight| CoreFault::RecipeMismatch {
            operation: flight.operation,
        };
        let mut world = World::admission_pending();
        let admission = world.flights[0];
        faulted(&mut world, mismatch(&admission), fault(&admission));
        // Also for the consumed admission of a live exchange.
        let mut world = World::release_pending();
        faulted(&mut world, mismatch(&admission), fault(&admission));
        let release = world.flights[1];
        faulted(&mut world, mismatch(&release), fault(&release));
        // Also for an operation that ended, while the owner retains it.
        let mut world = World::ended();
        faulted(&mut world, mismatch(&release), fault(&release));
    }

    #[test]
    fn a_result_of_an_effect_the_owner_never_emitted_is_a_fault() {
        let mut world = World::admission_pending();
        let admission = world.flights[0];
        let next = world.owner.counters.effect.expect("far from the end");
        let with = |effect: EffectId| Input::StoreResult {
            effect,
            operation: admission.operation,
            reply: StoreReply::Outcome(StoreOutcome::Committed(admission.facts)),
        };
        for effect in [
            EffectId {
                sequence: next,
                ..admission.effect
            },
            EffectId {
                epoch: RunEpoch::new(EPOCH - 1),
                ..admission.effect
            },
            EffectId {
                owner: LogicalOwnerId::new(5),
                ..admission.effect
            },
        ] {
            faulted(&mut world, with(effect), CoreFault::ImpossibleInput);
        }
        assert_eq!(world.sent(with(admission.effect)), ["EmitPubrec"]);
    }

    // ADR 0001: the commit is reconciled even when no connection is
    // current, and the answer follows the replay after the reconnect.
    #[test]
    fn a_commit_without_a_connection_is_reconciled_and_answered_after_the_reconnect() {
        let mut world = World::admission_pending();
        assert_eq!(world.sent(pubrel(7)), ["CloseConnection"]);
        let admission = world.flights[0];
        world.applied(committed(&admission));
        assert!(world.outbox().is_empty());
        assert_eq!(world.phase(7), AWAIT);
        assert_eq!(world.sent(connect(11, b"p1-producer")), ["EmitConnack"]);
        world.applied(publish_with(11, 7, Qos::ExactlyOnce, b"payload"));
        assert_eq!(world.drain()[0].1, frame_to(11, pubrec(AckReason::Success)));
        // The same without a connection at the end of the exchange.
        let mut world = World::release_pending();
        assert_eq!(world.sent(publish(7)), ["CloseConnection"]);
        world.applied(committed(&world.flights[1]));
        assert!(world.outbox().is_empty());
        assert_eq!(world.phase(7), None);
        assert_eq!(used(&world), (0, 0));
    }

    #[test]
    fn a_wake_up_creates_nothing_and_no_continuation_exists() {
        let mut world = World::admission_pending();
        absorbed(
            &mut world,
            Input::WorkAvailable {
                session: SessionId::new(PRODUCER),
            },
        );
        let resume = Input::Continue {
            continuation: crate::contract::ids::ContinuationId::new(1),
            budget: WorkUnits::new(1),
        };
        faulted(&mut world, resume, CoreFault::ImpossibleInput);
    }

    #[test]
    fn a_recovery_page_larger_than_the_work_limit_is_handed_back() {
        let mut world = World::cold(EPOCH, CAPACITY);
        let before = world.snapshot();
        let short = TurnGrant {
            work_limit: WorkUnits::new(FIXTURE_WORK - 1),
        };
        assert_eq!(
            world.owner.step(page(&fixture(EPOCH), true), short),
            StepOutcome::NeedCapacity {
                input: page(&fixture(EPOCH), true),
                need: CapacityNeed {
                    resource: Resource::Work,
                    amount: 1,
                    wake: WakeCondition::TurnGranted,
                },
                work_used: WorkUnits::ZERO,
            }
        );
        assert_eq!(world.snapshot(), before);
        let exact = TurnGrant {
            work_limit: WorkUnits::new(FIXTURE_WORK),
        };
        let outcome = world.owner.step(page(&fixture(EPOCH), true), exact);
        assert!(
            matches!(outcome, StepOutcome::Applied { .. }),
            "{outcome:?}"
        );
    }

    // ----- Review of the core at fe26568 -----

    /// The work of the fixture page: three facts of one unit, two routes
    /// that each compare with the route table, three sessions that each
    /// compare with the session table, and the check that closes the
    /// inventory, every route against every session (config: 2 routes,
    /// 3 sessions, 2 exchanges).
    const FIXTURE_WORK: u64 = 3 + 2 * (1 + 2) + 3 * (1 + 3) + (1 + 2 * 3);

    // Finding 7: the work counted for a page bounds the comparisons its
    // checks make, which grow with the tables, not only with the page.
    #[test]
    fn the_work_of_a_recovery_page_bounds_every_comparison_it_makes() {
        let mut world = World::cold(EPOCH, CAPACITY);
        let mut facts = fixture(EPOCH);
        facts.push(exchange_fact(1, 7, 1));
        facts.push(operation_fact(1));
        assert_eq!(
            world.step(page(&facts, true)),
            StepOutcome::Applied {
                // An exchange compares with the sessions and the exchanges.
                work_used: WorkUnits::new(FIXTURE_WORK + (1 + 3 + 2) + 1),
                progress: Progress::Complete,
            }
        );
        // A page that does not close the inventory does not pay for the
        // check that closes it.
        let mut world = World::cold(EPOCH, CAPACITY);
        assert_eq!(
            world.step(page(&fixture(EPOCH), false)),
            StepOutcome::Applied {
                work_used: WorkUnits::new(FIXTURE_WORK - (1 + 2 * 3)),
                progress: Progress::Complete,
            }
        );
    }

    // inbound_qos2.release_pending.rejected.definitive.stop: a release
    // refused for a reason that a new preparation cannot change is not
    // prepared again; the owner stops, publishes nothing and changes
    // nothing (finding 1 of the review of the core).
    #[test]
    fn rule_inbound_qos2_release_pending_rejected_definitive_stop() {
        for reason in [
            RejectReason::PreconditionFailed,
            RejectReason::EpochClosed,
            RejectReason::CapacityExceeded(Resource::ExchangeSlots),
        ] {
            let mut world = World::release_pending();
            let release = world.flights[1];
            let refusal = result(&release, StoreOutcome::RejectedCertain(reason));
            let fault = CoreFault::ReleaseRefused {
                operation: release.operation,
                reason,
            };
            faulted(&mut world, refusal, fault);
        }
    }

    // inbound_qos2.release_pending.rejected.fenced_at_bound.stop: a fenced
    // release is prepared again until the bound of preparations; one more
    // fence stops the owner, which publishes nothing and changes nothing
    // (finding 1 of the review of the core).
    #[test]
    fn rule_inbound_qos2_release_pending_rejected_fenced_at_bound_stop() {
        let mut world = World::release_pending();
        let mut release = world.flights[1];
        for _ in 1..MAX_RELEASE_PREPARATIONS {
            release = world.submit(fenced(&release));
        }
        let preparations = usize::try_from(MAX_RELEASE_PREPARATIONS).expect("small");
        assert_eq!(world.flights.len(), 1 + preparations);
        let fault = CoreFault::ReleaseRefused {
            operation: release.operation,
            reason: RejectReason::Fenced,
        };
        faulted(&mut world, fenced(&release), fault);
    }

    // Finding 2: a terminal result that contradicts an operation which no
    // exchange holds any more is a fault, not a stale answer.
    #[test]
    fn a_late_result_that_contradicts_an_operation_that_ended_is_a_fault() {
        let contradiction = |flight: &Flight| CoreFault::ContradictoryOutcome {
            operation: flight.operation,
        };
        let recipe = |flight: &Flight| CoreFault::RecipeMismatch {
            operation: flight.operation,
        };
        // An admission refused, then reported committed: a publication the
        // client was refused is in fact admitted.
        let mut world = refused();
        let admission = world.flights[0];
        faulted(&mut world, committed(&admission), contradiction(&admission));
        faulted(&mut world, mismatch(&admission), recipe(&admission));
        // An exchange that ended, then its admission or release refused.
        let mut world = World::ended();
        for flight in world.flights.clone() {
            faulted(&mut world, rejected(&flight), contradiction(&flight));
            faulted(&mut world, mismatch(&flight), recipe(&flight));
        }
        // A release refused and prepared again, then reported committed.
        let mut world = World::release_pending();
        let first = world.flights[1];
        let _ = world.submit(fenced(&first));
        faulted(&mut world, committed(&first), contradiction(&first));
    }

    // Finding 3: a result is matched by the effect the owner emitted for
    // the operation, not by the operation alone.
    #[test]
    fn a_result_is_matched_to_its_operation_by_the_effect_that_asked_for_it() {
        let mut world = World::connected();
        let admission = world.submit(publish(7));
        let effect = |sequence: u64| EffectId {
            sequence: EffectSeq::new(sequence),
            ..admission.effect
        };
        // The CONNACK of the session and the deadline of the admission.
        let connack = effect(1);
        let arm = effect(admission.effect.sequence.get() + 1);
        for answer in [connack, arm] {
            let input = Input::StoreResult {
                effect: answer,
                operation: admission.operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(admission.facts)),
            };
            absorbed(&mut world, input);
            assert_eq!(world.phase(7), PENDING);
        }
        assert_eq!(world.sent(committed(&admission)), ["EmitPubrec"]);
    }

    // Finding 4: a connection the owner ordered closed resumes nothing.
    #[test]
    fn a_connection_the_owner_closed_resumes_nothing() {
        let mut world = World::connected();
        assert_eq!(
            world.sent(connect(11, b"p1-producer")),
            ["CloseConnection", "EmitConnack"]
        );
        absorbed(&mut world, connect(CONN, b"p1-producer"));
        assert_eq!(
            world.owner.connection_of(SessionId::new(PRODUCER)),
            Some(ConnectionGeneration::new(11))
        );
        // Closed for a protocol error.
        let mut world = World::connected();
        assert_eq!(
            world.sent(connect(CONN, b"p1-producer")),
            ["CloseConnection"]
        );
        absorbed(&mut world, connect(CONN, b"p1-producer"));
        assert_eq!(world.owner.connection_of(SessionId::new(PRODUCER)), None);
    }

    // Finding 6: an allocation that fails while planning stops the owner;
    // no wake-up gives memory back.
    #[test]
    fn a_failed_allocation_while_planning_is_a_fault() {
        assert_eq!(
            Stop::from(AllocationFailed),
            Stop::Fatal(CoreFault::AllocationFailed)
        );
    }

    // The limits of the windows, stated: the owner retains the latest
    // resolutions of an operation, the latest operations that ended and
    // the latest connections it closed, and nothing before them.

    #[test]
    fn an_answer_to_a_resolution_the_owner_no_longer_retains_is_not_matched() {
        let mut world = World::admission_pending();
        let admission = world.flights[0];
        let mut token = admission.deadline;
        let mut resolutions = Vec::new();
        for _ in 0..5 {
            world.applied(deadline(token));
            let effects = world.drain();
            let Effect::ArmDeadline { token: next, .. } = effects[1].1 else {
                panic!("a deadline")
            };
            resolutions.push(effects[0].0);
            token = next;
        }
        let answer = |effect: EffectId| Input::StoreResult {
            effect,
            operation: admission.operation,
            reply: StoreReply::Outcome(StoreOutcome::Committed(admission.facts)),
        };
        // Ranks 1 to 5: the command, the two latest (4, 5) and the
        // landmarks (1, 2, 4) are retained; rank 3 is not.
        let retained = world.in_flight(7).answers;
        assert!(retained.names(admission.effect));
        for (rank, effect) in (1..).zip(&resolutions) {
            assert_eq!(retained.names(*effect), rank != 3, "rank {rank}");
        }
        // Its answer is absorbed, and a retained one answers.
        absorbed(&mut world, answer(resolutions[2]));
        assert_eq!(world.phase(7), PENDING);
        assert_eq!(world.sent(answer(resolutions[0])), ["EmitPubrec"]);
    }

    // F3, the bound: an operation never retains more than the command, the
    // recent resolutions and the landmarks, and the landmarks are the
    // latest resolutions whose rank is a power of two.
    #[test]
    fn an_operation_retains_a_bounded_number_of_answers() {
        let effect = |sequence: u64| EffectId {
            epoch: RunEpoch::new(EPOCH),
            owner: LogicalOwnerId::new(4),
            sequence: EffectSeq::new(sequence),
        };
        let mut answers = Answers::new(effect(0));
        for rank in 1..=1000u64 {
            answers = answers.with(effect(rank));
            let held = (1..=rank).filter(|r| answers.names(effect(*r))).count();
            assert!(
                held <= RECENT_RESOLUTIONS + LANDMARK_RESOLUTIONS,
                "rank {rank}"
            );
            assert!(answers.names(effect(0)));
            assert!(answers.names(effect(rank)));
            let powers: Vec<u64> = (0..64)
                .map(|bit| 1u64 << bit)
                .filter(|power| *power <= rank)
                .collect();
            for (index, power) in powers.iter().rev().enumerate() {
                let recent = rank - *power < u64::try_from(RECENT_RESOLUTIONS).expect("small");
                assert_eq!(
                    answers.names(effect(*power)),
                    index < LANDMARK_RESOLUTIONS || recent,
                    "rank {rank}, landmark {power}"
                );
            }
        }
    }

    #[test]
    fn an_operation_that_ended_before_the_retained_ones_is_no_longer_seen() {
        let config = OwnerConfig {
            retained_operations: bound(1),
            ..config()
        };
        let mut world = World::with(config, EPOCH, CAPACITY);
        world.applied(page(&fixture(EPOCH), true));
        assert_eq!(world.sent(connect(CONN, b"p1-producer")), ["EmitConnack"]);
        let admission = world.submit(publish(7));
        assert_eq!(world.sent(committed(&admission)), ["EmitPubrec"]);
        let release = world.submit(pubrel(7));
        assert_eq!(world.sent(committed(&release)), ["EmitPubcomp"]);
        // The release ended last and is the one retained; the admission
        // before it is no longer seen.
        let fault = CoreFault::ContradictoryOutcome {
            operation: release.operation,
        };
        faulted(&mut world, rejected(&release), fault);
        absorbed(&mut world, rejected(&admission));
    }

    #[test]
    fn a_connection_closed_before_the_remembered_ones_is_no_longer_known_as_closed() {
        let config = OwnerConfig {
            remembered_connections: bound(1),
            ..config()
        };
        let mut world = World::with(config, EPOCH, CAPACITY);
        world.applied(page(&fixture(EPOCH), true));
        assert_eq!(world.sent(connect(CONN, b"p1-producer")), ["EmitConnack"]);
        for next in [11, 12] {
            assert_eq!(
                world.sent(connect(next, b"p1-producer")),
                ["CloseConnection", "EmitConnack"]
            );
        }
        absorbed(&mut world, connect(11, b"p1-producer"));
        // The connection closed first is forgotten: the adapter must not
        // deliver what it carries after the owner closed it this long ago.
        assert_eq!(
            world.sent(connect(CONN, b"p1-producer")),
            ["CloseConnection", "EmitConnack"]
        );
    }

    // Finding 3, for an operation that ended: only its own effects answer
    // it, so a result naming another effect is not a contradiction.
    #[test]
    fn a_late_result_is_matched_to_its_operation_by_the_effect_that_asked_for_it() {
        let mut world = refused();
        let admission = world.flights[0];
        let foreign = Input::StoreResult {
            effect: EffectId {
                sequence: EffectSeq::new(1),
                ..admission.effect
            },
            operation: admission.operation,
            reply: StoreReply::Outcome(StoreOutcome::Committed(admission.facts)),
        };
        absorbed(&mut world, foreign);
        let fault = CoreFault::ContradictoryOutcome {
            operation: admission.operation,
        };
        faulted(&mut world, committed(&admission), fault);
    }

    // Finding 5, across pages: a route read on an earlier page counts.
    #[test]
    fn a_route_read_twice_across_pages_is_a_fault() {
        let facts = fixture(EPOCH);
        let mut world = World::cold(EPOCH, CAPACITY);
        world.applied(page(&facts[..4], false));
        let before = world.snapshot();
        assert_eq!(
            world.step(page(&[route_fact(b"p1/example", 2)], false)),
            StepOutcome::Fatal(CoreFault::ImpossibleInput)
        );
        assert_eq!(world.snapshot(), before);
        world.applied(page(&facts[4..], true));
    }

    // Finding 7: the facts a check compares with are those of one kind,
    // the run they form on the page, and nothing of a later kind.
    #[test]
    fn the_facts_of_one_kind_are_the_run_they_form_on_a_page() {
        let mut facts = fixture(EPOCH);
        facts.push(exchange_fact(1, 7, 1));
        let mut starts = [None; 7];
        for (position, fact) in facts.iter().enumerate() {
            let start = &mut starts[fact.rank() as usize];
            if start.is_none() {
                *start = Some(position);
            }
        }
        let end = facts.len();
        assert_eq!(run(&facts, &starts, FactRank::Route, end), &facts[3..5]);
        assert_eq!(run(&facts, &starts, FactRank::Session, end), &facts[5..8]);
        assert_eq!(run(&facts, &starts, FactRank::Session, 6), &facts[5..6]);
        assert_eq!(
            run(&facts, &starts, FactRank::InboundExchange, end),
            &facts[8..]
        );
        assert!(run(&facts, &starts, FactRank::Operation, end).is_empty());
    }

    // ----- Audit of the first wave -----

    // F4: no path builds two owners of one identity. Two owners of one
    // identity would issue the same identities from fresh counters, so two
    // different publications could receive the same `OperationId`. An
    // owner takes its box, and the authority creates no second box for an
    // identity that is active, also once the first box was dropped without
    // being retired (ADR 0009).
    #[test]
    fn two_owners_of_one_identity_never_issue_one_operation_id() {
        let mut authority = authority(INCARNATION, EPOCH);
        let create = |authority: &mut OwnerAuthority, owner: u64| {
            authority.create_outbox(
                LogicalOwnerId::new(owner),
                nonzero(CAPACITY),
                bound(ENTRIES),
            )
        };
        let outbox = create(&mut authority, 4).expect("first box");
        let first = Owner::new(config(), outbox).expect("large enough");
        assert_eq!(
            create(&mut authority, 4).err(),
            Some(CreateRefused::OwnerAlreadyActive)
        );
        drop(first);
        assert_eq!(
            create(&mut authority, 4).err(),
            Some(CreateRefused::OwnerAlreadyActive),
            "a box dropped without retirement leaves its owner active"
        );
        // Owners of two identities issue two identity spaces.
        let mut authority = authority_of_owners(2);
        let mut operations = Vec::new();
        for (raw, payload) in [(5, &b"one"[..]), (6, &b"two"[..])] {
            let outbox = create(&mut authority, raw).expect("a box");
            let mut owner = Owner::new(config(), outbox).expect("large enough");
            for input in [
                page(&fixture(EPOCH), true),
                connect(CONN, b"p1-producer"),
                publish_with(CONN, 7, Qos::ExactlyOnce, payload),
            ] {
                let outcome = owner.step(input, GRANT);
                assert!(
                    matches!(outcome, StepOutcome::Applied { .. }),
                    "{outcome:?}"
                );
            }
            let mut sink = Taking(Vec::new());
            let _ = owner.dispatch(&mut sink);
            for effect in sink.0 {
                if let (_, Effect::Persist { command }) = effect.into_parts() {
                    operations.push(command.recipe.operation());
                }
            }
        }
        assert_eq!(operations.len(), 2);
        assert_ne!(operations[0], operations[1], "one identity, two recipes");
    }

    /// Drives one admission whose first `Persist` committed and whose
    /// result was lost. Each deadline emits one resolution, every
    /// resolution answers Committed, and the answer of a resolution is
    /// delivered once `lag` newer resolutions were emitted, in order. The
    /// answers whose rank is in `lost` are never delivered, those in
    /// `doubled` are delivered twice. Returns the number of resolutions
    /// emitted when PUBREC was published, or `None` after `limit`.
    fn resolve_with_lag(
        lag: usize,
        lost: &[usize],
        doubled: &[usize],
        limit: usize,
    ) -> Option<usize> {
        let mut world = World::admission_pending();
        let admission = world.flights[0];
        let mut token = admission.deadline;
        let mut pending: std::collections::VecDeque<(usize, EffectId)> = Default::default();
        for emitted in 1..=limit {
            world.applied(deadline(token));
            let effects = world.drain();
            assert_eq!(kinds(&effects), ["ResolveAndFence", "ArmDeadline"]);
            let Effect::ArmDeadline { token: next, .. } = effects[1].1 else {
                panic!("a deadline")
            };
            token = next;
            pending.push_back((emitted, effects[0].0));
            while pending.len() > lag {
                let (rank, effect) = pending.pop_front().expect("one");
                if lost.contains(&rank) {
                    continue;
                }
                let times = if doubled.contains(&rank) { 2 } else { 1 };
                for _ in 0..times {
                    world.applied(Input::StoreResult {
                        effect,
                        operation: admission.operation,
                        reply: StoreReply::Outcome(StoreOutcome::Committed(admission.facts)),
                    });
                    let answer = world.drain();
                    if world.phase(7) == AWAIT {
                        assert_eq!(kinds(&answer), ["EmitPubrec"]);
                        return Some(emitted);
                    }
                    assert!(answer.is_empty());
                }
            }
            assert_eq!(world.phase(7), PENDING);
        }
        None
    }

    // F3, the counterexample of the audit: every answer arrives after four
    // newer resolutions. The exchange must leave AdmissionPending.
    #[test]
    fn resolutions_whose_answers_arrive_after_four_newer_ones_still_commit() {
        assert!(resolve_with_lag(4, &[], &[], 24).is_some());
    }

    // F3: progress under any finite lag, with losses and duplicates.
    #[test]
    fn resolutions_progress_under_a_finite_lag_with_losses_and_duplicates() {
        for lag in [0, 1, 3, 4, 5, 7, 20, 100] {
            assert!(
                resolve_with_lag(lag, &[], &[], 4 * (lag + 2)).is_some(),
                "lag {lag}"
            );
            let lost: Vec<usize> = (1..=lag.max(2) + 2).collect();
            assert!(
                resolve_with_lag(lag, &lost, &[1, 2, 3], 8 * (lag + 4)).is_some(),
                "lag {lag}, losses {lost:?}"
            );
        }
    }

    // F3: a resolution answers only its own operation. The answer of a
    // resolution of another exchange, named with this operation, is not
    // applied to it.
    #[test]
    fn a_resolution_of_another_operation_does_not_answer_this_one() {
        let mut world = World::connected();
        let first = world.submit(publish(7));
        let second = world.submit(publish_with(CONN, 8, Qos::ExactlyOnce, b"abc"));
        world.applied(deadline(second.deadline));
        let effects = world.drain();
        assert_eq!(kinds(&effects), ["ResolveAndFence", "ArmDeadline"]);
        let foreign = Input::StoreResult {
            effect: effects[0].0,
            operation: first.operation,
            reply: StoreReply::Outcome(StoreOutcome::Committed(first.facts)),
        };
        absorbed(&mut world, foreign);
        assert_eq!(world.phase(7), PENDING);
        assert_eq!(world.sent(committed(&first)), ["EmitPubrec"]);
    }

    // F3, I3: the answer of the first landmark p >= k is lost; the next
    // landmark, 2p, makes the exchange progress. With a lag of 4 and every
    // answer below rank 8 lost, the landmark 8 answers by rank 12; with
    // every answer below rank 16 lost, the landmark 16 answers by rank 20.
    #[test]
    fn the_next_landmark_progresses_when_the_answer_of_one_is_lost() {
        let below = |rank: usize| (1..rank).collect::<Vec<_>>();
        assert_eq!(resolve_with_lag(4, &below(8), &[], 64), Some(12));
        assert_eq!(resolve_with_lag(4, &below(16), &[], 64), Some(20));
        // And with the lag of the counterexample, nothing lost, the first
        // landmark answers as soon as its answer arrives.
        assert_eq!(resolve_with_lag(4, &[], &[], 24), Some(5));
    }

    /// Like `resolve_with_lag`, with every answer delivered, but in an
    /// order that is not the order of the resolutions: once more than `lag`
    /// answers wait, one of the older half of them, chosen by `seed`, is
    /// delivered. The answers delivered are rarely among the latest, so the
    /// recent resolutions alone do not make progress.
    fn resolve_out_of_order(lag: usize, seed: u64, limit: usize) -> Option<usize> {
        let mut world = World::admission_pending();
        let admission = world.flights[0];
        let mut token = admission.deadline;
        let mut waiting: Vec<EffectId> = Vec::new();
        let mut state = seed;
        for emitted in 1..=limit {
            world.applied(deadline(token));
            let effects = world.drain();
            let Effect::ArmDeadline { token: next, .. } = effects[1].1 else {
                panic!("a deadline")
            };
            token = next;
            waiting.push(effects[0].0);
            while waiting.len() > lag {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let older = waiting.len().div_ceil(2);
                let pick = usize::try_from(state >> 33).expect("small") % older;
                let effect = waiting.remove(pick);
                world.applied(Input::StoreResult {
                    effect,
                    operation: admission.operation,
                    reply: StoreReply::Outcome(StoreOutcome::Committed(admission.facts)),
                });
                if world.phase(7) == AWAIT {
                    assert_eq!(kinds(&world.drain()), ["EmitPubrec"]);
                    return Some(emitted);
                }
                assert!(world.drain().is_empty());
            }
        }
        None
    }

    // F3: every answer arrives, out of order, each after a finite number of
    // newer resolutions.
    #[test]
    fn resolutions_progress_when_their_answers_arrive_out_of_order() {
        for lag in [1, 4, 7, 20] {
            for seed in 0..16 {
                assert!(
                    resolve_out_of_order(lag, seed, 8 * (lag + 2)).is_some(),
                    "lag {lag}, seed {seed}"
                );
            }
        }
    }

    // F3, I4: a resolution answers RejectedCertain(Fenced), the admission
    // ends refused, then an earlier resolution still retained answers
    // Committed. Two terminal results disagree: the owner stops.
    #[test]
    fn a_late_commit_after_a_refusal_of_one_admission_is_a_fault() {
        let mut world = World::admission_pending();
        let admission = world.flights[0];
        let mut token = admission.deadline;
        let mut resolutions = Vec::new();
        for _ in 0..2 {
            world.applied(deadline(token));
            let effects = world.drain();
            let Effect::ArmDeadline { token: next, .. } = effects[1].1 else {
                panic!("a deadline")
            };
            resolutions.push(effects[0].0);
            token = next;
        }
        let answer = |effect: EffectId, outcome: StoreOutcome| Input::StoreResult {
            effect,
            operation: admission.operation,
            reply: StoreReply::Outcome(outcome),
        };
        let fenced = StoreOutcome::RejectedCertain(RejectReason::Fenced);
        assert_eq!(
            world.sent(answer(resolutions[1], fenced)),
            ["EmitPubrecRefusal"]
        );
        assert_eq!(world.phase(7), None);
        let late = answer(resolutions[0], StoreOutcome::Committed(admission.facts));
        let fault = CoreFault::ContradictoryOutcome {
            operation: admission.operation,
        };
        faulted(&mut world, late, fault);
    }

    // ----- Review of the second wave: R2 -----

    // R2, the counterexample of the auditor, as given. On its own it also
    // passes when the identity is burnt (`else { return; }`); the tests
    // after it tell an authority kept from an identity burnt.
    #[test]
    fn review_failed_renewal_must_not_reopen_the_owner_identity() {
        let mut world = World::connected();
        let first = world.submit(publish(7));
        assert_eq!(
            world
                .owner
                .renew_box(&mut world.authority, nonzero(usize::MAX), bound(1)),
            Err(RenewRefused::Create(CreateRefused::TooLarge)),
        );
        let Ok(outbox) = world.authority.create_outbox(
            LogicalOwnerId::new(4),
            nonzero(CAPACITY),
            bound(ENTRIES),
        ) else {
            return;
        };
        let mut second = Owner::new(config(), outbox).expect("new owner accepted");
        for input in [
            page(&fixture(EPOCH), true),
            connect(CONN + 1, b"p1-producer"),
            publish_with(CONN + 1, 8, Qos::ExactlyOnce, b"xyz"),
        ] {
            assert!(matches!(
                second.step(input, GRANT),
                StepOutcome::Applied { .. }
            ));
        }
        let mut sink = Taking(Vec::new());
        let _ = second.dispatch(&mut sink);
        let second_operation = sink
            .0
            .into_iter()
            .find_map(|effect| {
                if let (_, Effect::Persist { command }) = effect.into_parts() {
                    Some(command.recipe.operation())
                } else {
                    None
                }
            })
            .expect("second Persist");
        assert_ne!(
            first.operation, second_operation,
            "a failed box renewal reopened the identity and reset its OperationId counter"
        );
    }

    /// A box no allocator provides, though its size fits an allocation.
    fn unobtainable() -> NonZeroUsize {
        nonzero(isize::MAX as usize / core::mem::size_of::<OwnedEffect>())
    }

    // R2, reception 1 to 3: a renewal refused because the new box cannot be
    // created, for its size or for lack of memory, keeps the owner's box,
    // its effects, its counters and its state, keeps the authority as it
    // was, and the identity stays taken: no second owner of it can be
    // built. The owner goes on stepping and dispatching.
    #[test]
    fn a_failed_renewal_keeps_the_box_the_state_and_the_authority() {
        for (capacity, refusal) in [
            (nonzero(usize::MAX), CreateRefused::TooLarge),
            (unobtainable(), CreateRefused::AllocationFailed),
        ] {
            let mut world = World::connected();
            let first = world.submit(publish(7));
            let serial = world.held().id().serial();
            let before = world.snapshot();
            assert_eq!(
                world
                    .owner
                    .renew_box(&mut world.authority, capacity, bound(1)),
                Err(RenewRefused::Create(refusal)),
                "{refusal:?}"
            );
            assert_eq!(world.held().id().serial(), serial, "{refusal:?}");
            assert_eq!(world.snapshot(), before, "{refusal:?}");
            assert_eq!(
                world.authority.active_box(LogicalOwnerId::new(4)),
                Some(world.held().id()),
                "{refusal:?}"
            );
            assert_eq!(
                world
                    .authority
                    .create_outbox(LogicalOwnerId::new(4), nonzero(CAPACITY), bound(ENTRIES))
                    .err(),
                Some(CreateRefused::OwnerAlreadyActive),
                "{refusal:?}"
            );
            // The owner goes on.
            assert_eq!(world.sent(committed(&first)), ["EmitPubrec"], "{refusal:?}");
            let next = world.submit(publish(8));
            assert_eq!(
                next.operation.sequence().get(),
                first.operation.sequence().get() + 1,
                "{refusal:?}"
            );
        }
    }

    // R2, reception 4: after refused renewals, a valid one succeeds and the
    // counters go on.
    #[test]
    fn a_valid_renewal_after_refused_ones_keeps_the_counters() {
        let mut world = World::connected();
        let first = world.submit(publish(7));
        for capacity in [nonzero(usize::MAX), unobtainable()] {
            assert!(
                world
                    .owner
                    .renew_box(&mut world.authority, capacity, bound(1))
                    .is_err()
            );
        }
        let serial = world.held().id().serial();
        assert_eq!(
            world
                .owner
                .renew_box(&mut world.authority, nonzero(CAPACITY), bound(ENTRIES)),
            Ok(())
        );
        assert_ne!(world.held().id().serial(), serial);
        assert_eq!(
            world.authority.active_box(LogicalOwnerId::new(4)),
            Some(world.held().id())
        );
        assert_eq!(world.sent(committed(&first)), ["EmitPubrec"]);
        let next = world.submit(publish(8));
        assert_eq!(
            next.operation.sequence().get(),
            first.operation.sequence().get() + 1
        );
    }
}
