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
//! The owner holds no effect box. The box of each step is handed to it,
//! so the holder of the creation authority may replace a box (ADR 0009)
//! and no counter of the owner starts again.

use core::num::{NonZeroU32, NonZeroUsize};

use crate::command::{CommandEvent, CommandPhase, OutcomeClass, Verdict};
use crate::contract::capacity::{
    AllocationFailed, BoundedVec, CapacityNeed, ExchangeSlots, PayloadBytes, ReceiptSlots,
    References, Resource, WakeCondition, WorkUnits,
};
use crate::contract::command::{
    AdmitPublication, Command, Costs, DeliveryCause, FrozenRecipe, PayloadRef, Recipe,
    ReleaseInbound, Source, Target,
};
use crate::contract::effects::{
    CloseReason, Effect, EffectReservation, FrameProgress, Input, Outbox, OwnedEffect,
    RecoveredFact, RecoveryOrder,
};
use crate::contract::ids::{
    AccountId, ConnectionGeneration, ConnectionKey, DeadlineToken, DeliveryId, EffectId, EffectSeq,
    ExchangeGeneration, ExchangeId, Exhausted, InboundPacketId, LeaseGeneration, LeaseKey,
    LogicalDuration, LogicalOwnerId, OperationId, OperationSeq, PayloadId, PayloadSeq,
    PublicationId, PublicationSeq, RequestId, RequestSeq, Revision, RunEpoch, SessionId,
    StoreIncarnation,
};
use crate::contract::outcome::{CommitFacts, StoreOutcome, StoreReply, StoreViolation};
use crate::contract::packet::{AckReason, InboundPacket, OutboundFrame, Qos, SharedBytes};
use crate::contract::step::{CoreFault, Exhaustion, PlanStamp, Progress, StepOutcome, TurnGrant};
use crate::credits::{Credits, ReleaseOutcome};
use crate::leases::{LeaseRefused, LeaseTable};

/// The largest reservation of effect slots one transition makes. The
/// constructor of an owner refuses a box that is smaller (ADR 0006,
/// amendment).
pub const MAX_TRANSITION_SLOTS: usize = 2;

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
}

/// Why an owner was not constructed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OwnerRefused {
    /// The box cannot hold the largest reservation of a transition.
    BoxTooSmall { capacity: usize, needed: usize },
    /// The tables could not be allocated.
    AllocationFailed,
}

impl From<AllocationFailed> for OwnerRefused {
    fn from(_: AllocationFailed) -> Self {
        Self::AllocationFailed
    }
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

/// The operation an exchange waits for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct InFlight {
    operation: OperationId,
    /// `Submitted` or `Indeterminate`.
    phase: CommandPhase,
    /// The deadline armed for its result. Each resolution arms another.
    deadline: DeadlineToken,
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
    admission: Option<OperationId>,
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
    Committed { slot: usize },
    /// The exchange ends and gives its lease back.
    End { slot: usize },
    /// The operation in flight changes phase, and deadline if one is given.
    Await {
        slot: usize,
        phase: CommandPhase,
        deadline: Option<DeadlineToken>,
    },
    /// The durable termination of the exchange is submitted.
    Release { slot: usize, in_flight: InFlight },
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
}

/// What a plan draws and publishes while it is computed.
struct Draft {
    epoch: RunEpoch,
    owner: LogicalOwnerId,
    counters: Counters,
    effects: Effects,
    progress: Progress,
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
        self.emit(Effect::CloseConnection { connection, reason })
            .map(|_| ())
    }
}

/// One logical owner: its sessions, its routes, its inbound exchanges and
/// the counters of the identities it issues.
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
    resolution_delay: LogicalDuration,
}

fn table<T>(bound: NonZeroU32) -> Result<BoundedVec<T>, AllocationFailed> {
    BoundedVec::with_capacity(usize::try_from(bound.get()).map_err(|_| AllocationFailed)?)
}

impl Owner {
    /// An owner that recovers: it accepts recovery pages and nothing else
    /// until the page that closes the inventory.
    ///
    /// The owner, the data set and the run epoch are those of `outbox`,
    /// which only a creation authority builds (ADR 0009). The box is not
    /// kept: every step is handed the box to publish into, and it must be
    /// a box of the same owner under the same authority.
    pub fn new(config: OwnerConfig, outbox: &Outbox) -> Result<Self, OwnerRefused> {
        if outbox.capacity() < MAX_TRANSITION_SLOTS {
            return Err(OwnerRefused::BoxTooSmall {
                capacity: outbox.capacity(),
                needed: MAX_TRANSITION_SLOTS,
            });
        }
        let id = outbox.id();
        let credit = Credits::new(
            config.payload_bytes,
            ExchangeSlots::new(u64::from(config.max_exchanges.get())),
        );
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
            routes: table(config.max_routes)?,
            sessions: table(config.max_sessions)?,
            slots: table(config.max_exchanges)?,
            leases: LeaseTable::new(config.account, credit, config.max_exchanges)?,
            resolution_delay: config.resolution_delay,
        })
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
    /// `outbox` is the box of this owner under the authority of its run
    /// epoch; anything else is a fault. A refusal keeps the input, the
    /// state, the identity counters, the account, the reservations held
    /// and the content of the box as they were.
    pub fn step(&mut self, input: Input, grant: TurnGrant, outbox: &mut Outbox) -> StepOutcome {
        let id = outbox.id();
        if id.owner() != self.id || id.incarnation() != self.incarnation || id.epoch() != self.epoch
        {
            return StepOutcome::Fatal(CoreFault::ImpossibleInput);
        }
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

    /// The work an input costs: one unit, and one per fact of a page.
    fn cost(input: &Input) -> WorkUnits {
        let facts = match input {
            Input::Recovery { facts, .. } => facts.len(),
            _ => 0,
        };
        WorkUnits::new(u64::try_from(facts).unwrap_or(u64::MAX).saturating_add(1))
    }

    /// Computes the transition of `input` and mutates nothing.
    fn plan(&self, input: &Input, grant: TurnGrant) -> Result<Plan, Refusal> {
        let work = Self::cost(input);
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
            } => Ok(self.plan_frame_result(*connection, *progress)),
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
                    SlotState::AwaitPubrel => self.plan_release(slot, draft),
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
        let recipe = FrozenRecipe::freeze(recipe).map_err(|_| IMPOSSIBLE)?;
        draft.emit(Effect::Persist {
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
        let mut list = BoundedVec::with_capacity(targets)
            .map_err(|_| lacking(Resource::References, references))?;

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
    fn plan_release(&self, slot: usize, draft: &mut Draft) -> Result<Change, Stop> {
        let held = &self.slots.as_slice()[slot];
        let operation = self.operation(draft)?;
        let recipe = Recipe::ReleaseInbound(ReleaseInbound {
            operation,
            source_session: held.session,
            packet_id: held.packet_id,
            exchange: held.exchange,
        });
        let in_flight = self.plan_submission(operation, recipe, None, draft)?;
        Ok(Change::Release { slot, in_flight })
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
            // No exchange waits for this operation. It is the consumed
            // admission of an exchange, or it is stale.
            if !slots.iter().any(|slot| slot.admission == Some(operation)) {
                return Ok(Change::Nothing);
            }
            let event = match reply {
                StoreReply::Violation(StoreViolation::RecipeMismatch) => {
                    return Err(Stop::Fatal(CoreFault::RecipeMismatch { operation }));
                }
                StoreReply::Outcome(outcome) => {
                    CommandEvent::StoreResult(OutcomeClass::of(outcome))
                }
            };
            return match CommandPhase::Consumed.on(event) {
                Verdict::Fault => Err(contradiction),
                _ => Ok(Change::Nothing),
            };
        };
        let slot = &slots[index];
        let Some(in_flight) = slot.in_flight() else {
            return Err(IMPOSSIBLE);
        };
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
                if admission {
                    // PUBREC becomes eligible here and nowhere earlier.
                    self.plan_answer(slot, OutboundFrame::Pubrec { packet_id, reason }, draft)?;
                    Ok(Change::Committed { slot: index })
                } else {
                    self.plan_answer(slot, OutboundFrame::Pubcomp { packet_id, reason }, draft)?;
                    Ok(Change::End { slot: index })
                }
            }
            (
                Verdict::Transition(CommandPhase::RejectedCertain),
                StoreOutcome::RejectedCertain(rejection),
            ) => {
                if admission {
                    // Nothing was admitted: the exchange ends and the
                    // client is told, as the protocol says.
                    let frame = OutboundFrame::Pubrec {
                        packet_id: slot.packet_id,
                        reason: AckReason::refusing(rejection),
                    };
                    self.plan_answer(slot, frame, draft)?;
                    Ok(Change::End { slot: index })
                } else {
                    // The exchange is still durably open and the client
                    // will not send PUBREL again on a live connection.
                    self.plan_release(index, draft)
                }
            }
            (Verdict::Transition(CommandPhase::Indeterminate), _) => Ok(Change::Await {
                slot: index,
                phase: CommandPhase::Indeterminate,
                deadline: None,
            }),
            // S0 retires no identity space: the store cannot report that
            // the detail of a result the exchange waits for is gone.
            _ => Err(IMPOSSIBLE),
        }
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
        draft.emit(Effect::ResolveAndFence {
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
            deadline: Some(deadline),
        })
    }

    fn plan_frame_result(&self, connection: ConnectionKey, progress: FrameProgress) -> Change {
        let failed = matches!(progress, FrameProgress::Failed);
        match self.session_of(connection.generation) {
            Some(session)
                if failed && self.sessions.as_slice()[session].id == connection.session =>
            {
                Change::Unbind { session }
            }
            _ => Change::Nothing,
        }
    }

    /// Checks a recovery page against the state and against itself. The
    /// application then cannot be refused.
    fn plan_recovery(&self, facts: &[RecoveredFact], last: bool) -> Result<Change, Stop> {
        let Some(mut order) = self.recovery.clone() else {
            return Err(IMPOSSIBLE);
        };
        let mut epoch_recovered = self.epoch_recovered;
        let mut limits = self.limits.is_some();
        let mut route_table = self.route_revision.is_some();
        let mut routes = self.routes.len();
        let mut sessions = self.sessions.len();
        let mut slots = self.slots.len();
        for (position, fact) in facts.iter().enumerate() {
            order.admit(fact).map_err(|_| IMPOSSIBLE)?;
            let earlier = &facts[..position];
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
                RecoveredFact::Route { .. } => {
                    routes += 1;
                    route_table && routes <= self.routes.capacity()
                }
                RecoveredFact::Session {
                    session, client_id, ..
                } => {
                    sessions += 1;
                    let held = self
                        .sessions
                        .as_slice()
                        .iter()
                        .any(|held| held.id == *session || held.client_id == *client_id);
                    let repeated = earlier.iter().any(|other| {
                        matches!(other, RecoveredFact::Session { session: id, client_id: name, .. }
                            if id == session || name == client_id)
                    });
                    sessions <= self.sessions.capacity() && !held && !repeated
                }
                RecoveredFact::InboundExchange {
                    exchange,
                    packet_id,
                    ..
                } => {
                    slots += 1;
                    let known = self
                        .sessions
                        .as_slice()
                        .iter()
                        .any(|held| held.id == exchange.session)
                        || earlier.iter().any(|other| {
                            matches!(other, RecoveredFact::Session { session, .. }
                                if *session == exchange.session)
                        });
                    let held = self.slots.as_slice().iter().any(|held| {
                        held.exchange == *exchange
                            || (held.session == exchange.session && held.packet_id == *packet_id)
                    });
                    let repeated = earlier.iter().any(|other| {
                        matches!(other, RecoveredFact::InboundExchange { exchange: id, packet_id: packet, .. }
                            if id == exchange
                                || (id.session == exchange.session && packet == packet_id))
                    });
                    // A generation of this epoch would meet one the owner
                    // is about to issue.
                    let older = generation_epoch(exchange.generation) < self.epoch.get();
                    slots <= self.slots.capacity() && known && !held && !repeated && older
                }
                RecoveredFact::Operation { operation, .. } => {
                    operation.incarnation() == self.incarnation && operation.epoch() < self.epoch
                }
            };
            if !valid {
                return Err(IMPOSSIBLE);
            }
        }
        // An inventory is closed only when it named the run epoch, the
        // limits and the route table: an admission needs all three.
        if last && !(epoch_recovered && limits && route_table) {
            return Err(IMPOSSIBLE);
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
                    state: SlotState::AdmissionPending(in_flight),
                };
                self.slots.try_push(slot).map_err(|_| STALE)?;
            }
            Change::Committed { slot } => {
                let held = self.slots.as_mut_slice().get_mut(slot).ok_or(STALE)?;
                let SlotState::AdmissionPending(in_flight) = held.state else {
                    return Err(STALE);
                };
                held.admission = Some(in_flight.operation);
                held.state = SlotState::AwaitPubrel;
            }
            Change::End { slot } => {
                let slots = self.slots.as_mut_slice();
                let last = slots.len().checked_sub(1).ok_or(STALE)?;
                let lease = slots.get(slot).ok_or(STALE)?.lease;
                if self.leases.release(lease) != Ok(ReleaseOutcome::Released) {
                    return Err(STALE);
                }
                self.slots.as_mut_slice().swap(slot, last);
                let _ = self.slots.pop();
            }
            Change::Await {
                slot,
                phase,
                deadline,
            } => {
                let held = self.slots.as_mut_slice().get_mut(slot).ok_or(STALE)?;
                let (SlotState::AdmissionPending(in_flight) | SlotState::ReleasePending(in_flight)) =
                    &mut held.state
                else {
                    return Err(STALE);
                };
                in_flight.phase = phase;
                if let Some(deadline) = deadline {
                    in_flight.deadline = deadline;
                }
            }
            Change::Release { slot, in_flight } => {
                let held = self.slots.as_mut_slice().get_mut(slot).ok_or(STALE)?;
                held.state = SlotState::ReleasePending(in_flight);
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
        outbox: Outbox,
        owner: Owner,
        /// The commands submitted so far, in order.
        flights: Vec<Flight>,
    }

    /// The generation the next lease of a slot takes, and the lease the
    /// slot holds.
    type LeaseView = (Option<u64>, Option<LeaseKey>);

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
        leases: (Credits, Vec<LeaseView>),
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
            let owner = Owner::new(config, &outbox).expect("large enough");
            Self {
                authority,
                outbox,
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

        fn step(&mut self, input: Input) -> StepOutcome {
            self.owner.step(input, GRANT, &mut self.outbox)
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
            let _ = self.outbox.dispatch(&mut sink);
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
                waiting: self.outbox.pending().map(OwnedEffect::id).collect(),
                free_slots: self.outbox.free_slots(),
                live_reservations: self.outbox.has_live_reservations(),
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
        let outcome = world.owner.step(input(&world), short, &mut world.outbox);
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
            let all = nonzero(world.outbox.free_slots());
            let _held = world.outbox.reserve(all).expect("free");
            world.applied(input(&world));
            return;
        }

        // The effect slots: one is missing. A twin world is squeezed the
        // same way and not stepped.
        let mut world = build();
        let mut twin = build();
        let hold = nonzero(world.outbox.free_slots() - (slots - 1));
        let held = world.outbox.reserve(hold).expect("free");
        let twin_held = twin.outbox.reserve(hold).expect("free");
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
        assert_eq!(world.outbox.release(held), Ok(hold.get()));
        assert_eq!(twin.outbox.release(twin_held), Ok(hold.get()));
        let next = world.outbox.reserve(nonzero(1)).expect("free");
        assert_eq!(Ok(next), twin.outbox.reserve(nonzero(1)));
        assert_eq!(world.outbox.release(next), Ok(1));
        // With room, the input that came back is applied.
        world.applied(input(&world));

        // The reservation table: every entry is held.
        let mut world = build();
        for _ in 0..ENTRIES {
            let _held = world.outbox.reserve(nonzero(1)).expect("free");
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
        assert_eq!(
            Owner::new(config(), &small).err(),
            Some(OwnerRefused::BoxTooSmall {
                capacity: 1,
                needed: 2,
            })
        );
        let exact = authority
            .create_outbox(
                LogicalOwnerId::new(5),
                nonzero(MAX_TRANSITION_SLOTS),
                bound(1),
            )
            .expect("second box");
        let owner = Owner::new(config(), &exact).expect("large enough");
        assert_eq!(owner.id(), LogicalOwnerId::new(5));
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
        assert_eq!(world.outbox.capacity(), MAX_TRANSITION_SLOTS);
    }

    #[test]
    fn a_box_of_another_owner_or_another_authority_is_a_fault() {
        let mut world = World::connected();
        let before = world.snapshot();
        let mut other = world
            .authority
            .create_outbox(LogicalOwnerId::new(5), nonzero(CAPACITY), bound(ENTRIES))
            .expect("second box");
        assert_eq!(
            world.owner.step(publish(7), GRANT, &mut other),
            StepOutcome::Fatal(CoreFault::ImpossibleInput)
        );
        let mut later = authority(INCARNATION, EPOCH + 1);
        let mut foreign = later
            .create_outbox(LogicalOwnerId::new(4), nonzero(CAPACITY), bound(ENTRIES))
            .expect("first box");
        assert_eq!(
            world.owner.step(publish(7), GRANT, &mut foreign),
            StepOutcome::Fatal(CoreFault::ImpossibleInput)
        );
        assert!(other.is_empty() && foreign.is_empty());
        assert_eq!(world.snapshot(), before);
    }

    // ADR 0009: replacing the box of an owner resets none of its counters.
    #[test]
    fn replacing_the_box_resets_no_counter_of_the_owner() {
        let mut world = World::connected();
        let first = world.submit(publish(7));
        let counters = world.owner.counters;
        let serial = world.outbox.id().serial();

        let World {
            mut authority,
            outbox,
            mut owner,
            ..
        } = world;
        assert_eq!(authority.retire(outbox).map_err(|_| ()), Ok(()));
        let mut outbox = authority
            .create_outbox(LogicalOwnerId::new(4), nonzero(CAPACITY), bound(ENTRIES))
            .expect("the box that replaces it");
        assert_ne!(outbox.id().serial(), serial);
        assert_eq!(owner.counters, counters);

        let outcome = owner.step(committed(&first), GRANT, &mut outbox);
        assert!(
            matches!(outcome, StepOutcome::Applied { .. }),
            "{outcome:?}"
        );
        let outcome = owner.step(publish(8), GRANT, &mut outbox);
        assert!(
            matches!(outcome, StepOutcome::Applied { .. }),
            "{outcome:?}"
        );
        let mut sink = Taking(Vec::new());
        let _ = outbox.dispatch(&mut sink);
        let effects: Vec<_> = sink.0.into_iter().map(OwnedEffect::into_parts).collect();
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
        let key = world.outbox.reserve(nonzero(2)).expect("free");
        assert_eq!(
            world
                .owner
                .apply(first, publish(7), Some(key), &mut world.outbox),
            Ok(Progress::WaitingForEffects)
        );
        let before = world.snapshot();
        let key = world.outbox.reserve(nonzero(2)).expect("free");
        assert_eq!(
            world
                .owner
                .apply(second, publish(7), Some(key), &mut world.outbox),
            Err(CoreFault::StalePlan)
        );
        assert_eq!(world.outbox.release(key), Ok(2));
        let key = world.outbox.reserve(nonzero(1)).expect("free");
        assert_eq!(
            world
                .owner
                .apply(other, pubrel(9), Some(key), &mut world.outbox),
            Err(CoreFault::StalePlan)
        );
        assert_eq!(world.outbox.release(key), Ok(1));
        assert_eq!(world.snapshot(), before);
    }

    #[test]
    fn a_plan_is_not_applied_to_another_owner() {
        let world = World::connected();
        let plan = world.owner.plan(&publish(7), GRANT).expect("a plan");
        // Another owner at the same revision, with its own box.
        let mut authority = authority(INCARNATION, EPOCH);
        let mut outbox = authority
            .create_outbox(LogicalOwnerId::new(5), nonzero(CAPACITY), bound(ENTRIES))
            .expect("first box");
        let mut owner = Owner::new(config(), &outbox).expect("large enough");
        for input in [page(&fixture(EPOCH), true), connect(CONN, b"p1-producer")] {
            let outcome = owner.step(input, GRANT, &mut outbox);
            assert!(
                matches!(outcome, StepOutcome::Applied { .. }),
                "{outcome:?}"
            );
        }
        assert_eq!(owner.revision(), world.owner.revision());
        let key = outbox.reserve(nonzero(2)).expect("free");
        assert_eq!(
            owner.apply(plan, publish(7), Some(key), &mut outbox),
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
            let key = world.outbox.reserve(nonzero(wrong)).expect("free");
            assert_eq!(
                world
                    .owner
                    .apply(plan, publish(7), Some(key), &mut world.outbox),
                Err(CoreFault::ReservationOverrun)
            );
            assert_eq!(world.outbox.release(key), Ok(wrong));
        }
        let plan = world.owner.plan(&publish(7), GRANT).expect("a plan");
        assert_eq!(
            world.owner.apply(plan, publish(7), None, &mut world.outbox),
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
                work_used: WorkUnits::new(9),
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
            assert!(world.outbox.is_empty());
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
        // The connection that was replaced has no session any more.
        assert_eq!(world.sent(publish(7)), ["CloseConnection"]);
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
        for input in [connect(CONN, b"stranger"), publish(7), pubrel(7)] {
            world.applied(input);
            assert_eq!(world.drain()[0].1, close(CONN));
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
        assert!(world.outbox.is_empty());
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

    /// Every result of an operation no exchange waits for is absorbed.
    fn every_result_is_absorbed(world: &mut World, flight: &Flight) {
        for input in [
            committed(flight),
            rejected(flight),
            unknown(flight),
            retired(flight),
        ] {
            absorbed(world, input);
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
        every_result_is_absorbed(&mut world, &admission);
        let mut world = refused();
        let admission = world.flights[0];
        every_result_is_absorbed(&mut world, &admission);
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
        every_result_is_absorbed(&mut world, &admission);
        assert_eq!(world.phase(7), None);
    }

    #[test]
    fn rule_inbound_qos2_absent_release_result_stale() {
        let mut world = World::ended();
        let release = world.flights[1];
        every_result_is_absorbed(&mut world, &release);
    }

    // Unreachable, for the same reason as the admission result.
    #[test]
    fn rule_inbound_qos2_absent_release_result_current() {
        let mut world = World::ended();
        assert_eq!(world.phase(7), None);
        assert!(world.owner.slots.is_empty());
        let release = world.flights[1];
        every_result_is_absorbed(&mut world, &release);
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
        assert!(world.outbox.is_empty());
        assert_eq!(world.phase(7), PENDING);
        assert_eq!(
            world.in_flight(7),
            InFlight {
                operation: admission.operation,
                phase: CommandPhase::Indeterminate,
                deadline: admission.deadline,
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
        every_result_is_absorbed(&mut world, &stale);
        assert_eq!(world.in_flight(7).operation, current.operation);
    }

    #[test]
    fn rule_inbound_qos2_admission_pending_release_result_stale() {
        let mut world = World::ended();
        let current = world.submit(publish(7));
        let stale = world.flights[1];
        every_result_is_absorbed(&mut world, &stale);
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
        every_result_is_absorbed(&mut world, &stale);
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
        let claim = StoreOutcome::Committed(CommitFacts::InboundReleased { exchange });
        absorbed(&mut world, result(&admission, claim));
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
        let again = world.submit(rejected(&refused));
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
        handed_back(World::release_pending, &|w| rejected(&w.flights[1]), 2);
    }

    #[test]
    fn rule_inbound_qos2_release_pending_unknown_keep() {
        let mut world = World::release_pending();
        let release = world.flights[1];
        world.applied(unknown(&release));
        assert!(world.outbox.is_empty());
        assert_eq!(world.phase(7), RELEASING);
        assert_eq!(
            world.in_flight(7),
            InFlight {
                operation: release.operation,
                phase: CommandPhase::Indeterminate,
                deadline: release.deadline,
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
        let current = world.submit(rejected(&stale));
        every_result_is_absorbed(&mut world, &stale);
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

    #[test]
    fn a_deadline_is_handed_back_at_each_boundary_of_capacity() {
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
        assert!(before.outbox.is_empty());
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
        let mismatch = |flight: &Flight| Input::StoreResult {
            effect: flight.effect,
            operation: flight.operation,
            reply: StoreReply::Violation(StoreViolation::RecipeMismatch),
        };
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
        // An operation no exchange holds any more has no recipe to compare.
        let mut world = World::ended();
        absorbed(&mut world, mismatch(&release));
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
        assert!(world.outbox.is_empty());
        assert_eq!(world.phase(7), AWAIT);
        assert_eq!(world.sent(connect(11, b"p1-producer")), ["EmitConnack"]);
        world.applied(publish_with(11, 7, Qos::ExactlyOnce, b"payload"));
        assert_eq!(world.drain()[0].1, frame_to(11, pubrec(AckReason::Success)));
        // The same without a connection at the end of the exchange.
        let mut world = World::release_pending();
        assert_eq!(world.sent(publish(7)), ["CloseConnection"]);
        world.applied(committed(&world.flights[1]));
        assert!(world.outbox.is_empty());
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
            work_limit: WorkUnits::new(8),
        };
        assert_eq!(
            world
                .owner
                .step(page(&fixture(EPOCH), true), short, &mut world.outbox),
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
            work_limit: WorkUnits::new(9),
        };
        let outcome = world
            .owner
            .step(page(&fixture(EPOCH), true), exact, &mut world.outbox);
        assert!(
            matches!(outcome, StepOutcome::Applied { .. }),
            "{outcome:?}"
        );
    }
}
