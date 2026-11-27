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

use alloc::collections::{BTreeMap, BTreeSet};
use core::num::{NonZeroU32, NonZeroUsize};

use crate::command::{CommandEvent, CommandPhase, OutcomeClass, Verdict};
use crate::contract::capacity::{
    AllocationFailed, BoundedQueue, BoundedVec, CapacityNeed, ExchangeSlots, PayloadBytes,
    ReceiptSlots, References, Resource, WakeCondition, WorkUnits,
};
use crate::contract::command::{
    AdmitPublication, ClaimedPublication, Command, Costs, DeliveryCause, FrozenRecipe, InlineClaim,
    MultiClaimedPublication, PayloadRef, Recipe, RecipeError, ReleaseInbound, Source, Target,
};
use crate::contract::effects::{
    CloseReason, CreateRefused, DispatchReport, Effect, EffectReservation, EffectSink, FactRank,
    FrameProgress, Input, Outbox, OwnedEffect, OwnerAuthority, RecoveredFact, RecoveryOrder,
    ReplaceRefusal, RetireRefusal,
};
use crate::contract::flat_map::{FlatMap, FlatSet, ScopedFlatMap};
use crate::contract::ids::{
    AccountId, ConnectionGeneration, ConnectionKey, ContinuationId, DeadlineToken, DeliveryId,
    EffectId, EffectSeq, ExchangeGeneration, ExchangeId, Exhausted, InboundPacketId,
    LeaseGeneration, LeaseKey, LogicalDuration, LogicalOwnerId, OperationId, OperationSeq,
    OutboundPacketId, PayloadId, PayloadSeq, PublicationId, PublicationSeq, RequestId, RequestSeq,
    Revision, RunEpoch, SessionId, StoreIncarnation,
};
use crate::contract::outcome::{
    CommitFacts, RejectReason, StoreOutcome, StoreReply, StoreViolation,
};
use crate::contract::packet::{
    AckReason, InboundPacket, OutboundFrame, PubackReason, Qos, SharedBytes,
};
use crate::contract::step::{CoreFault, Exhaustion, Progress, StepOutcome, TurnGrant};
use crate::credits::{Credits, ReleaseOutcome};
use crate::leases::{LeaseRefused, LeaseTable};

use crate::contract::message_expiry::{
    ExpiryState, MessageExpiry, MessageExpiryBlock, MessageTime,
};
use crate::slab::{BoundedSlab, SmallIndexSet};

mod connections;
mod deliveries;
mod pubsub;
mod receipts;
mod routing;
mod sessions;
mod shard;
mod will;
pub use deliveries::{OUTGOING_PI_BITMAP_BITS, outgoing_pi_candidate_limit};
pub use pubsub::{MAX_OUTGOING_WINDOW, QOS0_CONSUMED_WINDOW, QOS0_WINDOW};
pub use sessions::SessionStatus;
pub use shard::{
    DelegatedClaim, DelegatedInlineBatch, DelegatedQos0Batch, DelegatedQos0Target,
    InlineClaimOracle, ShardAckProgress, ShardLimits, ShardStepOutcome,
};
pub use will::{WillDeferredReason, WillStatus};

/// The largest reservation of effect slots one transition makes. The
/// constructor of an owner refuses a box that is smaller (ADR 0006,
/// amendment).
pub const MAX_TRANSITION_SLOTS: usize = 2;
/// The opt-in lifecycle path may reserve close + persist + deadline together.
/// Historical S0 construction keeps its two-slot minimum; an insufficient
/// lifecycle box receives the existing permanent reservation refusal.
const MAX_TRANSITION_EFFECTS: usize = 3;

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
    /// Inbound admissions/exchanges and outstanding QoS 1 acknowledgements
    /// the owner can hold at a time, across both QoS levels. The product
    /// outgoing-delivery table has this same bound independently.
    pub max_exchanges: NonZeroU32,
    /// Inbound QoS 2 exchanges one session can hold at a time, across its
    /// connections: the exchanges a client can keep open, between PUBREC
    /// and its PUBREL. A QoS 2 publication that would be one more waits
    /// while the owner itself is admitting or releasing one of them, and
    /// closes its connection when every one of them awaits a PUBREL of the
    /// client (record 0061). A table of `max_sessions` times this bound
    /// cannot be filled by exchanges only clients can end.
    pub session_qos2_exchanges: NonZeroU32,
    /// The account charged for the exchanges this owner holds.
    pub account: AccountId,
    /// Payload bytes of the publications this owner is admitting or
    /// acknowledging: an exchange holds its payload until its QoS 1
    /// acknowledgement ends it or until it awaits its PUBREL, which holds
    /// an exchange and no byte. Loaded outgoing payloads have this same
    /// bound independently of the incoming leases.
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
    /// The new box could not be created. The owner keeps its box and the
    /// authority is unchanged: the identity stays taken (review R2).
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
    /// The result of the admission is known and its answer waits for the
    /// answers of the publications the connection sent before it (record
    /// 0050).
    AnswerHeld,
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
    continuation: Option<crate::contract::ids::ContinuationId>,
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
            continuation: Some(crate::contract::ids::ContinuationId::new(1)),
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
    expiry: crate::contract::session::SessionExpiry,
    opened_by: Option<OperationId>,
    detached: Option<alloc::boxed::Box<crate::contract::session::SessionDetachment>>,
    /// Product closure authority carries an explicit Will disposition.
    will_managed: bool,
}

impl Session {
    fn detached(&self) -> Option<crate::contract::session::SessionDetachment> {
        self.detached.as_deref().copied()
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
struct Route {
    topic: SharedBytes,
    target: SessionId,
    target_revision: Revision,
    max_qos: Qos,
    options: crate::contract::pubsub::SubscriptionOptions,
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
///   up to rank `p * 2^LANDMARK_RESOLUTIONS`. The bound `k` is uniform: it
///   holds for every resolution of the operation, and it is measured in
///   newer resolutions, not in time. Delays that are each finite but have
///   no common bound are outside I3, and the exchange may then stay
///   unresolved.
/// - I4, monotonic knowledge. A repeated result changes nothing. A result
///   that contradicts what the operation established is a fault while the
///   operation is retained; beyond the retained terminated operations it
///   is absorbed (`OwnerConfig::retained_operations`).
/// - I5, consumed. A terminated operation never goes through the
///   application path again; its late results only meet the check of I4.
///
/// Prefix-stripped answer set: all retained effects for an
/// operation belong to one `(epoch, owner)` scope, so only 8-byte `EffectSeq`
/// values plus a presence bitmask are stored inline (88 B vs 280 B).
#[derive(Clone, PartialEq, Eq, Debug)]
struct Resolutions {
    recent: [EffectSeq; RECENT_RESOLUTIONS],
    landmarks: [EffectSeq; LANDMARK_RESOLUTIONS],
    /// Resolutions emitted for the operation so far.
    emitted: u64,
    /// Bit `0..RECENT_RESOLUTIONS` = populated `recent`, next bits = `landmarks`.
    present: u8,
}

#[derive(Clone, PartialEq, Eq, Debug)]
struct Answers {
    persist: EffectId,
    resolutions: Option<alloc::boxed::Box<Resolutions>>,
}

impl Answers {
    fn new(persist: EffectId) -> Self {
        Self {
            persist,
            resolutions: None,
        }
    }

    fn names(&self, effect: EffectId) -> bool {
        if effect.epoch != self.persist.epoch || effect.owner != self.persist.owner {
            return false;
        }
        let seq = effect.sequence;
        if self.persist.sequence == seq {
            return true;
        }
        let Some(res) = self.resolutions.as_deref() else {
            return false;
        };
        for i in 0..RECENT_RESOLUTIONS {
            if (res.present & (1u8 << i)) != 0 && res.recent[i] == seq {
                return true;
            }
        }
        for i in 0..LANDMARK_RESOLUTIONS {
            if (res.present & (1u8 << (RECENT_RESOLUTIONS + i))) != 0 && res.landmarks[i] == seq {
                return true;
            }
        }
        false
    }

    /// Retains the next resolution of the operation.
    fn with(mut self, resolution: EffectId) -> Self {
        if resolution.epoch != self.persist.epoch || resolution.owner != self.persist.owner {
            return self;
        }
        let res = self.resolutions.get_or_insert_with(|| {
            alloc::boxed::Box::new(Resolutions {
                recent: [EffectSeq::new(0); RECENT_RESOLUTIONS],
                landmarks: [EffectSeq::new(0); LANDMARK_RESOLUTIONS],
                emitted: 0,
                present: 0,
            })
        });
        res.emitted = res.emitted.saturating_add(1);
        let rank = res.emitted;
        let place = |rank: u64, len: usize| {
            usize::try_from(rank % u64::try_from(len).unwrap_or(1)).unwrap_or(0)
        };
        let recent_idx = place(rank, RECENT_RESOLUTIONS);
        if let Some(slot) = res.recent.get_mut(recent_idx) {
            *slot = resolution.sequence;
            res.present |= 1u8 << recent_idx;
        }
        if rank.is_power_of_two() {
            let landmark = u64::from(rank.trailing_zeros());
            let landmark_idx = place(landmark, LANDMARK_RESOLUTIONS);
            if let Some(slot) = res.landmarks.get_mut(landmark_idx) {
                *slot = resolution.sequence;
                res.present |= 1u8 << (RECENT_RESOLUTIONS + landmark_idx);
            }
        }
        self
    }
}

/// The operation an exchange waits for.
#[derive(Clone, PartialEq, Eq, Debug)]
struct InFlight {
    operation: OperationId,
    /// `Submitted` or `Indeterminate`.
    phase: CommandPhase,
    /// The deadline armed for its result. Each resolution arms another.
    deadline: DeadlineToken,
    answers: Answers,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DeliveryFactKind {
    Claimed,
    Finished,
    Started,
    PubrelReady,
    Expired,
}

/// Prefix-stripped compact representation of `CommitFacts` for retained
/// terminal operations: zero heap allocations for hot QoS 1/2 facts.
#[derive(Clone, PartialEq, Eq, Debug)]
enum CompactFacts {
    PublicationAdmitted {
        publication_seq: PublicationSeq,
        exchange: ExchangeId,
    },
    DeliveryLocal {
        kind: DeliveryFactKind,
        packet_id: OutboundPacketId,
        index: u32,
        pub_owner: LogicalOwnerId,
        pub_seq: PublicationSeq,
        target: SessionId,
        claim_owner: LogicalOwnerId,
        claim_seq: OperationSeq,
    },
    InboundReleased {
        exchange: ExchangeId,
    },
    Boxed(alloc::boxed::Box<CommitFacts>),
}

impl CompactFacts {
    fn from_commit(operation: OperationId, facts: CommitFacts) -> Self {
        let run_scope = (operation.incarnation(), operation.epoch());
        let scope = (run_scope.0, run_scope.1, operation.owner());
        match facts {
            CommitFacts::PublicationAdmitted {
                publication,
                exchange,
            } if (
                publication.incarnation(),
                publication.epoch(),
                publication.owner(),
            ) == scope =>
            {
                Self::PublicationAdmitted {
                    publication_seq: publication.sequence(),
                    exchange,
                }
            }
            CommitFacts::InboundReleased { exchange } => Self::InboundReleased { exchange },
            CommitFacts::DeliveryClaimed {
                delivery,
                target,
                packet_id,
                claim,
            }
            | CommitFacts::DeliveryFinished {
                delivery,
                target,
                packet_id,
                claim,
            }
            | CommitFacts::DeliveryStarted {
                delivery,
                target,
                packet_id,
                claim,
            }
            | CommitFacts::DeliveryPubrelReady {
                delivery,
                target,
                packet_id,
                claim,
            }
            | CommitFacts::DeliveryExpired {
                delivery,
                target,
                packet_id,
                claim,
            } if (
                delivery.publication.incarnation(),
                delivery.publication.epoch(),
            ) == run_scope
                && (claim.incarnation(), claim.epoch()) == run_scope =>
            {
                let kind = match facts {
                    CommitFacts::DeliveryClaimed { .. } => DeliveryFactKind::Claimed,
                    CommitFacts::DeliveryFinished { .. } => DeliveryFactKind::Finished,
                    CommitFacts::DeliveryStarted { .. } => DeliveryFactKind::Started,
                    CommitFacts::DeliveryPubrelReady { .. } => DeliveryFactKind::PubrelReady,
                    CommitFacts::DeliveryExpired { .. } => DeliveryFactKind::Expired,
                    _ => panic!("matched delivery commit facts variant"),
                };
                Self::DeliveryLocal {
                    kind,
                    packet_id,
                    index: delivery.index,
                    pub_owner: delivery.publication.owner(),
                    pub_seq: delivery.publication.sequence(),
                    target,
                    claim_owner: claim.owner(),
                    claim_seq: claim.sequence(),
                }
            }
            other => Self::Boxed(alloc::boxed::Box::new(other)),
        }
    }

    fn matches(&self, operation: OperationId, facts: CommitFacts) -> bool {
        let run_scope = (operation.incarnation(), operation.epoch());
        let scope = (run_scope.0, run_scope.1, operation.owner());
        match (self, facts) {
            (
                Self::PublicationAdmitted {
                    publication_seq,
                    exchange,
                },
                CommitFacts::PublicationAdmitted {
                    publication,
                    exchange: other_ex,
                },
            ) => {
                (
                    publication.incarnation(),
                    publication.epoch(),
                    publication.owner(),
                ) == scope
                    && publication.sequence() == *publication_seq
                    && other_ex == *exchange
            }
            (
                Self::InboundReleased { exchange },
                CommitFacts::InboundReleased { exchange: other },
            ) => *exchange == other,
            (
                Self::DeliveryLocal {
                    kind,
                    packet_id,
                    index,
                    pub_owner,
                    pub_seq,
                    target,
                    claim_owner,
                    claim_seq,
                },
                other,
            ) => {
                let (other_kind, delivery, other_target, other_pi, claim) = match other {
                    CommitFacts::DeliveryClaimed {
                        delivery,
                        target,
                        packet_id,
                        claim,
                    } => (
                        DeliveryFactKind::Claimed,
                        delivery,
                        target,
                        packet_id,
                        claim,
                    ),
                    CommitFacts::DeliveryFinished {
                        delivery,
                        target,
                        packet_id,
                        claim,
                    } => (
                        DeliveryFactKind::Finished,
                        delivery,
                        target,
                        packet_id,
                        claim,
                    ),
                    CommitFacts::DeliveryStarted {
                        delivery,
                        target,
                        packet_id,
                        claim,
                    } => (
                        DeliveryFactKind::Started,
                        delivery,
                        target,
                        packet_id,
                        claim,
                    ),
                    CommitFacts::DeliveryPubrelReady {
                        delivery,
                        target,
                        packet_id,
                        claim,
                    } => (
                        DeliveryFactKind::PubrelReady,
                        delivery,
                        target,
                        packet_id,
                        claim,
                    ),
                    CommitFacts::DeliveryExpired {
                        delivery,
                        target,
                        packet_id,
                        claim,
                    } => (
                        DeliveryFactKind::Expired,
                        delivery,
                        target,
                        packet_id,
                        claim,
                    ),
                    _ => return false,
                };
                *kind == other_kind
                    && *packet_id == other_pi
                    && *index == delivery.index
                    && *target == other_target
                    && (
                        delivery.publication.incarnation(),
                        delivery.publication.epoch(),
                    ) == run_scope
                    && delivery.publication.owner() == *pub_owner
                    && delivery.publication.sequence() == *pub_seq
                    && (claim.incarnation(), claim.epoch()) == run_scope
                    && claim.owner() == *claim_owner
                    && claim.sequence() == *claim_seq
            }
            (Self::Boxed(stored), other) => **stored == other,
            _ => false,
        }
    }
}

/// An operation that ended, as the owner knows it: kept so that a late
/// result that contradicts it is a fault, not a stale answer.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Finished {
    operation: OperationId,
    answers: Answers,
    /// `Consumed` or `RejectedCertain`.
    phase: CommandPhase,
    /// What its commit established, when it committed.
    facts: Option<CompactFacts>,
}

impl Finished {
    fn new(
        operation: OperationId,
        answers: Answers,
        phase: CommandPhase,
        facts: Option<CommitFacts>,
    ) -> Self {
        Self {
            operation,
            answers,
            phase,
            facts: facts.map(|f| CompactFacts::from_commit(operation, f)),
        }
    }

    fn facts_match(&self, facts: CommitFacts) -> bool {
        self.facts
            .as_ref()
            .is_none_or(|established| established.matches(self.operation, facts))
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
enum SlotState {
    AdmissionPending(InFlight),
    AwaitPubrel,
    ReleasePending(InFlight),
    /// A known fenced termination awaits room for a fresh preparation.
    ReleaseRetry(alloc::boxed::Box<Finished>),
    /// The admission ended, but its own PUBACK frame is still outstanding.
    /// A later PUBLISH may reuse the Packet Identifier before this frame's
    /// write receipt is processed. Only effect + generation identifies it.
    AckPending {
        effect: EffectId,
        connection: ConnectionKey,
    },
    /// The result of the admission is known and consumed, and `frame`
    /// answers it, but an earlier publication of the same connection has
    /// no answer yet: the frame waits its turn (record 0050). `resent`
    /// says the frame repeats a PUBREC, for a PUBLISH received again while
    /// the exchange awaited its PUBREL: the client may then send PUBREL.
    AnswerHeld {
        frame: OutboundFrame,
        resent: bool,
    },
}

/// A held answer the plan of a transition releases beside its change.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Released {
    slot: usize,
    next: Next,
}

/// What an exchange becomes when its held answer leaves.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Next {
    /// A PUBACK waits for its write receipt.
    Ack {
        effect: EffectId,
        connection: ConnectionKey,
    },
    /// A PUBREC that accepts: the exchange awaits its PUBREL.
    AwaitPubrel,
    /// A PUBREC that refuses: the exchange is over.
    End,
}

/// One slot seen as the transition being planned leaves it: unanswered at
/// `order` in the order of its connection, or answered (`None`).
#[derive(Clone, Copy)]
struct Seen {
    slot: usize,
    order: Option<u64>,
}

/// One bounded inbound occurrence, using the shared admission path.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Slot {
    session: SessionId,
    packet_id: InboundPacketId,
    exchange: ExchangeId,
    publication: PublicationId,
    qos: Qos,
    /// The latest PUBLISH of this active occurrence. Recovery supplies no
    /// invented network ingress order.
    publish_origin: Option<(ConnectionGeneration, u64)>,
    /// The admission of this exchange named no target: its PUBACK says
    /// that no subscription matched (record 0049). `false` for an exchange
    /// read from durable data, whose targets this owner never selected.
    unmatched: bool,
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
    /// A known durable lifecycle cut cancelled this session's own exchange.
    /// Unknown command knowledge remains here until its receipt is consumed.
    retired: bool,
}

impl Slot {
    /// The reason of the PUBACK that accepts this exchange.
    const fn puback_reason(&self) -> PubackReason {
        if self.unmatched {
            PubackReason::NoMatchingSubscribers
        } else {
            PubackReason::Success
        }
    }

    fn in_flight(&self) -> Option<&InFlight> {
        match &self.state {
            SlotState::AdmissionPending(in_flight) | SlotState::ReleasePending(in_flight) => {
                Some(in_flight)
            }
            SlotState::AwaitPubrel
            | SlotState::ReleaseRetry(_)
            | SlotState::AckPending { .. }
            | SlotState::AnswerHeld { .. } => None,
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

#[derive(Clone, Copy, Debug)]
struct InlineClaimPlan {
    target_index: u16,
    target: SessionId,
    target_revision: Revision,
    qos: Qos,
    packet_id: OutboundPacketId,
    generation: ConnectionGeneration,
}

impl InlineClaimPlan {
    fn claim(&self) -> InlineClaim {
        InlineClaim {
            target_index: self.target_index,
            packet_id: self.packet_id,
        }
    }
}

#[derive(Debug)]
enum InlineClaimPlans {
    None,
    One(InlineClaimPlan),
    Many(alloc::vec::Vec<InlineClaimPlan>),
}

impl InlineClaimPlans {
    fn from_vec(plans: alloc::vec::Vec<InlineClaimPlan>) -> Self {
        match plans.len() {
            0 => Self::None,
            1 => Self::One(plans[0]),
            _ => Self::Many(plans),
        }
    }

    fn is_empty(&self) -> bool {
        matches!(self, Self::None)
    }

    fn as_slice(&self) -> &[InlineClaimPlan] {
        match self {
            Self::None => &[],
            Self::One(plan) => core::slice::from_ref(plan),
            Self::Many(plans) => plans,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum InlineSlotOutcome {
    Committed {
        slot: usize,
        emission: Option<(EffectId, ConnectionKey)>,
    },
    Remove(usize),
}

#[derive(Debug)]
enum InlineOutcome {
    One(InlineSlotOutcome),
    Many(alloc::vec::Vec<InlineSlotOutcome>),
}

/// The change of state of a transition. Indices name entries of the
/// tables at the revision the plan was computed against.
#[derive(Debug)]
enum Change {
    Session(alloc::boxed::Box<sessions::Change>),
    Will(alloc::boxed::Box<will::Change>),
    Receipts(alloc::boxed::Box<receipts::Change>),
    Pubsub(pubsub::Change),
    /// The input is absorbed: late, repeated, or answered by effects only.
    Nothing,
    MessageClock(MessageTime),
    /// The facts of the recovery page in the input join the state.
    Recover,
    /// The exclusive writer confirmed this durable session after the
    /// recovery inventory closed. Its table place was checked by the plan.
    Establish {
        session: alloc::boxed::Box<Session>,
    },
    /// The connection becomes the current one of the session.
    Bind {
        session: usize,
        connection: ConnectionGeneration,
    },
    /// The session has no current connection any more.
    Unbind {
        session: usize,
    },
    /// A new exchange, its admission submitted.
    Admit {
        session: SessionId,
        packet_id: InboundPacketId,
        exchange: ExchangeId,
        publication: PublicationId,
        qos: Qos,
        publish_origin: (ConnectionGeneration, u64),
        unmatched: bool,
        cost: Credits,
        in_flight: InFlight,
        inline_claims: InlineClaimPlans,
        dirty_targets: Option<alloc::boxed::Box<[SessionId]>>,
    },
    /// The admission of the exchange is known committed.
    Committed {
        slot: usize,
        admission: Finished,
    },
    /// A repeated active PUBLISH keeps the same operation and refreshes
    /// only the network occurrence to which its future ACK is correlated.
    Replay {
        slot: usize,
        publish_origin: (ConnectionGeneration, u64),
    },
    AckReady {
        slot: usize,
        admission: Finished,
        effect: EffectId,
        connection: ConnectionKey,
    },
    /// The answer of the exchange is known and waits for the answers of
    /// earlier publications of its connection (record 0050). `admission`
    /// is the operation the result ended, absent when the frame repeats a
    /// PUBREC; `publish_origin` is the new occurrence of a PUBLISH
    /// received again.
    Hold {
        slot: usize,
        admission: Option<Finished>,
        frame: OutboundFrame,
        resent: bool,
        publish_origin: Option<(ConnectionGeneration, u64)>,
    },
    /// The exchange ends with the operation it waited for, and gives its
    /// lease back.
    End {
        slot: usize,
        finished: Finished,
    },
    /// The operation in flight changes phase, and, after a resolution,
    /// deadline and the effects that answer it.
    Await {
        slot: usize,
        phase: CommandPhase,
        resolution: Option<(DeadlineToken, EffectId)>,
    },
    /// The durable termination of the exchange is submitted, after the
    /// one the store refused if there was one.
    ReleaseDeferred {
        slot: usize,
        refused: Finished,
    },
    ReleaseWake {
        continuation: ContinuationId,
    },
    ReleaseYield,
    Release {
        resumed: bool,
        slot: usize,
        in_flight: InFlight,
        refused: Option<Finished>,
    },
}

/// The effects of a plan: at most `MAX_TRANSITION_EFFECTS`, in the order of
/// their publication.
#[derive(Debug)]
struct Effects {
    items: [Option<OwnedEffect>; MAX_TRANSITION_EFFECTS],
    count: usize,
}

/// What a plan draws and publishes while it is computed.
#[derive(Debug)]
struct Draft {
    epoch: RunEpoch,
    owner: LogicalOwnerId,
    counters: Counters,
    effects: Effects,
    progress: Progress,
    closed: Option<ConnectionGeneration>,
    receipt_issued: Option<receipts::Entry>,
    receipt_consumed: Option<OperationId>,
    released: Option<Released>,
    inline_outcome: Option<InlineOutcome>,
    delegated_inline_resolved: Option<(OperationId, bool)>,
    non_inline_resolved: Option<(OperationId, bool)>,
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
        self.emit(Effect::EmitFrame {
            connection,
            frame,
            publish_order: None,
            pubrel: None,
        })
        .map(|_| ())
    }

    /// Captures correlation while planning, before an Outbox refusal or
    /// a later reuse can change the meaning of the Packet Identifier.
    fn publish_frame(
        &mut self,
        connection: ConnectionKey,
        frame: OutboundFrame,
        publish_order: Option<u64>,
    ) -> Result<EffectId, Stop> {
        self.emit(Effect::EmitFrame {
            connection,
            frame,
            publish_order,
            pubrel: None,
        })
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
/// #     session_qos2_exchanges: NonZeroU32::MAX,
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
/// #     session_qos2_exchanges: NonZeroU32::MAX,
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
    limits: Option<ShardLimits>,
    route_revision: Option<Revision>,
    routes: BoundedVec<Route>,
    route_index: routing::RouteIndex,
    route_by_key: BTreeMap<(SessionId, SharedBytes), usize>,
    sessions: BoundedVec<Session>,
    session_by_id: FlatMap<SessionId, usize>,
    session_by_client: BTreeMap<SharedBytes, usize>,
    session_by_conn: FlatMap<ConnectionGeneration, usize>,
    slots: BoundedSlab<Slot>,
    slot_by_key: FlatMap<(SessionId, InboundPacketId), usize>,
    slot_by_op: ScopedFlatMap<OperationId, usize>,
    slot_by_deadline: FlatMap<DeadlineToken, usize>,
    slot_by_effect: ScopedFlatMap<EffectId, usize>,
    slots_by_session: FlatMap<SessionId, SmallIndexSet>,
    held_answers: usize,
    /// Total slots currently in `AdmissionPending` or `AnswerHeld`.
    unanswered_slots: usize,
    /// Total slots currently in `AdmissionPending` (depend on `route_revision`).
    pending_admissions: usize,
    /// Set when the last applied `Input::ObservedPuback` did not match a live
    /// delivery on the connection, so the runtime can emit `puback_ignored`
    /// without a redundant pre-step `puback_target` lookup.
    last_observed_puback_ignored: bool,
    /// Conservative hint: false only when the last applied transition provably
    /// could not make new outbound pub/sub work ready for its target session.
    last_step_may_unblock_work: bool,
    /// If the last applied `PublicationAdmitted` / `RejectedCertain` resolved
    /// one or more inline claims (or unclaimed targets) that may still have
    /// ready work, holds the first target inline and any additional targets in
    /// `last_inline_ready_extra`.
    last_inline_ready_target: Option<SessionId>,
    last_inline_ready_extra: alloc::vec::Vec<SessionId>,
    /// See `OwnerConfig::session_qos2_exchanges`.
    session_qos2_exchanges: usize,
    release_retry_slots: BTreeSet<usize>,
    leases: LeaseTable,
    /// The latest operations that ended; see `OwnerConfig`.
    finished: BoundedQueue<Finished>,
    /// Optional lookup index used only when `retained_operations > 256`; for
    /// small rings (e.g. production `128`), `remember_finished` is an O(1)
    /// overwrite with zero hashing or cloning, and cold late-result lookups
    /// scan the small ring directly.
    finished_by_op: Option<ScopedFlatMap<OperationId, (Finished, u32)>>,
    /// The latest connections that were closed; see `OwnerConfig`.
    closed: BoundedQueue<ConnectionGeneration>,
    closed_set: BTreeSet<ConnectionGeneration>,
    resolution_delay: LogicalDuration,
    pubsub: pubsub::State,
    receipts: receipts::State,
    wills: will::State,
    lifecycle: Option<sessions::Flight>,
    lifecycle_enabled: bool,
    inline_claim_enabled: bool,
    delegated_deliveries: bool,
    delegated_inline_by_op: ScopedFlatMap<OperationId, DelegatedInlineBatch>,
    delegated_inline_by_session: FlatMap<SessionId, usize>,
    /// The delegated inline admissions whose outcome is unknown: their
    /// claims may still commit, and the committed claims of their targets
    /// wait for them (`Self::delegated_inline_unknown`).
    delegated_inline_unknown: FlatSet<OperationId>,
    /// On a delivery shard, the sessions it does not read from the store
    /// now: the coordinator may still owe them a committed inline claim
    /// (`Self::shard_fence_scans`).
    scan_fences: FlatSet<SessionId>,
    last_delegated_committed: Option<DelegatedInlineBatch>,
    last_delegated_rejected: Option<DelegatedInlineBatch>,
    last_delegated_qos0: Option<DelegatedQos0Batch>,
    last_finished_delivery: Option<(SessionId, Option<OutboundPacketId>)>,
    non_inline_targets_by_op:
        ScopedFlatMap<OperationId, (PublicationId, alloc::vec::Vec<SessionId>)>,
    message_now: Option<MessageTime>,
    release_continuation: Option<ContinuationId>,
    release_continuations_started: bool,
    /// Last priority control attempts confirmed absent from `outbox` so
    /// steady-state `Owner::dispatch` avoids rotating the entire outbox.
    receipt_priority_dispatched: Option<(EffectId, DeadlineToken)>,
    lifecycle_flight_dispatched: Option<(OperationId, DeadlineToken)>,
    will_flight_dispatched: Option<(OperationId, DeadlineToken)>,
    /// The one box of this owner. A renewal replaces it in place.
    outbox: Outbox,
}

fn table<T>(bound: NonZeroU32) -> Result<BoundedVec<T>, AllocationFailed> {
    BoundedVec::with_capacity(usize::try_from(bound.get()).map_err(|_| AllocationFailed)?)
}

fn slab<T>(bound: NonZeroU32) -> Result<BoundedSlab<T>, AllocationFailed> {
    BoundedSlab::with_capacity(usize::try_from(bound.get()).map_err(|_| AllocationFailed)?)
}

fn ring<T>(bound: NonZeroU32) -> Result<BoundedQueue<T>, AllocationFailed> {
    BoundedQueue::with_capacity(usize::try_from(bound.get()).map_err(|_| AllocationFailed)?)
}

fn capacity_of(bound: usize) -> u64 {
    u64::try_from(bound).unwrap_or(u64::MAX)
}

/// The facts of one kind read so far on a page, before `upto`. The order
/// of an inventory puts the facts of one kind together: they start where
/// the first of them was seen and end where a later kind starts.
fn run<'a>(
    facts: &'a [RecoveredFact],
    starts: &[Option<usize>; 9],
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
                slab(config.max_exchanges)?,
                LeaseTable::new(config.account, credit, config.max_exchanges)?,
                ring(config.retained_operations)?,
                ring(config.remembered_connections)?,
                pubsub::State::new(config)?,
                receipts::State::new(crate::contract::receipts::ReceiptOrigin {
                    incarnation: id.incarnation(),
                    epoch: id.epoch(),
                    owner: id.owner(),
                })?,
            ))
        })();
        let Ok((routes, sessions, slots, leases, finished, closed, pubsub, receipts)) = tables
        else {
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
            route_index: routing::RouteIndex::default(),
            route_by_key: BTreeMap::new(),
            sessions,
            session_by_id: FlatMap::new(),
            session_by_client: BTreeMap::new(),
            session_by_conn: FlatMap::new(),
            slots,
            slot_by_key: FlatMap::new(),
            slot_by_op: ScopedFlatMap::with_scope((id.incarnation(), id.epoch(), id.owner())),
            slot_by_deadline: FlatMap::new(),
            slot_by_effect: ScopedFlatMap::with_scope((id.epoch(), id.owner())),
            slots_by_session: FlatMap::new(),
            held_answers: 0,
            unanswered_slots: 0,
            pending_admissions: 0,
            last_observed_puback_ignored: false,
            last_step_may_unblock_work: true,
            last_inline_ready_target: None,
            last_inline_ready_extra: alloc::vec::Vec::new(),
            session_qos2_exchanges: usize::try_from(config.session_qos2_exchanges.get())
                .unwrap_or(usize::MAX),
            release_retry_slots: BTreeSet::new(),
            leases,
            finished,
            finished_by_op: (config.retained_operations.get() > 256)
                .then(|| ScopedFlatMap::with_scope((id.incarnation(), id.epoch(), id.owner()))),
            closed,
            closed_set: BTreeSet::new(),
            resolution_delay: config.resolution_delay,
            pubsub,
            receipts,
            wills: will::State::new(),
            lifecycle: None,
            lifecycle_enabled: false,
            inline_claim_enabled: false,
            delegated_deliveries: false,
            delegated_inline_by_op: ScopedFlatMap::with_scope((
                id.incarnation(),
                id.epoch(),
                id.owner(),
            )),
            delegated_inline_by_session: FlatMap::new(),
            delegated_inline_unknown: FlatSet::new(),
            scan_fences: FlatSet::new(),
            last_delegated_committed: None,
            last_delegated_rejected: None,
            last_delegated_qos0: None,
            last_finished_delivery: None,
            non_inline_targets_by_op: ScopedFlatMap::with_scope((
                id.incarnation(),
                id.epoch(),
                id.owner(),
            )),
            message_now: None,
            release_continuation: None,
            release_continuations_started: false,
            receipt_priority_dispatched: None,
            lifecycle_flight_dispatched: None,
            will_flight_dispatched: None,
            outbox,
        })
    }

    /// Enables retained publication and session-lifecycle admission paths on an
    /// owner whose sessions were pre-installed from a fixture rather than opened
    /// via `Input::OpenSession`.
    pub fn enable_session_lifecycle(&mut self) {
        self.lifecycle_enabled = true;
    }

    /// Enables atomic online admission + claim (`Recipe::ClaimedPublication`)
    /// when a live publication targets a single caught-up online subscriber.
    pub fn enable_inline_claims(&mut self) {
        self.inline_claim_enabled = true;
    }

    /// The box of the owner. It is always present: a renewal replaces it
    /// in place, and a refused renewal keeps it (review R2).
    #[must_use]
    pub fn outbox(&self) -> &Outbox {
        &self.outbox
    }

    /// Hands effects to `sink` with the receipt and lifecycle control lanes.
    /// A transport refusal preserves write order on its generation.
    pub fn dispatch(&mut self, sink: &mut dyn EffectSink) -> DispatchReport {
        if self.outbox.is_empty() {
            return DispatchReport::default();
        }
        let priority = self.receipts.priority().and_then(|key| {
            if !self.outbox.has_receipt_control() && self.receipt_priority_dispatched == Some(key) {
                return None;
            }
            let report = self.outbox.dispatch_receipt_controls(sink, key.0, key.1);
            if report.blocked.is_none() {
                self.receipt_priority_dispatched = Some(key);
            }
            Some(report)
        });
        let pending_route_admissions = self.has_pending_route_admissions();
        let lifecycle = if self.lifecycle_enabled
            && !(pending_route_admissions && self.lifecycle_mutates_routes())
        {
            let flight = self.lifecycle_dispatch();
            if !self.outbox.has_session_control()
                && (flight.is_none() || self.lifecycle_flight_dispatched == flight)
            {
                None
            } else {
                let report = self.outbox.dispatch_session_controls(sink, flight);
                if report.blocked.is_none() {
                    self.lifecycle_flight_dispatched = flight;
                }
                Some(report)
            }
        } else {
            None
        };
        let will = if (self.will_status().reading || self.will_status().admission_pending)
            && !pending_route_admissions
        {
            if lifecycle
                .as_ref()
                .is_none_or(|control| control.blocked.is_none())
            {
                let flight = self.will_dispatch();
                if !self.outbox.has_will_control()
                    && (flight.is_none() || self.will_flight_dispatched == flight)
                {
                    None
                } else {
                    let report = self.outbox.dispatch_will_controls(sink, flight);
                    if report.blocked.is_none() {
                        self.will_flight_dispatched = flight;
                    }
                    Some(report)
                }
            } else {
                None
            }
        } else {
            None
        };
        let blocked = lifecycle
            .as_ref()
            .and_then(|control| control.blocked)
            .or_else(|| will.as_ref().and_then(|control| control.blocked));
        let mut report = if blocked.is_some() {
            DispatchReport {
                accepted: 0,
                blocked,
            }
        } else {
            self.outbox.dispatch(sink)
        };
        if let Some(control) = lifecycle {
            report.accepted += control.accepted;
        }
        if let Some(control) = will {
            report.accepted += control.accepted;
        }
        if let Some(priority) = priority {
            report.accepted += priority.accepted;
        }
        report
    }

    /// Replaces the box of the owner under `authority` by a new one for
    /// the same identity, at once (ADR 0009, review R2): the owner keeps a
    /// box and its identity stays taken throughout. The counters and the
    /// state of the owner are kept. On any refusal the owner keeps its box
    /// and the authority is unchanged; the replacement is refused while an
    /// effect waits or a reservation lives.
    pub fn renew_box(
        &mut self,
        authority: &mut OwnerAuthority,
        capacity: NonZeroUsize,
        max_reservations: NonZeroU32,
    ) -> Result<(), RenewRefused> {
        if authority.incarnation() != self.incarnation || authority.epoch() != self.epoch {
            return Err(RenewRefused::ForeignAuthority);
        }
        let needed = MAX_TRANSITION_SLOTS + if self.receipts.enabled { 2 } else { 0 };
        if capacity.get() < needed {
            return Err(RenewRefused::BoxTooSmall {
                capacity: capacity.get(),
                needed,
            });
        }
        authority
            .replace_outbox(&mut self.outbox, capacity, max_reservations)
            .map_err(|refusal| match refusal {
                ReplaceRefusal::Retire(reason) => RenewRefused::Retire(reason),
                ReplaceRefusal::Create(reason) => RenewRefused::Create(reason),
            })?;
        self.receipt_priority_dispatched = None;
        self.lifecycle_flight_dispatched = None;
        self.will_flight_dispatched = None;
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
        self.session_by_id
            .get(&session)
            .and_then(|&idx| self.sessions.as_slice().get(idx))
            .and_then(|held| held.connection)
    }

    /// The session bound to `connection`, if any, in O(log S).
    #[must_use]
    pub fn session_for_connection(&self, connection: ConnectionGeneration) -> Option<SessionId> {
        self.session_by_conn
            .get(&connection)
            .and_then(|&idx| self.sessions.as_slice().get(idx))
            .map(|held| held.id)
    }

    /// Every session that has a connection, with that connection: what
    /// [`Owner::connection_of`] answers for each of them, in one pass.
    pub fn connections(&self) -> impl Iterator<Item = (SessionId, ConnectionGeneration)> + '_ {
        self.session_by_conn
            .iter()
            .filter_map(|(&connection, &idx)| {
                self.sessions
                    .as_slice()
                    .get(idx)
                    .map(|held| (held.id, connection))
            })
    }

    fn is_closed(&self, connection: ConnectionGeneration) -> bool {
        self.closed_set.contains(&connection)
    }

    fn remember_closed(&mut self, connection: ConnectionGeneration) -> Result<(), CoreFault> {
        if self.closed.len() == self.closed.capacity()
            && let Some(evicted) = self.closed.pop_front()
            && !self.closed.iter().any(|&g| g == evicted)
        {
            self.closed_set.remove(&evicted);
        }
        self.closed
            .try_push_back(connection)
            .map_err(|_| CoreFault::ReservationOverrun)?;
        self.closed_set.insert(connection);
        Ok(())
    }

    fn remember_finished(&mut self, item: Finished) -> Result<(), CoreFault> {
        let Some(index) = self.finished_by_op.as_mut() else {
            if self.finished.len() == self.finished.capacity() {
                let _ = self.finished.pop_front();
            }
            return self
                .finished
                .try_push_back(item)
                .map_err(|_| CoreFault::ReservationOverrun);
        };
        if self.finished.len() == self.finished.capacity()
            && let Some(evicted) = self.finished.pop_front()
            && let Some((_, count)) = index.get_mut(&evicted.operation)
        {
            *count = count.saturating_sub(1);
            if *count == 0 {
                index.remove(&evicted.operation);
            }
        }
        let op = item.operation;
        self.finished
            .try_push_back(item.clone())
            .map_err(|_| CoreFault::ReservationOverrun)?;
        match index.get_mut(&op) {
            Some((stored, count)) => {
                *stored = item;
                *count = count.saturating_add(1);
            }
            None => {
                index.insert(op, (item, 1));
            }
        }
        Ok(())
    }

    fn push_route(&mut self, route: Route) -> Result<(), CoreFault> {
        let index = self.routes.len();
        let key = (route.target, route.topic.clone());
        self.routes
            .try_push(route)
            .map_err(|_| CoreFault::StalePlan)?;
        self.route_index
            .insert(index, &self.routes.as_slice()[index]);
        self.route_by_key.insert(key, index);
        Ok(())
    }

    fn rebuild_route_index(&mut self) {
        self.route_index.rebuild(self.routes.as_slice());
        self.route_by_key.clear();
        for (index, route) in self.routes.as_slice().iter().enumerate() {
            self.route_by_key
                .insert((route.target, route.topic.clone()), index);
        }
    }

    fn index_session(&mut self, index: usize) {
        let session = &self.sessions.as_slice()[index];
        self.session_by_id.insert(session.id, index);
        self.session_by_client
            .insert(session.client_id.clone(), index);
        if let Some(conn) = session.connection {
            self.session_by_conn.insert(conn, index);
        }
    }

    fn unindex_session(&mut self, index: usize) {
        let session = &self.sessions.as_slice()[index];
        self.session_by_id.remove(&session.id);
        self.session_by_client.remove(&session.client_id);
        if let Some(conn) = session.connection {
            self.session_by_conn.remove(&conn);
        }
    }

    fn push_session(&mut self, session: Session) -> Result<(), CoreFault> {
        let index = self.sessions.len();
        self.sessions
            .try_push(session)
            .map_err(|_| CoreFault::StalePlan)?;
        self.index_session(index);
        Ok(())
    }

    fn replace_session(&mut self, index: usize, session: Session) -> Result<(), CoreFault> {
        if index >= self.sessions.len() {
            return Err(CoreFault::StalePlan);
        }
        self.unindex_session(index);
        self.sessions.as_mut_slice()[index] = session;
        self.index_session(index);
        Ok(())
    }

    fn set_session_connection(
        &mut self,
        index: usize,
        connection: Option<ConnectionGeneration>,
    ) -> Result<(), CoreFault> {
        let held = self
            .sessions
            .as_mut_slice()
            .get_mut(index)
            .ok_or(CoreFault::StalePlan)?;
        if let Some(old) = held.connection {
            self.session_by_conn.remove(&old);
        }
        held.connection = connection;
        if let Some(new) = connection {
            self.session_by_conn.insert(new, index);
        }
        Ok(())
    }

    fn remove_session_at(&mut self, index: usize) -> Result<Session, CoreFault> {
        let last = self
            .sessions
            .len()
            .checked_sub(1)
            .ok_or(CoreFault::StalePlan)?;
        if index > last {
            return Err(CoreFault::StalePlan);
        }
        self.unindex_session(index);
        if index != last {
            self.unindex_session(last);
            self.sessions.as_mut_slice().swap(index, last);
        }
        let removed = self.sessions.pop().ok_or(CoreFault::StalePlan)?;
        if self
            .slots_by_session
            .get(&removed.id)
            .is_some_and(SmallIndexSet::is_empty)
        {
            self.slots_by_session.remove(&removed.id);
        }
        self.pubsub.drop_session_cache(removed.id);
        if index != last {
            self.index_session(index);
        }
        Ok(removed)
    }

    /// Whether any pending admission or subscription update depends on the
    /// current `route_revision` and target session set.
    fn has_pending_route_admissions(&self) -> bool {
        self.pending_admissions > 0
            || self.pubsub.retained.is_some()
            || self.pubsub.control_pending()
    }

    /// Whether the active lifecycle command may remove a session or advance
    /// `route_revision` in the store.
    fn lifecycle_mutates_routes(&self) -> bool {
        self.lifecycle.as_ref().is_some_and(|flight| {
            matches!(
                &flight.command.action,
                crate::contract::session::SessionAction::Close { .. }
                    | crate::contract::session::SessionAction::EndWill {
                        expiry: crate::contract::session::SessionExpiry::OnDisconnect,
                        ..
                    }
                    | crate::contract::session::SessionAction::Expire { .. }
                    | crate::contract::session::SessionAction::Open {
                        expected: Some(_),
                        clean_start: true,
                        ..
                    }
                    | crate::contract::session::SessionAction::OpenAt {
                        expected: Some(_),
                        clean_start: true,
                        ..
                    }
                    | crate::contract::session::SessionAction::OpenWillAt {
                        expected: Some(_),
                        clean_start: true,
                        ..
                    }
            )
        })
    }

    fn index_slot_state(&mut self, index: usize) {
        let slot = &self.slots[index];
        let session = slot.session;
        let packet_id = slot.packet_id;
        match &slot.state {
            SlotState::AckPending { effect, .. } => {
                self.slot_by_effect.insert(*effect, index);
            }
            SlotState::AdmissionPending(f) => {
                let op = f.operation;
                let dl = f.deadline;
                self.slot_by_key.insert((session, packet_id), index);
                self.slot_by_op.insert(op, index);
                self.slot_by_deadline.insert(dl, index);
                self.unanswered_slots += 1;
                self.pending_admissions += 1;
            }
            SlotState::ReleasePending(f) => {
                let op = f.operation;
                let dl = f.deadline;
                self.slot_by_key.insert((session, packet_id), index);
                self.slot_by_op.insert(op, index);
                self.slot_by_deadline.insert(dl, index);
            }
            SlotState::ReleaseRetry(_) => {
                self.slot_by_key.insert((session, packet_id), index);
                self.release_retry_slots.insert(index);
            }
            SlotState::AnswerHeld { .. } => {
                self.slot_by_key.insert((session, packet_id), index);
                self.held_answers += 1;
                self.unanswered_slots += 1;
            }
            SlotState::AwaitPubrel => {
                self.slot_by_key.insert((session, packet_id), index);
            }
        }
    }

    fn unindex_slot_state(&mut self, index: usize) {
        let slot = &self.slots[index];
        let session = slot.session;
        let packet_id = slot.packet_id;
        match &slot.state {
            SlotState::AckPending { effect, .. } => {
                let effect = *effect;
                self.slot_by_effect.remove(&effect);
            }
            SlotState::AdmissionPending(f) => {
                let op = f.operation;
                let dl = f.deadline;
                self.slot_by_key.remove(&(session, packet_id));
                self.slot_by_op.remove(&op);
                self.slot_by_deadline.remove(&dl);
                self.unanswered_slots = self.unanswered_slots.saturating_sub(1);
                self.pending_admissions = self.pending_admissions.saturating_sub(1);
            }
            SlotState::ReleasePending(f) => {
                let op = f.operation;
                let dl = f.deadline;
                self.slot_by_key.remove(&(session, packet_id));
                self.slot_by_op.remove(&op);
                self.slot_by_deadline.remove(&dl);
            }
            SlotState::ReleaseRetry(_) => {
                self.slot_by_key.remove(&(session, packet_id));
                self.release_retry_slots.remove(&index);
            }
            SlotState::AnswerHeld { .. } => {
                self.slot_by_key.remove(&(session, packet_id));
                self.held_answers = self.held_answers.saturating_sub(1);
                self.unanswered_slots = self.unanswered_slots.saturating_sub(1);
            }
            SlotState::AwaitPubrel => {
                self.slot_by_key.remove(&(session, packet_id));
            }
        }
    }

    fn index_slot(&mut self, index: usize) {
        let session = self.slots[index].session;
        self.slots_by_session
            .get_or_insert_default(session)
            .insert(index);
        self.index_slot_state(index);
    }

    fn unindex_slot(&mut self, index: usize) {
        let session = self.slots[index].session;
        if let Some(set) = self.slots_by_session.get_mut(&session) {
            set.remove(&index);
        }
        self.unindex_slot_state(index);
    }

    fn push_slot(&mut self, slot: Slot) -> Result<(), CoreFault> {
        let index = self.slots.insert(slot).map_err(|_| CoreFault::StalePlan)?;
        self.index_slot(index);
        Ok(())
    }

    fn remove_slot_at(&mut self, index: usize) -> Result<Slot, CoreFault> {
        if self.slots.get(index).is_none() {
            return Err(CoreFault::StalePlan);
        }
        self.unindex_slot(index);
        self.slots.remove(index).ok_or(CoreFault::StalePlan)
    }

    /// The exchange the session holds under this Packet Identifier, and
    /// its state; `None` is the `Absent` of the registry.
    #[must_use]
    pub fn exchange(
        &self,
        session: SessionId,
        packet_id: InboundPacketId,
    ) -> Option<(ExchangeId, ExchangePhase)> {
        let slot = self.slots.get(self.slot_of(session, packet_id)?)?;
        let phase = match slot.state {
            SlotState::AdmissionPending(_) => ExchangePhase::AdmissionPending,
            SlotState::AwaitPubrel => ExchangePhase::AwaitPubrel,
            SlotState::ReleasePending(_) | SlotState::ReleaseRetry(_) => {
                ExchangePhase::ReleasePending
            }
            SlotState::AnswerHeld { .. } => ExchangePhase::AnswerHeld,
            SlotState::AckPending { .. } => return None,
        };
        Some((slot.exchange, phase))
    }

    /// Whether any inbound exchange currently has an answer held behind an
    /// earlier unanswered PUBLISH on the same connection.
    #[must_use]
    pub fn has_held_answers(&self) -> bool {
        self.held_answers != 0
    }

    /// Whether the last applied transition could have unblocked outbound
    /// pub/sub work (returns `false` only when provably no work became ready).
    #[must_use]
    pub fn last_step_may_unblock_work(&self) -> bool {
        self.last_step_may_unblock_work
    }

    /// First target session of an inline-claimed admission whose resolution may
    /// have left ready work to re-arm.
    #[must_use]
    pub fn last_inline_ready_target(&self) -> Option<SessionId> {
        self.last_inline_ready_target
    }

    /// Additional target sessions (beyond [`Self::last_inline_ready_target`])
    /// from a multi-target admission whose resolution may have left ready work.
    #[must_use]
    pub fn last_inline_ready_extra(&self) -> &[SessionId] {
        &self.last_inline_ready_extra
    }

    fn push_inline_ready_target(&mut self, session: SessionId) {
        if self.last_inline_ready_target.is_none() {
            self.last_inline_ready_target = Some(session);
        } else {
            self.last_inline_ready_extra.push(session);
        }
    }

    /// Whether the owner's effect outbox is currently empty.
    #[must_use]
    pub fn outbox_is_empty(&self) -> bool {
        self.outbox.is_empty()
    }

    /// Whether the last applied `Input::ObservedPuback` did not match any
    /// current delivery occurrence on its connection.
    #[must_use]
    pub fn last_observed_puback_ignored(&self) -> bool {
        self.last_observed_puback_ignored
    }

    /// Applies `input` entirely, or hands it back intact (DEM-006).
    ///
    /// A refusal keeps the input, the state, the identity counters, the
    /// account, the reservations held and the content of the box as they
    /// were.
    pub fn step(&mut self, input: Input, grant: TurnGrant) -> StepOutcome {
        self.step_with_sink_and_oracle(input, grant, None, None).0
    }

    /// Applies `input` and, when the outbox is empty and has no live
    /// reservations, dispatches the transition's effects straight into `sink`
    /// without passing them through the outbox queue. Refused effects fall back
    /// into the outbox in exact FIFO order.
    pub fn step_and_dispatch(
        &mut self,
        input: Input,
        grant: TurnGrant,
        sink: &mut dyn EffectSink,
    ) -> (StepOutcome, usize) {
        self.step_with_sink_and_oracle(input, grant, Some(sink), None)
    }

    pub(super) fn step_with_sink_and_oracle<'o>(
        &mut self,
        input: Input,
        grant: TurnGrant,
        sink: Option<&mut dyn EffectSink>,
        mut oracle: Option<&mut (dyn InlineClaimOracle + 'o)>,
    ) -> (StepOutcome, usize) {
        self.last_observed_puback_ignored = false;
        self.last_step_may_unblock_work = true;
        self.last_inline_ready_target = None;
        if !self.last_inline_ready_extra.is_empty() {
            self.last_inline_ready_extra.clear();
        }
        self.last_delegated_committed = None;
        self.last_delegated_rejected = None;
        self.last_delegated_qos0 = None;
        self.last_finished_delivery = None;
        let planned_work = self.cost(&input);
        if let Some(missing) = planned_work.checked_sub(grant.work_limit)
            && missing > WorkUnits::ZERO
        {
            return (
                StepOutcome::NeedCapacity {
                    input,
                    need: CapacityNeed {
                        resource: Resource::Work,
                        amount: missing.get(),
                        wake: WakeCondition::TurnGranted,
                    },
                    work_used: WorkUnits::ZERO,
                },
                0,
            );
        }
        let mut draft = Draft {
            epoch: self.epoch,
            owner: self.id,
            counters: self.counters,
            effects: Effects {
                items: [None, None, None],
                count: 0,
            },
            progress: Progress::Complete,
            closed: None,
            receipt_issued: None,
            receipt_consumed: None,
            released: None,
            inline_outcome: None,
            delegated_inline_resolved: None,
            non_inline_resolved: None,
        };
        let change = match if self.revision.checked_next().is_ok() {
            self.plan_change(&input, &mut draft, &mut oracle)
        } else {
            Err(Stop::Exhausted(Exhaustion::IdentityCounter))
        } {
            Ok(change) => change,
            Err(stop) => {
                return (
                    match stop {
                        Stop::Need(need) => StepOutcome::NeedCapacity {
                            input,
                            need,
                            work_used: planned_work,
                        },
                        Stop::Exhausted(exhaustion) => StepOutcome::Exhausted {
                            input,
                            exhaustion,
                            work_used: planned_work,
                        },
                        Stop::Fatal(fault) => StepOutcome::Fatal(fault),
                    },
                    0,
                );
            }
        };
        let cleanup = if draft.closed.is_some() {
            capacity_of(self.slots.capacity())
                .min(32)
                .saturating_mul(3)
                .saturating_add(capacity_of(self.sessions.capacity()).min(16))
                .saturating_add(self.pubsub.close_work())
        } else {
            0
        };
        let work_used = WorkUnits::new(planned_work.get().saturating_add(cleanup));
        if let Some(missing) = work_used.checked_sub(grant.work_limit)
            && missing > WorkUnits::ZERO
        {
            Self::rollback_delegated_change(&change, &mut oracle);
            return (
                StepOutcome::NeedCapacity {
                    input,
                    need: CapacityNeed {
                        resource: Resource::Work,
                        amount: missing.get(),
                        wake: WakeCondition::TurnGranted,
                    },
                    work_used: planned_work,
                },
                0,
            );
        }
        if self.receipts.enabled
            && !self.receipts.maintenance_input(&input)
            && draft.effects.count > 0
            && self.outbox.free_slots() < draft.effects.count.saturating_add(2)
        {
            Self::rollback_delegated_change(&change, &mut oracle);
            return (
                StepOutcome::NeedCapacity {
                    input,
                    need: CapacityNeed {
                        resource: Resource::EffectSlots,
                        amount: u64::try_from(
                            draft.effects.count.saturating_add(2) - self.outbox.free_slots(),
                        )
                        .unwrap_or(u64::MAX),
                        wake: WakeCondition::EffectAccepted,
                    },
                    work_used: planned_work,
                },
                0,
            );
        }
        let direct_sink =
            sink.filter(|_| self.outbox.is_empty() && !self.outbox.has_live_reservations());
        let reservation = match (
            NonZeroUsize::new(draft.effects.count),
            direct_sink.is_some(),
        ) {
            (None, _) => None,
            (Some(slots), true) => {
                if let Err(refusal) = self.outbox.check_reserve(slots) {
                    Self::rollback_delegated_change(&change, &mut oracle);
                    return (
                        StepOutcome::reservation_refused(input, refusal, planned_work),
                        0,
                    );
                }
                None
            }
            (Some(slots), false) => match self.outbox.reserve(slots) {
                Ok(key) => Some(key),
                Err(refusal) => {
                    Self::rollback_delegated_change(&change, &mut oracle);
                    return (
                        StepOutcome::reservation_refused(input, refusal, planned_work),
                        0,
                    );
                }
            },
        };
        if matches!(input, Input::ObservedPuback { .. })
            && matches!(change, Change::Nothing)
            && draft.closed.is_none()
        {
            self.last_observed_puback_ignored = true;
        }
        match self.apply_draft(
            &mut draft,
            change,
            input,
            reservation,
            direct_sink,
            &mut oracle,
        ) {
            Ok((progress, accepted)) => (
                StepOutcome::Applied {
                    work_used,
                    progress,
                },
                accepted,
            ),
            Err(fault) => (StepOutcome::Fatal(fault), 0),
        }
    }

    fn rollback_delegated_change<'o>(
        change: &Change,
        oracle: &mut Option<&mut (dyn InlineClaimOracle + 'o)>,
    ) {
        if let Some(oracle) = oracle.as_deref_mut()
            && let Change::Admit { inline_claims, .. } = change
        {
            match inline_claims {
                InlineClaimPlans::None => {}
                InlineClaimPlans::One(plan) => oracle.rollback(plan.target, plan.packet_id),
                InlineClaimPlans::Many(plans) => {
                    for plan in plans {
                        oracle.rollback(plan.target, plan.packet_id);
                    }
                }
            }
        }
    }

    /// The work an input costs: one unit, plus bounded pub/sub table work
    /// when the product profile is enabled. For a recovery page, a bound
    /// of the comparisons its checks make. A fact is compared with the
    /// table it joins: a route with the routes, a session with the
    /// sessions, an exchange with the sessions and the exchanges. The page
    /// that closes the inventory compares every route with every session.
    fn cost(&self, input: &Input) -> WorkUnits {
        if matches!(input, Input::MessageClock { .. }) {
            return WorkUnits::ZERO;
        }
        let Input::Recovery { facts, last } = input else {
            return WorkUnits::new(
                1u64.saturating_add(self.pubsub.work_bound(input))
                    .saturating_add(self.session_work_bound(input))
                    .saturating_add(self.routing_work_bound(input))
                    .saturating_add(self.retained_work_bound(input))
                    .saturating_add(self.receipts.work(input))
                    .saturating_add(self.will_work_bound(input))
                    .saturating_add(
                        if self.receipts.enabled
                            && matches!(input, Input::MaintainReceipts | Input::Continue { .. })
                        {
                            16
                        } else {
                            0
                        },
                    ),
            );
        };
        let routes = capacity_of(self.routes.capacity());
        let sessions = capacity_of(self.sessions.capacity());
        let slots = capacity_of(self.slots.capacity());
        let mut work: u64 = 0;
        for fact in facts.as_slice() {
            let compared = match fact.rank() {
                FactRank::Route => routes,
                FactRank::Session => sessions,
                FactRank::InboundExchange | FactRank::OutboundDelivery => {
                    sessions.saturating_add(slots)
                }
                FactRank::Epoch
                | FactRank::Limits
                | FactRank::RouteTable
                | FactRank::ReceiptFrontier
                | FactRank::Operation => 0,
            };
            work = work.saturating_add(compared.saturating_add(1));
        }
        if *last {
            work = work.saturating_add(routes.saturating_mul(sessions).saturating_add(1));
        }
        WorkUnits::new(work)
    }

    fn plan_change<'o>(
        &self,
        input: &Input,
        draft: &mut Draft,
        oracle: &mut Option<&mut (dyn InlineClaimOracle + 'o)>,
    ) -> Result<Change, Stop> {
        if let Input::MessageClock { now } = input {
            if self
                .message_now
                .is_some_and(|old| old.domain == now.domain && old.tick > now.tick)
            {
                return Err(IMPOSSIBLE);
            }
            if !self.pubsub.accepts_message_time(*now) {
                return Err(IMPOSSIBLE);
            }
            return Ok(Change::MessageClock(*now));
        }
        if self.recovery.is_some() {
            return match input {
                Input::Recovery { facts, last } => self.plan_recovery(facts.as_slice(), *last),
                _ => Err(IMPOSSIBLE),
            };
        }
        match input {
            Input::OpenSessionWillAt {
                connection,
                client_id,
                clean_start,
                expiry,
                now,
                will,
            } => self.plan_session_will_open(
                *connection,
                client_id,
                *clean_start,
                *expiry,
                *now,
                will.as_ref(),
                draft,
            ),
            Input::EndSessionWill {
                connection,
                opened_by,
                expiry,
                observed_at,
                publish,
            } => self.plan_session_will_end(
                *connection,
                *opened_by,
                *expiry,
                *observed_at,
                *publish,
                draft,
            ),
            Input::DiscoverWills => self.plan_will_discover(draft),
            Input::WillPage {
                effect,
                items,
                next,
            } => self.plan_will_page(*effect, items.as_slice(), *next, draft),
            Input::MessageClock { .. } => Err(IMPOSSIBLE),
            Input::MessagesExpired { deliveries } => {
                self.plan_messages_expired(deliveries.as_slice())
            }
            Input::MaintainReceipts => self.plan_receipt_maintenance(draft),
            Input::OpenSession {
                connection,
                client_id,
                clean_start,
                expiry,
            } => self.plan_session_open(
                *connection,
                client_id,
                *clean_start,
                *expiry,
                None,
                None,
                draft,
            ),
            Input::OpenSessionAt {
                connection,
                client_id,
                clean_start,
                expiry,
                now,
            } => self.plan_session_open(
                *connection,
                client_id,
                *clean_start,
                *expiry,
                Some(*now),
                None,
                draft,
            ),
            Input::DetachSession {
                connection,
                opened_by,
                expiry,
                observed_at,
            } => self.plan_session_detach(*connection, *opened_by, *expiry, *observed_at, draft),
            Input::ExpireSession {
                guard,
                detached,
                now,
            } => self.plan_session_expire(*guard, *detached, *now, draft),
            Input::EndSession {
                connection,
                opened_by,
                reason,
            } => self.plan_session_end(*connection, *opened_by, *reason, draft),
            Input::ReceiptsCollected {
                effect,
                collection,
                reply,
            } => self.plan_receipt_result(*effect, *collection, *reply),
            Input::Qos0EmitRefused {
                effect,
                connection,
                reason,
            } => self.plan_qos0_refusal(*effect, *connection, *reason, draft),
            Input::ConnectionSettings {
                connection,
                receive_maximum,
                maximum_packet_size,
            } => self.plan_settings(*connection, *receive_maximum, *maximum_packet_size, draft),
            Input::WorkPage {
                effect,
                session,
                items,
                next,
            } => self.plan_work_page(*effect, *session, items.as_slice(), *next),
            Input::SessionEstablished {
                connection,
                session,
                client_id,
                revision,
                session_present,
            } => self.plan_established(
                *connection,
                *session,
                client_id,
                *revision,
                *session_present,
                draft,
            ),
            Input::ConnectionClosed { connection } => {
                self.plan_connection_closed(*connection, draft)
            }
            Input::Continue { continuation, .. } => self.plan_continue(*continuation, draft),
            Input::Recovery { .. } => Err(IMPOSSIBLE),
            // A wake-up carries no authority, and this perimeter keeps no
            // work behind one.
            Input::WorkAvailable { session } => self.plan_work(*session, draft),
            Input::ObservedPuback {
                connection,
                packet_id,
                reason,
                target,
            } => self.plan_observed_puback(*connection, *packet_id, *reason, *target, draft),
            Input::ObservedQos2Ack {
                connection,
                kind,
                packet_id,
                reason,
                target,
            } => {
                self.plan_observed_qos2_ack(*connection, *kind, *packet_id, *reason, *target, draft)
            }
            Input::Packet {
                connection,
                ingress_order,
                packet,
            } => self.plan_packet(*connection, *ingress_order, packet, draft, oracle),
            Input::StoreResult {
                effect,
                operation,
                reply,
            } => self.plan_result(*effect, *operation, reply, draft),
            Input::Deadline { token, .. } => self.plan_deadline(*token, draft),
            Input::FrameResult {
                effect,
                connection,
                progress,
            } => self.plan_frame_result(*effect, *connection, *progress, draft),
        }
    }

    fn session_of(&self, connection: ConnectionGeneration) -> Option<usize> {
        self.session_by_conn.get(&connection).copied()
    }

    fn slot_of(&self, session: SessionId, packet_id: InboundPacketId) -> Option<usize> {
        self.slot_by_key.get(&(session, packet_id)).copied()
    }

    /// Installs only an exclusive writer's confirmed fact. The adapter
    /// correlates it with its session request before producing this input;
    /// no network request alone establishes a durable session.
    fn plan_established(
        &self,
        connection: Option<ConnectionGeneration>,
        session: SessionId,
        client_id: &SharedBytes,
        revision: Revision,
        session_present: bool,
        draft: &mut Draft,
    ) -> Result<Change, Stop> {
        let known = self
            .session_by_id
            .get(&session)
            .copied()
            .or_else(|| self.session_by_client.get(client_id).copied());
        if let Some(index) = known {
            let held = &self.sessions.as_slice()[index];
            if held.id != session || held.client_id != *client_id || held.revision != revision {
                return Err(IMPOSSIBLE);
            }
        }
        if let Some(connection) = connection
            && self
                .session_of(connection)
                .is_some_and(|index| Some(index) != known)
        {
            // One transport generation cannot denote two sessions.
            return Err(IMPOSSIBLE);
        }
        let connection = connection.filter(|connection| !self.is_closed(*connection));
        if let Some(index) = known {
            let held = &self.sessions.as_slice()[index];
            let Some(connection) = connection else {
                // A completed creation on a closed transport still joins
                // durable knowledge, but never unbinds a newer connection.
                return Ok(Change::Nothing);
            };
            if held.connection == Some(connection) {
                // Replaying the same confirmation cannot emit a second
                // CONNACK, including the result of a fresh creation.
                return Ok(Change::Nothing);
            }
            if !session_present {
                return Err(IMPOSSIBLE);
            }
            if let Some(previous) = held.connection {
                draft.close(previous, CloseReason::SessionTakenOver)?;
            }
            draft.frame(
                ConnectionKey {
                    session,
                    generation: connection,
                },
                OutboundFrame::Connack {
                    session_present: true,
                },
            )?;
            return Ok(Change::Bind {
                session: index,
                connection,
            });
        }
        // Product startup must keep the writer's session bound coherent
        // with this table. Sessions do not leave it in this perimeter, so
        // no resource-release event can make an excess fact admissible.
        if self.sessions.len() == self.sessions.capacity() {
            return Err(IMPOSSIBLE);
        }
        if let Some(generation) = connection {
            draft.frame(
                ConnectionKey {
                    session,
                    generation,
                },
                OutboundFrame::Connack { session_present },
            )?;
        }
        Ok(Change::Establish {
            session: alloc::boxed::Box::new(Session {
                id: session,
                client_id: client_id.clone(),
                revision,
                connection,
                expiry: crate::contract::session::SessionExpiry::Never,
                opened_by: None,
                detached: None,
                will_managed: false,
            }),
        })
    }

    /// The transport already closed: remember it without requesting a
    /// second close or discarding an operation the store may still commit.
    fn plan_connection_closed(
        &self,
        connection: ConnectionGeneration,
        draft: &mut Draft,
    ) -> Result<Change, Stop> {
        if let Some(change) = self.plan_session_transport_closed(connection, draft)? {
            return Ok(change);
        }
        if self.is_closed(connection) {
            return Ok(Change::Nothing);
        }
        draft.end(connection)?;
        Ok(self
            .session_of(connection)
            .map_or(Change::Nothing, |session| Change::Unbind { session }))
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

    fn plan_packet<'o>(
        &self,
        connection: ConnectionGeneration,
        ingress_order: u64,
        packet: &InboundPacket,
        draft: &mut Draft,
        oracle: &mut Option<&mut (dyn InlineClaimOracle + 'o)>,
    ) -> Result<Change, Stop> {
        // The owner ordered this connection closed, or saw it fail: what
        // it still carries changes nothing and is not answered.
        if self.is_closed(connection) {
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
            let known = self.session_by_client.get(client_id).copied();
            let Some(index) = known else {
                // New durable sessions enter through SessionEstablished;
                // this packet alone can resume only a session already held.
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
        if self.control_collision(connection, packet) {
            return protocol_error(draft);
        }
        if matches!(packet, InboundPacket::Publish { .. })
            && (self.pubsub.control_pending() || self.lifecycle.is_some())
        {
            return Err(Stop::Need(CapacityNeed {
                resource: Resource::ResultSlots,
                amount: 1,
                wake: WakeCondition::ResultDelivered,
            }));
        }
        match packet {
            InboundPacket::Subscribe { .. }
            | InboundPacket::Unsubscribe { .. }
            | InboundPacket::Qos2Ack { .. }
            | InboundPacket::Puback { .. } => {
                self.plan_pubsub_packet(index, connection, packet, draft)
            }
            InboundPacket::ConnectResume { .. } => Err(IMPOSSIBLE),
            InboundPacket::Pingreq => {
                draft.frame(key, OutboundFrame::Pingresp)?;
                Ok(Change::Nothing)
            }
            InboundPacket::Publish {
                qos,
                retain,
                packet_id,
                ..
            } => {
                if *retain && !self.lifecycle_enabled {
                    return protocol_error(draft);
                }
                if *qos == Qos::AtMostOnce {
                    if *retain {
                        return self.plan_retained_qos0(
                            index,
                            (connection, ingress_order),
                            packet,
                            draft,
                        );
                    }
                    return self.plan_qos0_packet(
                        index,
                        (connection, ingress_order),
                        packet,
                        draft,
                    );
                }
                let Some(packet_id) = *packet_id else {
                    return Err(IMPOSSIBLE);
                };
                let Some(slot) = self.slot_of(session.id, packet_id) else {
                    return self.plan_admission(
                        index,
                        (connection, ingress_order),
                        packet,
                        draft,
                        oracle,
                    );
                };
                let held = &self.slots[slot];
                if held.qos != *qos {
                    return protocol_error(draft);
                }
                let publish_origin = (connection, ingress_order);
                match held.state {
                    // The exchange owns its admission: the replay creates
                    // no command and no acknowledgement (ADR 0001). It
                    // takes a new place in the order of its connection:
                    // an answer it preceded may leave (record 0050).
                    SlotState::AdmissionPending(_) | SlotState::AnswerHeld { .. } => {
                        let seen = Seen {
                            slot,
                            order: Some(ingress_order),
                        };
                        self.plan_held_release(session.id, Some(seen), draft)?;
                        Ok(Change::Replay {
                            slot,
                            publish_origin,
                        })
                    }
                    SlotState::AwaitPubrel => {
                        let frame = OutboundFrame::Pubrec {
                            packet_id,
                            reason: AckReason::Success,
                        };
                        // The PUBREC sent again waits for the answers of
                        // the publications received before this copy.
                        if self.preceded(session.id, ingress_order, None) {
                            return Ok(Change::Hold {
                                slot,
                                admission: None,
                                frame,
                                resent: true,
                                publish_origin: Some(publish_origin),
                            });
                        }
                        draft.publish_frame(key, frame, Some(ingress_order))?;
                        Ok(Change::Replay {
                            slot,
                            publish_origin,
                        })
                    }
                    // After its PUBREL a client may only resend PUBREL.
                    SlotState::ReleasePending(_) | SlotState::ReleaseRetry(_) => {
                        protocol_error(draft)
                    }
                    SlotState::AckPending { .. } => Err(IMPOSSIBLE),
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
                let held = &self.slots[slot];
                if held.qos != Qos::ExactlyOnce {
                    return protocol_error(draft);
                }
                match held.state {
                    // No PUBREC of this exchange was ever made eligible.
                    SlotState::AdmissionPending(_)
                    | SlotState::AnswerHeld { resent: false, .. } => protocol_error(draft),
                    // The held frame repeats a PUBREC the client may hold:
                    // its PUBREL is served and the repetition never leaves.
                    // An answer this exchange preceded may leave.
                    SlotState::AnswerHeld { resent: true, .. } => {
                        let change = self.plan_release(slot, None, draft)?;
                        let seen = Seen { slot, order: None };
                        self.plan_held_release(session.id, Some(seen), draft)?;
                        Ok(change)
                    }
                    SlotState::AwaitPubrel => self.plan_release(slot, None, draft),
                    // No second release, and no PUBCOMP before the commit.
                    SlotState::ReleasePending(_) | SlotState::ReleaseRetry(_) => {
                        Ok(Change::Nothing)
                    }
                    SlotState::AckPending { .. } => Err(IMPOSSIBLE),
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
        self.prepare_receipt_entry(&recipe, draft)?;
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

    /// A new PUBLISH: identities, hierarchical routing from the recovered
    /// routes, a frozen recipe, `Persist`.
    fn check_message_expiry(&self, expiry: Option<MessageExpiry>) -> Result<(), Stop> {
        if expiry.is_some_and(|expiry| {
            expiry.deadline().is_err()
                || self
                    .message_now
                    .is_some_and(|now| expiry.state(now).is_err())
        }) {
            return Err(IMPOSSIBLE);
        }
        Ok(())
    }

    fn plan_admission<'o>(
        &self,
        session: usize,
        publish_origin: (ConnectionGeneration, u64),
        packet: &InboundPacket,
        draft: &mut Draft,
        oracle: &mut Option<&mut (dyn InlineClaimOracle + 'o)>,
    ) -> Result<Change, Stop> {
        let InboundPacket::Publish {
            qos,
            topic,
            packet_id: Some(packet_id),
            payload,
            retain,
            expiry,
            properties,
            ..
        } = packet
        else {
            return Err(IMPOSSIBLE);
        };
        if let Some(props) = properties
            && (props.is_empty()
                || props.check().is_err()
                || props.check_payload(payload.as_slice()).is_err())
        {
            return Err(IMPOSSIBLE);
        }
        self.check_message_expiry(*expiry)?;
        let (qos, packet_id) = (*qos, *packet_id);
        let (connection, _) = publish_origin;
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
        let selected = match self.route_index.select(
            self.routes.as_slice(),
            self.sessions.as_slice(),
            &self.session_by_id,
            source.id,
            topic.as_slice(),
            qos,
            limits.max_targets,
        ) {
            Ok(selected) => selected,
            Err(routing::Refusal::Capacity) => return never(draft),
            Err(routing::Refusal::Revision) => return Err(IMPOSSIBLE),
        };
        let targets = selected.len();
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
        // One session holds a bounded number of QoS 2 exchanges, whatever
        // the number of its connections (record 0061): between PUBREC and
        // PUBREL only the client ends an exchange. A QoS 1 exchange ends
        // by the owner's own acknowledgement and is not counted. While the
        // owner is admitting or releasing one of the session's QoS 2
        // exchanges the publication waits for it. When every one awaits a
        // PUBREL a wait would be the client's to end: its connection is
        // closed.
        if qos == Qos::ExactlyOnce
            && let Some(held) = self.slots_by_session.get(&source.id)
        {
            let exchanges = || {
                held.iter()
                    .map(|index| &self.slots[index])
                    .filter(|slot| slot.qos == Qos::ExactlyOnce)
            };
            if exchanges().count() >= self.session_qos2_exchanges {
                let owed = |slot: &Slot| {
                    matches!(
                        slot.state,
                        SlotState::AwaitPubrel | SlotState::AnswerHeld { resent: true, .. }
                    )
                };
                if exchanges().all(owed) {
                    return never(draft);
                }
                return Err(lacking(Resource::ExchangeSlots, 1));
            }
        }
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
        let references = u64::try_from(targets)
            .unwrap_or(u64::MAX)
            .checked_add(u64::from(*retain && !payload.is_empty()))
            .ok_or(IMPOSSIBLE)?;
        let mut list =
            crate::contract::command::TargetVec::with_capacity(targets).map_err(Stop::from)?;

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
        for (index, selected) in selected.iter().enumerate() {
            let target = Target {
                session: selected.session,
                session_revision: selected.revision,
                delivery: DeliveryId {
                    publication,
                    index: u32::try_from(index).map_err(|_| IMPOSSIBLE)?,
                },
                effective_qos: selected.qos,
                cause: DeliveryCause::LiveExact,
            };
            list.try_push(target).map_err(|_| IMPOSSIBLE)?;
        }
        let admission = AdmitPublication {
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
            qos,
            retain: *retain,
            topic: topic.clone(),
            payload: PayloadRef {
                id: payload_id,
                length: PayloadBytes::new(length),
            },
            costs: Costs {
                payload_bytes: PayloadBytes::new(length),
                references: References::new(references),
                exchange_slots: ExchangeSlots::new(u64::from(qos == Qos::ExactlyOnce)),
                receipt_slots: ReceiptSlots::new(1),
            },
            targets: list,
        };
        let mut inline_claims = InlineClaimPlans::None;
        let dirty_targets;
        let recipe = if *retain || expiry.is_some() {
            let retain_targets = if *retain {
                let mut iter = selected.iter();
                crate::contract::retained::RetainMask::from_predicate(selected.len(), |_| {
                    iter.next().is_some_and(|target| target.rap)
                })
                .map_err(|_| IMPOSSIBLE)?
            } else {
                crate::contract::retained::RetainMask::EMPTY
            };
            let retained = crate::contract::retained::RetainedPublication {
                admission,
                retain_targets,
            };
            dirty_targets = Self::sessions_of(&selected);
            if let Some(expiry) = expiry {
                Recipe::ExpiringPublication(crate::contract::command::ExpiringPublication {
                    admission: crate::contract::command::ExpiringAdmission::Qos12(retained),
                    expiry: *expiry,
                })
            } else {
                Recipe::RetainedPublication(retained)
            }
        } else {
            (inline_claims, dirty_targets) = if self.delegated_deliveries {
                if targets > 0
                    && self.inline_claims_globally_allowed()
                    && let Some(oracle_ref) = oracle.as_deref_mut()
                {
                    self.delegated_claims(&selected, length, oracle_ref)
                } else {
                    (InlineClaimPlans::None, Self::sessions_of(&selected))
                }
            } else {
                self.local_claims(&selected, length)
            };
            match &inline_claims {
                InlineClaimPlans::None => Recipe::AdmitPublication(admission),
                InlineClaimPlans::One(only) => Recipe::ClaimedPublication(ClaimedPublication {
                    admission,
                    claim: only.claim(),
                }),
                InlineClaimPlans::Many(plans) => match Self::claims_of(plans) {
                    Ok(claims) => Recipe::MultiClaimedPublication(MultiClaimedPublication {
                        admission,
                        claims,
                    }),
                    Err(stop) => {
                        Self::roll_back_claims(oracle, &inline_claims);
                        return Err(stop);
                    }
                },
            }
        };
        let recipe = if let Some(props) = properties {
            Recipe::AnnotatedPublication(crate::contract::command::AnnotatedPublication {
                inner: alloc::boxed::Box::new(recipe),
                properties: props.clone(),
            })
        } else {
            recipe
        };
        let in_flight = match self.plan_submission(operation, recipe, Some(payload.clone()), draft)
        {
            Ok(in_flight) => in_flight,
            Err(stop) => {
                Self::roll_back_claims(oracle, &inline_claims);
                return Err(stop);
            }
        };
        Ok(Change::Admit {
            session: source.id,
            packet_id,
            exchange,
            publication,
            qos,
            publish_origin,
            unmatched: targets == 0,
            cost,
            in_flight,
            inline_claims,
            dirty_targets,
        })
    }

    /// The inline claims the shards reserve for an admission: one for
    /// each QoS 1 or 2 target that no unresolved non-inline admission
    /// names and whose shard grants the reservation. The other targets
    /// are read from the store after the commit.
    fn delegated_claims(
        &self,
        selected: &routing::Selection,
        length: u64,
        oracle: &mut dyn InlineClaimOracle,
    ) -> (InlineClaimPlans, Option<alloc::boxed::Box<[SessionId]>>) {
        let mut plans = alloc::vec::Vec::with_capacity(selected.len());
        let mut unclaimed = alloc::vec::Vec::new();
        for (idx, target) in selected.iter().enumerate() {
            if target.qos != Qos::AtMostOnce
                && !self
                    .pubsub
                    .non_inline_by_session
                    .contains_key(&target.session)
                && let Ok(target_index) = u16::try_from(idx)
                && let Some((target_gen, out_packet_id)) =
                    oracle.try_reserve(target.session, target.qos, length)
            {
                plans.push(InlineClaimPlan {
                    target_index,
                    target: target.session,
                    target_revision: target.revision,
                    qos: target.qos,
                    packet_id: out_packet_id,
                    generation: target_gen,
                });
            } else {
                unclaimed.push(target.session);
            }
        }
        (
            InlineClaimPlans::from_vec(plans),
            (!unclaimed.is_empty()).then(|| unclaimed.into_boxed_slice()),
        )
    }

    /// The inline claims the owner plans when it keeps its deliveries:
    /// the only target when it can be claimed or, with several targets,
    /// each one that can while the delivery table has a free row and the
    /// payload budget room for it. The other targets are read from the
    /// store after the commit.
    fn local_claims(
        &self,
        selected: &routing::Selection,
        length: u64,
    ) -> (InlineClaimPlans, Option<alloc::boxed::Box<[SessionId]>>) {
        if selected.len() == 1
            && let Some(only) = selected.iter().next().copied()
            && let Some((target_gen, out_packet_id)) = self.plan_inline_claim(only, length)
        {
            let plan = InlineClaimPlan {
                target_index: 0,
                target: only.session,
                target_revision: only.revision,
                qos: only.qos,
                packet_id: out_packet_id,
                generation: target_gen,
            };
            return (InlineClaimPlans::One(plan), None);
        }
        if selected.len() <= 1 || !self.inline_claims_globally_allowed() {
            return (InlineClaimPlans::None, Self::sessions_of(selected));
        }
        let mut remaining_slots = self
            .pubsub
            .deliveries
            .capacity()
            .saturating_sub(self.pubsub.deliveries.len());
        let mut running_bytes = self.pubsub.deliveries.loaded_bytes();
        let mut plans = alloc::vec::Vec::with_capacity(selected.len());
        let mut unclaimed = alloc::vec::Vec::new();
        for (idx, target) in selected.iter().enumerate() {
            if let Ok(target_index) = u16::try_from(idx)
                && remaining_slots > 0
                && running_bytes
                    .checked_add(length)
                    .is_some_and(|total| total <= self.pubsub.payload_limit())
                && let Some((target_gen, out_packet_id)) = self.plan_inline_claim_target(*target)
            {
                remaining_slots -= 1;
                running_bytes = running_bytes.saturating_add(length);
                plans.push(InlineClaimPlan {
                    target_index,
                    target: target.session,
                    target_revision: target.revision,
                    qos: target.qos,
                    packet_id: out_packet_id,
                    generation: target_gen,
                });
            } else {
                unclaimed.push(target.session);
            }
        }
        (
            InlineClaimPlans::from_vec(plans),
            (!unclaimed.is_empty()).then(|| unclaimed.into_boxed_slice()),
        )
    }

    /// The sessions of every selected target, read from the store after
    /// the commit; none when nothing is selected.
    fn sessions_of(selected: &routing::Selection) -> Option<alloc::boxed::Box<[SessionId]>> {
        if selected.len() == 0 {
            return None;
        }
        let mut sessions = alloc::vec::Vec::with_capacity(selected.len());
        for target in selected.iter() {
            sessions.push(target.session);
        }
        Some(sessions.into_boxed_slice())
    }

    /// The claims of a recipe that names several inline claims.
    fn claims_of(plans: &[InlineClaimPlan]) -> Result<BoundedVec<InlineClaim>, Stop> {
        let mut claims = BoundedVec::with_capacity(plans.len())?;
        for plan in plans {
            claims.try_push(plan.claim()).map_err(|_| IMPOSSIBLE)?;
        }
        Ok(claims)
    }

    /// Gives back to the shards the identifiers they reserved for the
    /// inline claims of a plan that is refused. Without shards nothing
    /// was reserved.
    fn roll_back_claims<'o>(
        oracle: &mut Option<&mut (dyn InlineClaimOracle + 'o)>,
        plans: &InlineClaimPlans,
    ) {
        if let Some(oracle) = oracle.as_deref_mut() {
            for plan in plans.as_slice() {
                oracle.rollback(plan.target, plan.packet_id);
            }
        }
    }

    /// One counted wake represents all deferred known-fenced releases. It
    /// is scheduled only after an absolute confirmed prefix leaves room.
    fn plan_release_wake(&self, draft: &mut Draft) -> Result<Option<Change>, Stop> {
        if !self.receipts.enabled
            || self.release_continuation.is_some()
            || !self.receipts.control_available()
            || (self.release_retry_slots.is_empty()
                && !self.pubsub.has_control_work(self.message_now))
        {
            return Ok(None);
        }
        let continuation = draw(
            &mut draft.counters.continuation,
            ContinuationId::checked_next,
        )?;
        draft.emit(Effect::Continue { continuation })?;
        Ok(Some(Change::ReleaseWake { continuation }))
    }

    fn plan_continue(
        &self,
        continuation: ContinuationId,
        draft: &mut Draft,
    ) -> Result<Change, Stop> {
        if self.release_continuation == Some(continuation) {
            // Another control may have used the available room since the
            // wake. Consume this wake without spinning or losing the work;
            // a subsequent confirmed prefix makes it eligible again.
            if !self.receipts.control_available() {
                return Ok(Change::ReleaseYield);
            }
            let Some((slot, refused)) =
                self.release_retry_slots.first().copied().and_then(|slot| {
                    match &self.slots[slot].state {
                        SlotState::ReleaseRetry(refused) => Some((slot, (**refused).clone())),
                        _ => None,
                    }
                })
            else {
                return Ok(self
                    .plan_pubsub_control_work(None, draft)?
                    .unwrap_or(Change::ReleaseYield));
            };
            return self.plan_release(slot, Some(refused), draft);
        }
        if self.release_continuations_started
            && self.pubsub.continuation != Some(continuation)
            && issued(continuation, self.counters.continuation)
        {
            return Ok(Change::Nothing);
        }
        self.plan_qos0_continue(continuation, draft)
    }

    /// PUBREL, or a release the store certainly refused: the durable
    /// termination is submitted under a new operation.
    fn plan_release(
        &self,
        slot: usize,
        refused: Option<Finished>,
        draft: &mut Draft,
    ) -> Result<Change, Stop> {
        let held = &self.slots[slot];
        let operation = self.operation(draft)?;
        let recipe = Recipe::ReleaseInbound(ReleaseInbound {
            operation,
            source_session: held.session,
            packet_id: held.packet_id,
            exchange: held.exchange,
        });
        let in_flight = self.plan_submission(operation, recipe, None, draft)?;
        Ok(Change::Release {
            resumed: matches!(held.state, SlotState::ReleaseRetry(_)),
            slot,
            in_flight,
            refused,
        })
    }

    /// The place of an exchange in the order of the connection that is
    /// current for its session: the ingress order of its PUBLISH, when
    /// that PUBLISH came on this connection. An exchange read from durable
    /// data, or whose PUBLISH came on a connection since replaced, has no
    /// place: it neither waits nor makes another wait (record 0050).
    fn place(&self, slot: &Slot) -> Option<u64> {
        let generation = self.connection_of(slot.session)?;
        slot.publish_origin
            .filter(|(origin, _)| *origin == generation)
            .map(|(_, order)| order)
    }

    /// Whether an answer at `order` on the connection of `session` would
    /// overtake: another exchange of the session whose PUBLISH came
    /// earlier on that connection has no answer yet, its admission pending
    /// or its own answer held. `seen` replaces what the state says of the
    /// one slot the transition being planned changes.
    fn preceded(&self, session: SessionId, order: u64, seen: Option<Seen>) -> bool {
        if seen.is_none() && self.unanswered_slots == 0 {
            return false;
        }
        let Some(indices) = self.slots_by_session.get(&session) else {
            return false;
        };
        if seen.is_none() && indices.len() <= 1 {
            return false;
        }
        let Some(generation) = self.connection_of(session) else {
            return false;
        };
        indices.iter().any(|index| {
            let slot = &self.slots[index];
            let unanswered = match seen {
                Some(seen) if seen.slot == index => seen.order,
                _ if matches!(
                    slot.state,
                    SlotState::AdmissionPending(_) | SlotState::AnswerHeld { .. }
                ) =>
                {
                    slot.publish_origin
                        .filter(|(origin, _)| *origin == generation)
                        .map(|(_, order)| order)
                }
                _ => None,
            };
            unanswered.is_some_and(|earlier| earlier < order)
        })
    }

    /// Whether the answer of `slot` must wait its turn.
    fn must_wait(&self, slot: &Slot) -> bool {
        let self_unanswered = matches!(
            slot.state,
            SlotState::AdmissionPending(_) | SlotState::AnswerHeld { .. }
        );
        if self.unanswered_slots <= usize::from(self_unanswered) {
            return false;
        }
        self.place(slot)
            .is_some_and(|order| self.preceded(slot.session, order, None))
    }

    /// Makes the oldest held answer of `session` eligible when nothing
    /// precedes it any more: one for each transition, beside its change,
    /// and only while the plan has room for its frame. The receipt of the
    /// frame is an input of its own, which releases the next (record 0050).
    fn plan_held_release(
        &self,
        session: SessionId,
        seen: Option<Seen>,
        draft: &mut Draft,
    ) -> Result<(), Stop> {
        if self.held_answers == 0
            || draft.released.is_some()
            || draft.effects.count >= MAX_TRANSITION_EFFECTS
        {
            return Ok(());
        }
        let Some(generation) = self.connection_of(session) else {
            return Ok(());
        };
        let Some(indices) = self.slots_by_session.get(&session) else {
            return Ok(());
        };
        let place = |index: usize, slot: &Slot| match seen {
            Some(seen) if seen.slot == index => seen.order,
            _ => self.place(slot),
        };
        let oldest = indices
            .iter()
            // A slot the transition answers itself holds nothing to release.
            .filter(|index| !seen.is_some_and(|seen| seen.slot == *index && seen.order.is_none()))
            .filter_map(|index| {
                let slot = &self.slots[index];
                match slot.state {
                    SlotState::AnswerHeld { frame, .. } => Some((place(index, slot), index, frame)),
                    _ => None,
                }
            })
            .min_by_key(|(order, index, _)| (*order, *index));
        let Some((order, index, frame)) = oldest else {
            return Ok(());
        };
        if order.is_some_and(|order| self.preceded(session, order, seen)) {
            return Ok(());
        }
        let connection = ConnectionKey {
            session,
            generation,
        };
        let effect = draft.publish_frame(connection, frame, order)?;
        let next = match frame {
            OutboundFrame::Puback { .. } => Next::Ack { effect, connection },
            OutboundFrame::Pubrec {
                reason: AckReason::Success,
                ..
            } => Next::AwaitPubrel,
            _ => Next::End,
        };
        draft.released = Some(Released { slot: index, next });
        Ok(())
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
        self.plan_answer_frame(slot, frame, draft).map(|_| ())
    }

    fn plan_answer_frame(
        &self,
        slot: &Slot,
        frame: OutboundFrame,
        draft: &mut Draft,
    ) -> Result<Option<(EffectId, ConnectionKey)>, Stop> {
        let Some(generation) = self.connection_of(slot.session) else {
            return Ok(None);
        };
        let connection = ConnectionKey {
            session: slot.session,
            generation,
        };
        let publish_order = slot
            .publish_origin
            .filter(|(origin, _)| *origin == generation)
            .map(|(_, order)| order);
        let effect = draft.publish_frame(connection, frame, publish_order)?;
        Ok(Some((effect, connection)))
    }

    fn plan_puback(
        &self,
        index: usize,
        finished: Finished,
        reason: PubackReason,
        draft: &mut Draft,
    ) -> Result<Change, Stop> {
        let slot = &self.slots[index];
        let frame = OutboundFrame::Puback {
            packet_id: slot.packet_id,
            reason,
        };
        if self.must_wait(slot) {
            return Ok(Change::Hold {
                slot: index,
                admission: Some(finished),
                frame,
                resent: false,
                publish_origin: None,
            });
        }
        match self.plan_answer_frame(slot, frame, draft)? {
            Some((effect, connection)) => Ok(Change::AckReady {
                slot: index,
                admission: finished,
                effect,
                connection,
            }),
            None => Ok(Change::End {
                slot: index,
                finished,
            }),
        }
    }

    /// Applies the result of a command, then lets the oldest held answer
    /// of the same session leave if the result answered or ended what
    /// preceded it (record 0050).
    fn plan_result(
        &self,
        effect: EffectId,
        operation: OperationId,
        reply: &StoreReply,
        draft: &mut Draft,
    ) -> Result<Change, Stop> {
        let change = self.plan_result_change(effect, operation, reply, draft)?;
        if self.held_answers == 0 {
            return Ok(change);
        }
        let (index, seen) = match change {
            Change::Committed { slot, .. }
            | Change::AckReady { slot, .. }
            | Change::End { slot, .. } => (slot, Some(Seen { slot, order: None })),
            // The exchange stays without an answer, at its place.
            Change::Hold { slot, .. } => (slot, None),
            _ => return Ok(change),
        };
        let session = self.slots[index].session;
        self.plan_held_release(session, seen, draft)?;
        Ok(change)
    }

    fn plan_result_change(
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
        if self.lifecycle.is_some()
            && let Some(change) = self.plan_session_result(effect, operation, reply, draft)?
        {
            return Ok(change);
        }
        if self.wills.has_flight()
            && let Some(change) = self.plan_will_result(effect, operation, reply, draft)?
        {
            return Ok(change);
        }
        if let Some(change) = self.plan_pubsub_result(effect, operation, reply, draft)? {
            return Ok(change);
        }
        let waiting = self.slot_by_op.get(&operation).copied();
        let Some(index) = waiting else {
            return self.plan_late_result(effect, operation, reply);
        };
        let slot = &self.slots[index];
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
        let finished =
            |phase, facts| Finished::new(operation, in_flight.answers.clone(), phase, facts);
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
                draft.receipt_consumed = Some(operation);
                let packet_id = slot.packet_id;
                let reason = AckReason::Success;
                // The owner publishes what depends on the commit in this
                // transition: the command is consumed.
                let finished = finished(CommandPhase::Consumed, Some(facts));
                let change = if slot.retired {
                    Change::End {
                        slot: index,
                        finished,
                    }
                } else if admission {
                    if slot.qos == Qos::AtLeastOnce {
                        self.plan_puback(index, finished, slot.puback_reason(), draft)?
                    } else {
                        // PUBREC becomes eligible here and nowhere earlier.
                        let frame = OutboundFrame::Pubrec { packet_id, reason };
                        if self.must_wait(slot) {
                            Change::Hold {
                                slot: index,
                                admission: Some(finished),
                                frame,
                                resent: false,
                                publish_origin: None,
                            }
                        } else {
                            self.plan_answer(slot, frame, draft)?;
                            Change::Committed {
                                slot: index,
                                admission: finished,
                            }
                        }
                    }
                } else {
                    self.plan_answer(slot, OutboundFrame::Pubcomp { packet_id, reason }, draft)?;
                    Change::End {
                        slot: index,
                        finished,
                    }
                };
                if admission {
                    draft.inline_outcome = self.plan_inline_claim_result(operation, true, draft)?;
                    if self.delegated_deliveries
                        && self.delegated_inline_by_op.contains_key(&operation)
                    {
                        draft.delegated_inline_resolved = Some((operation, true));
                    }
                    if !self.non_inline_targets_by_op.is_empty() {
                        draft.non_inline_resolved = Some((operation, true));
                    }
                }
                Ok(change)
            }
            (
                Verdict::Transition(CommandPhase::RejectedCertain),
                StoreOutcome::RejectedCertain(rejection),
            ) => {
                draft.receipt_consumed = Some(operation);
                let finished = finished(CommandPhase::RejectedCertain, None);
                if admission {
                    draft.inline_outcome =
                        self.plan_inline_claim_result(operation, false, draft)?;
                    if self.delegated_deliveries
                        && self.delegated_inline_by_op.contains_key(&operation)
                    {
                        draft.delegated_inline_resolved = Some((operation, false));
                    }
                    if !self.non_inline_targets_by_op.is_empty() {
                        draft.non_inline_resolved = Some((operation, false));
                    }
                }
                if slot.retired {
                    return Ok(Change::End {
                        slot: index,
                        finished,
                    });
                }
                if admission {
                    if slot.qos == Qos::AtLeastOnce {
                        return self.plan_puback(
                            index,
                            finished,
                            PubackReason::refusing(rejection),
                            draft,
                        );
                    }
                    // Nothing was admitted: the exchange ends and the
                    // client is told, as the protocol says.
                    let frame = OutboundFrame::Pubrec {
                        packet_id: slot.packet_id,
                        reason: AckReason::refusing(rejection),
                    };
                    if self.must_wait(slot) {
                        return Ok(Change::Hold {
                            slot: index,
                            admission: Some(finished),
                            frame,
                            resent: false,
                            publish_origin: None,
                        });
                    }
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
                        if self.receipts.enabled && !self.receipts.control_available() {
                            Ok(Change::ReleaseDeferred {
                                slot: index,
                                refused: finished,
                            })
                        } else {
                            self.plan_release(index, Some(finished), draft)
                        }
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
        let known: Option<&Finished> = match self.finished_by_op.as_ref() {
            Some(index) => index.get(&operation).map(|(item, _)| item),
            None => self
                .finished
                .iter()
                .filter(|item| item.operation == operation)
                .last(),
        }
        .or_else(|| self.pubsub.known(operation))
        .or_else(|| {
            self.slots
                .values()
                .filter_map(|slot| slot.admission.as_ref())
                .chain(self.release_retry_slots.iter().filter_map(|&idx| {
                    match &self.slots[idx].state {
                        SlotState::ReleaseRetry(finished) => Some(finished.as_ref()),
                        _ => None,
                    }
                }))
                .find(|finished| finished.operation == operation)
        });
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
        if outcome == StoreOutcome::RetiredOutcomeUnavailable && self.receipts.retired(operation) {
            return Ok(Change::Nothing);
        }
        let contradiction = Stop::Fatal(CoreFault::ContradictoryOutcome { operation });
        let event = CommandEvent::StoreResult(OutcomeClass::of(&outcome));
        if known.phase.on(event) == Verdict::Fault {
            return Err(contradiction);
        }
        if let StoreOutcome::Committed(facts) = outcome
            && !known.facts_match(facts)
        {
            return Err(contradiction);
        }
        Ok(Change::Nothing)
    }

    fn plan_deadline(&self, token: DeadlineToken, draft: &mut Draft) -> Result<Change, Stop> {
        if !issued(token, self.counters.deadline) {
            return Err(IMPOSSIBLE);
        }
        if let Some(change) = self.plan_receipt_deadline(token, draft)? {
            return Ok(change);
        }
        if let Some(change) = self.plan_session_deadline(token, draft)? {
            return Ok(change);
        }
        if let Some(change) = self.plan_will_deadline(token, draft)? {
            return Ok(change);
        }
        if let Some(change) = self.plan_pubsub_deadline(token, draft)? {
            return Ok(change);
        }
        let waiting = self
            .slot_by_deadline
            .get(&token)
            .copied()
            .and_then(|index| {
                let held = self.slots[index].in_flight()?;
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
        effect: EffectId,
        connection: ConnectionKey,
        progress: FrameProgress,
        draft: &mut Draft,
    ) -> Result<Change, Stop> {
        if let Some(change) = self.plan_qos0_or_pubsub_frame(effect, connection, progress, draft)? {
            return Ok(change);
        }
        let failed = matches!(progress, FrameProgress::Failed);
        let completed = matches!(
            progress,
            FrameProgress::Written { confirmed, total } if total > 0 && confirmed == total
        );
        let ended = self.slot_by_effect.get(&effect).copied().and_then(|index| {
            let slot = &self.slots[index];
            let SlotState::AckPending {
                effect: held,
                connection: destination,
            } = slot.state
            else {
                return None;
            };
            (held == effect && destination == connection).then(|| (index, slot.admission.clone()))
        });
        // The receipt of a frame of the current connection is where the
        // next held answer of its session leaves (record 0050): each
        // acknowledgement released brings such a receipt.
        let current = self.connection_of(connection.session) == Some(connection.generation);
        if (failed || completed)
            && let Some((slot, Some(finished))) = ended
        {
            if failed && current {
                draft.end(connection.generation)?;
            } else if current {
                self.plan_held_release(connection.session, None, draft)?;
            }
            return Ok(Change::End { slot, finished });
        }
        match self.session_of(connection.generation) {
            Some(session)
                if failed && self.sessions.as_slice()[session].id == connection.session =>
            {
                draft.end(connection.generation)?;
                Ok(Change::Unbind { session })
            }
            _ => {
                if current && !failed {
                    self.plan_held_release(connection.session, None, draft)?;
                }
                Ok(Change::Nothing)
            }
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
        let mut starts = [None; 9];
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
                RecoveredFact::ReceiptFrontier(closure) => {
                    closure.origin.incarnation == self.incarnation
                        && closure.origin.epoch < self.epoch
                }
                RecoveredFact::OutboundDelivery {
                    delivery,
                    target,
                    target_revision,
                    qos,
                    claim,
                    expiry,
                    started,
                } => self.valid_outbound_fact(
                    *delivery,
                    *target,
                    *target_revision,
                    (*qos, *claim, *expiry, *started),
                    same,
                    run(facts, &starts, FactRank::Session, position),
                ),
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
                        && crate::contract::topic::valid_filter(topic.as_slice())
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
                    session,
                    client_id,
                    expiry,
                    detached,
                    ..
                } => self.session_metadata_valid(*expiry, *detached)
                    && self.sessions.len() + same.len() < self.sessions.capacity()
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
                        && !self.slots.values().any(|held| {
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

    /// Applies a planned transition. The draft is taken by reference: it is
    /// the largest value of a step, and a move of it costs a copy that the
    /// optimiser does not always elide.
    fn apply_draft<'o>(
        &mut self,
        draft: &mut Draft,
        change: Change,
        input: Input,
        reservation: Option<EffectReservation>,
        direct_sink: Option<&mut dyn EffectSink>,
        oracle: &mut Option<&mut (dyn InlineClaimOracle + 'o)>,
    ) -> Result<(Progress, usize), CoreFault> {
        let Draft {
            counters,
            ref mut effects,
            progress,
            closed,
            receipt_issued,
            receipt_consumed,
            released,
            ref mut inline_outcome,
            delegated_inline_resolved,
            non_inline_resolved,
            ..
        } = *draft;
        if direct_sink.is_none() {
            let reserved = match reservation {
                Some(key) => self
                    .outbox
                    .remaining(key)
                    .map_err(|_| CoreFault::ReservationOverrun)?,
                None => 0,
            };
            if reserved != effects.count {
                return Err(CoreFault::ReservationOverrun);
            }
        }
        let revision = self
            .revision
            .checked_next()
            .map_err(|_| CoreFault::StalePlan)?;

        // A release that leaves its slot in place is applied first: the
        // change may remove a slot and move the last one. One that removes
        // its slot is applied after the change, unless the change removes
        // a slot of a smaller index: the larger index is removed first, so
        // that neither removal moves the slot of the other.
        let after = match (released, &change) {
            (
                Some(Released {
                    slot,
                    next: Next::End,
                }),
                Change::End { slot: ended, .. },
            ) => slot < *ended,
            (
                Some(Released {
                    next: Next::End, ..
                }),
                _,
            ) => true,
            _ => false,
        };
        if !after && let Some(released) = released {
            self.apply_release(released)?;
        }
        self.apply_change(change, input, oracle)?;
        let has_non_inline = non_inline_resolved
            .as_ref()
            .is_some_and(|(op, _)| self.non_inline_targets_by_op.contains_key(op));
        if let Some(outcome) = inline_outcome.take() {
            self.apply_inline_claim_outcome(outcome, has_non_inline)?;
        }
        if let Some((operation, committed)) = delegated_inline_resolved
            && let Some(batch) = self.delegated_inline_by_op.remove(&operation)
        {
            if !self.delegated_inline_unknown.is_empty() {
                self.delegated_inline_unknown.remove(&operation);
            }
            for claim in &batch.claims {
                if let Some(count) = self.delegated_inline_by_session.get_mut(&claim.target) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        self.delegated_inline_by_session.remove(&claim.target);
                    }
                }
            }
            if committed {
                self.last_delegated_committed = Some(batch);
                if !has_non_inline {
                    self.last_step_may_unblock_work = false;
                }
            } else {
                if let Some(oracle_ref) = oracle.as_deref_mut() {
                    for claim in &batch.claims {
                        oracle_ref.rollback(claim.target, claim.packet_id);
                    }
                }
                self.last_delegated_rejected = Some(batch);
            }
        }
        if let Some((operation, committed)) = non_inline_resolved
            && let Some((publication, targets)) = self.non_inline_targets_by_op.remove(&operation)
        {
            self.pubsub.pending_non_inline_pubs.remove(&publication);
            for target in targets {
                if let Some(count) = self.pubsub.non_inline_by_session.get_mut(&target) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        self.pubsub.non_inline_by_session.remove(&target);
                    }
                }
                if committed {
                    if self.delegated_deliveries
                        && let Some(oracle_ref) = oracle.as_deref_mut()
                    {
                        oracle_ref.mark_ineligible(target);
                    }
                    self.mark_session_dirty(target, true);
                    self.push_inline_ready_target(target);
                    self.last_step_may_unblock_work = true;
                }
            }
        }
        if after && let Some(released) = released {
            self.apply_release(released)?;
        }
        self.receipts.apply_issued(receipt_issued)?;
        self.receipts.consume(receipt_consumed)?;
        if let Some(connection) = closed {
            if let Some(flight) = self.lifecycle.as_mut()
                && flight.connection == connection
            {
                flight.connection_closed = true;
            }
            // Closing a generation is terminal for its queued/emitting
            // ACKs, even if the adapter never accepted their Outbox effect.
            // Uncertain admissions are deliberately kept for reconciliation.
            self.finish_closed_acks(connection)?;
            self.pubsub.close(connection);
            if let Some(session) = self.session_of(connection) {
                self.set_session_connection(session, None)?;
            }
            self.remember_closed(connection)?;
        }
        self.counters = counters;
        self.revision = revision;

        let mut accepted = 0;
        if let Some(sink) = direct_sink {
            if effects.count > 0 {
                accepted = self
                    .outbox
                    .dispatch_immediate(&mut effects.items[..effects.count], sink)
                    .map_err(|_| CoreFault::ReservationOverrun)?;
            }
        } else if let Some(key) = reservation {
            for slot in &mut effects.items[..effects.count] {
                if let Some(effect) = slot.take() {
                    self.outbox
                        .publish(key, effect)
                        .map_err(|_| CoreFault::ReservationOverrun)?;
                }
            }
        }
        Ok((progress, accepted))
    }

    fn apply_change<'o>(
        &mut self,
        change: Change,
        input: Input,
        oracle: &mut Option<&mut (dyn InlineClaimOracle + 'o)>,
    ) -> Result<(), CoreFault> {
        const STALE: CoreFault = CoreFault::StalePlan;
        match change {
            Change::Session(change) => self.apply_session(*change)?,
            Change::Receipts(change) => self.receipts.apply(*change)?,
            Change::Pubsub(change) => self.apply_pubsub(change, input)?,
            Change::Will(change) => self.apply_will(*change)?,
            Change::Nothing => {
                self.last_step_may_unblock_work = false;
            }
            Change::MessageClock(now) => {
                self.message_now = Some(now);
                self.pubsub.evict_expired_cache(now);
            }
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
            Change::Establish { session } => {
                self.push_session(*session)?;
            }
            Change::Bind {
                session,
                connection,
            } => {
                self.set_session_connection(session, Some(connection))?;
            }
            Change::Unbind { session } => {
                self.set_session_connection(session, None)?;
            }
            Change::Admit {
                session,
                packet_id,
                exchange,
                publication,
                qos,
                publish_origin,
                unmatched,
                cost,
                in_flight,
                inline_claims,
                dirty_targets,
            } => {
                // The plan checked the account and the table.
                let lease = self.leases.reserve(cost).map_err(|_| STALE)?;
                let op = in_flight.operation;
                let slot = Slot {
                    session,
                    packet_id,
                    exchange,
                    publication,
                    qos,
                    publish_origin: Some(publish_origin),
                    unmatched,
                    lease,
                    admission: None,
                    release_preparations: 0,
                    state: SlotState::AdmissionPending(in_flight),
                    retired: false,
                };
                self.push_slot(slot)?;
                if !inline_claims.is_empty() {
                    let Input::Packet {
                        packet:
                            InboundPacket::Publish {
                                topic,
                                payload,
                                properties,
                                ..
                            },
                        ..
                    } = input
                    else {
                        return Err(STALE);
                    };
                    if self.delegated_deliveries {
                        let mut claims = alloc::vec::Vec::new();
                        let make_claim = |plan: InlineClaimPlan| DelegatedClaim {
                            delivery: DeliveryId {
                                publication,
                                index: u32::from(plan.target_index),
                            },
                            target: plan.target,
                            target_revision: plan.target_revision,
                            qos: plan.qos,
                            claim_operation: op,
                            packet_id: plan.packet_id,
                            generation: plan.generation,
                        };
                        match inline_claims {
                            InlineClaimPlans::None => {}
                            InlineClaimPlans::One(plan) => {
                                *self
                                    .delegated_inline_by_session
                                    .get_or_insert_default(plan.target) += 1;
                                claims.push(make_claim(plan));
                            }
                            InlineClaimPlans::Many(plans) => {
                                claims.reserve(plans.len());
                                for plan in plans {
                                    *self
                                        .delegated_inline_by_session
                                        .get_or_insert_default(plan.target) += 1;
                                    claims.push(make_claim(plan));
                                }
                            }
                        }
                        self.delegated_inline_by_op.insert(
                            op,
                            DelegatedInlineBatch {
                                operation: op,
                                topic,
                                payload,
                                properties,
                                claims,
                            },
                        );
                    } else {
                        match inline_claims {
                            InlineClaimPlans::None => {}
                            InlineClaimPlans::One(plan) => {
                                self.apply_inline_claim_insert(
                                    op,
                                    publication,
                                    plan,
                                    topic,
                                    payload,
                                    properties,
                                )?;
                            }
                            InlineClaimPlans::Many(plans) => {
                                for plan in plans {
                                    self.apply_inline_claim_insert(
                                        op,
                                        publication,
                                        plan,
                                        topic.clone(),
                                        payload.clone(),
                                        properties.clone(),
                                    )?;
                                }
                            }
                        }
                    }
                }
                if let Some(dirty_targets) = dirty_targets {
                    let dirty_targets = dirty_targets.into_vec();
                    for &target in &dirty_targets {
                        if self.delegated_deliveries
                            && let Some(oracle_ref) = oracle.as_deref_mut()
                        {
                            oracle_ref.mark_ineligible(target);
                        }
                        *self
                            .pubsub
                            .non_inline_by_session
                            .get_or_insert_default(target) += 1;
                        self.mark_session_dirty(target, false);
                    }
                    self.pubsub.pending_non_inline_pubs.insert(publication);
                    self.non_inline_targets_by_op
                        .insert(op, (publication, dirty_targets));
                }
            }
            Change::Committed { slot, admission } => {
                if !matches!(
                    self.slots.get(slot).map(|h| &h.state),
                    Some(SlotState::AdmissionPending(_))
                ) {
                    return Err(STALE);
                }
                self.slots[slot].admission = Some(admission);
                self.await_pubrel(slot)?;
            }
            Change::Replay {
                slot,
                publish_origin,
            } => {
                let held = self.slots.get_mut(slot).ok_or(STALE)?;
                held.publish_origin = Some(publish_origin);
            }
            Change::AckReady {
                slot,
                admission,
                effect,
                connection,
            } => {
                let Some(current) = self.slots.get(slot) else {
                    return Err(STALE);
                };
                if !matches!(current.state, SlotState::AdmissionPending(_))
                    || current.qos != Qos::AtLeastOnce
                {
                    return Err(STALE);
                }
                self.unindex_slot_state(slot);
                let held = &mut self.slots[slot];
                held.admission = Some(admission);
                held.state = SlotState::AckPending { effect, connection };
                self.index_slot_state(slot);
            }
            Change::Hold {
                slot,
                admission,
                frame,
                resent,
                publish_origin,
            } => {
                let Some(current) = self.slots.get(slot) else {
                    return Err(STALE);
                };
                // A first answer is held with the result that produced it;
                // a PUBREC sent again, for an exchange that awaits PUBREL.
                let expected = match (resent, &current.state) {
                    (false, SlotState::AdmissionPending(_)) => admission.is_some(),
                    (true, SlotState::AwaitPubrel) => admission.is_none(),
                    _ => false,
                };
                if !expected {
                    return Err(STALE);
                }
                self.unindex_slot_state(slot);
                let held = &mut self.slots[slot];
                if admission.is_some() {
                    held.admission = admission;
                }
                if publish_origin.is_some() {
                    held.publish_origin = publish_origin;
                }
                held.state = SlotState::AnswerHeld { frame, resent };
                self.index_slot_state(slot);
            }
            Change::End { slot, finished } => {
                if matches!(input, Input::FrameResult { .. }) {
                    self.last_step_may_unblock_work = false;
                }
                let lease = self.slots.get(slot).ok_or(STALE)?.lease;
                if self.leases.release(lease) != Ok(ReleaseOutcome::Released) {
                    return Err(STALE);
                }
                let held = self.remove_slot_at(slot)?;
                // The operations of the exchange stay visible, the latest
                // last.
                if let Some(admission) = held.admission
                    && admission.operation != finished.operation
                {
                    self.remember_finished(admission)?;
                }
                self.remember_finished(finished)?;
            }
            Change::Await {
                slot,
                phase,
                resolution,
            } => {
                if !matches!(
                    self.slots.get(slot).map(|h| &h.state),
                    Some(SlotState::AdmissionPending(_) | SlotState::ReleasePending(_))
                ) {
                    return Err(STALE);
                }
                self.unindex_slot_state(slot);
                let held = &mut self.slots[slot];
                let (SlotState::AdmissionPending(in_flight) | SlotState::ReleasePending(in_flight)) =
                    &mut held.state
                else {
                    return Err(STALE);
                };
                let unknown_now = in_flight.phase == CommandPhase::Submitted
                    && phase == CommandPhase::Indeterminate;
                let operation = in_flight.operation;
                in_flight.phase = phase;
                if let Some((deadline, effect)) = resolution {
                    in_flight.deadline = deadline;
                    let prev = core::mem::replace(&mut in_flight.answers, Answers::new(effect));
                    in_flight.answers = prev.with(effect);
                }
                self.index_slot_state(slot);
                if unknown_now && self.delegated_inline_by_op.contains_key(&operation) {
                    self.delegated_inline_unknown.insert(operation);
                }
            }
            Change::ReleaseDeferred { slot, refused } => {
                if self.slots.get(slot).is_none() {
                    return Err(STALE);
                }
                self.unindex_slot_state(slot);
                self.slots[slot].state = SlotState::ReleaseRetry(alloc::boxed::Box::new(refused));
                self.index_slot_state(slot);
            }
            Change::ReleaseWake { continuation } => {
                self.release_continuation = Some(continuation);
                self.release_continuations_started = true;
            }
            Change::ReleaseYield => self.release_continuation = None,
            Change::Release {
                resumed,
                slot,
                in_flight,
                refused,
            } => {
                if self.slots.get(slot).is_none() {
                    return Err(STALE);
                }
                self.unindex_slot_state(slot);
                let held = &mut self.slots[slot];
                held.state = SlotState::ReleasePending(in_flight);
                if resumed {
                    self.release_continuation = None;
                }
                held.release_preparations = held.release_preparations.saturating_add(1);
                self.index_slot_state(slot);
                if let Some(refused) = refused {
                    self.remember_finished(refused)?;
                }
            }
        }
        Ok(())
    }

    /// A held answer left: its exchange goes where the answer takes it.
    fn apply_release(&mut self, released: Released) -> Result<(), CoreFault> {
        const STALE: CoreFault = CoreFault::StalePlan;
        let Some(current) = self.slots.get(released.slot) else {
            return Err(STALE);
        };
        if !matches!(current.state, SlotState::AnswerHeld { .. }) {
            return Err(STALE);
        }
        match released.next {
            Next::Ack { effect, connection } => {
                self.unindex_slot_state(released.slot);
                self.slots[released.slot].state = SlotState::AckPending { effect, connection };
                self.index_slot_state(released.slot);
            }
            Next::AwaitPubrel => self.await_pubrel(released.slot)?,
            Next::End => self.end_held(released.slot)?,
        }
        Ok(())
    }

    /// The exchange at `index` now awaits the PUBREL of its client. Its
    /// publication is committed and no longer in this process: the lease
    /// keeps the exchange and returns the bytes of the payload, as the
    /// lease of an exchange recovered from the store holds none (record
    /// 0061).
    fn await_pubrel(&mut self, index: usize) -> Result<(), CoreFault> {
        let lease = self.slots[index].lease;
        let kept = Credits::new(PayloadBytes::ZERO, ExchangeSlots::new(1));
        let lease = self
            .leases
            .lighten(lease, kept)
            .ok_or(CoreFault::StalePlan)?;
        self.unindex_slot_state(index);
        let held = &mut self.slots[index];
        held.lease = lease;
        held.state = SlotState::AwaitPubrel;
        self.index_slot_state(index);
        Ok(())
    }

    /// Ends an exchange whose answer was held: its lease returns and the
    /// operation its result ended stays visible to a late result.
    fn end_held(&mut self, index: usize) -> Result<(), CoreFault> {
        const STALE: CoreFault = CoreFault::StalePlan;
        let lease = {
            let held = self.slots.get(index).ok_or(STALE)?;
            if held.admission.is_none() {
                return Err(STALE);
            }
            held.lease
        };
        if self.leases.release(lease) != Ok(ReleaseOutcome::Released) {
            return Err(STALE);
        }
        let held = self.remove_slot_at(index)?;
        let finished = held.admission.ok_or(STALE)?;
        self.remember_finished(finished)
    }

    fn finish_closed_acks(&mut self, connection: ConnectionGeneration) -> Result<(), CoreFault> {
        // Check only the session's slots when bound; fall back to scanning
        // slots only if the generation is already unbound.
        let candidates: alloc::vec::Vec<usize> =
            if let Some(session_idx) = self.session_of(connection) {
                let sid = self.sessions.as_slice()[session_idx].id;
                self.slots_by_session
                    .get(&sid)
                    .map(|set| set.iter().collect())
                    .unwrap_or_default()
            } else {
                self.slots.iter().map(|(i, _)| i).collect()
            };
        for index in candidates {
            let Some(held) = self.slots.get(index) else {
                continue;
            };
            match held.state {
                SlotState::AnswerHeld { frame, .. }
                    if held
                        .publish_origin
                        .is_some_and(|(origin, _)| origin == connection) =>
                {
                    if matches!(
                        frame,
                        OutboundFrame::Pubrec {
                            reason: AckReason::Success,
                            ..
                        }
                    ) {
                        self.await_pubrel(index)?;
                    } else {
                        self.end_held(index)?;
                    }
                }
                SlotState::AckPending {
                    connection: key, ..
                } if key.generation == connection => {
                    let lease = held.lease;
                    if held.admission.is_none() {
                        return Err(CoreFault::StalePlan);
                    }
                    if self.leases.release(lease) != Ok(ReleaseOutcome::Released) {
                        return Err(CoreFault::StalePlan);
                    }
                    let removed = self.remove_slot_at(index)?;
                    let finished = removed.admission.ok_or(CoreFault::StalePlan)?;
                    self.remember_finished(finished)?;
                }
                _ => {}
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
            RecoveredFact::OutboundDelivery {
                delivery,
                target,
                target_revision,
                qos,
                claim,
                expiry,
                started,
            } => self.add_outbound_fact(
                *delivery,
                *target,
                *target_revision,
                (*qos, *claim, *expiry, *started),
            )?,
            RecoveredFact::Epoch { .. } => self.epoch_recovered = true,
            RecoveredFact::Limits {
                max_targets_per_admission,
                max_topic_bytes,
                max_payload_bytes,
            } => {
                self.limits = Some(ShardLimits {
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
                options,
            } => {
                let route = Route {
                    topic: topic.clone(),
                    target: *target,
                    target_revision: *target_revision,
                    max_qos: *max_qos,
                    options: *options,
                };
                self.push_route(route)?;
            }
            RecoveredFact::Session {
                session,
                client_id,
                revision,
                expiry,
                opened_by,
                detached,
            } => {
                let session = Session {
                    id: *session,
                    client_id: client_id.clone(),
                    revision: *revision,
                    connection: None,
                    expiry: *expiry,
                    opened_by: *opened_by,
                    detached: detached.map(alloc::boxed::Box::new),
                    will_managed: false,
                };
                self.push_session(session)?;
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
                    qos: Qos::ExactlyOnce,
                    publish_origin: None,
                    unmatched: false,
                    lease,
                    admission: None,
                    release_preparations: 0,
                    state: SlotState::AwaitPubrel,
                    retired: false,
                };
                self.push_slot(slot)?;
            }
            // What an older epoch established about its operations
            // changes nothing here: a committed admission is found through
            // its exchange, and no operation of a closed epoch executes.
            RecoveredFact::Operation { .. } | RecoveredFact::ReceiptFrontier(_) => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
