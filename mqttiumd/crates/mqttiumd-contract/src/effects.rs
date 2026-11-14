//! Inputs, effects and the ownership of effects (DEM-007, DEM-008).
//!
//! Inputs and effects are closed unions of plain data: no future, no socket,
//! no closure, no mutable pointer to another owner. Each owner keeps a
//! bounded effect box that survives scheduler turns; nothing in it is dropped
//! because a downstream queue is full.
//!
//! The box is volatile: a crash destroys it. Safety after a crash rests on
//! durable facts and protocol replay, never on this memory.

use crate::capacity::{
    AllocationFailed, BoundedQueue, BoundedVec, CapacityNeed, EffectSlots, Resource, WakeCondition,
    WorkUnits,
};
use crate::command::Command;
use crate::ids::{
    ConnectionGeneration, ConnectionKey, ContinuationId, DeadlineToken, EffectId, ExchangeId,
    InboundPacketId, LogicalTime, OperationId, PublicationId, Revision, RunEpoch, SessionId,
    StoreIncarnation,
};
use crate::outcome::{Knowledge, StoreReply};
use crate::packet::{InboundPacket, OutboundFrame, SharedBytes};

/// Durable phase of an inbound QoS 2 exchange.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InboundPhase {
    /// Admission committed; PUBREC is owed and PUBREL is awaited.
    AwaitPubrel,
}

/// One fact read from durable data during recovery. Identities found here
/// come from the store, never from the bench (DEM-012).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RecoveredFact {
    /// The data set and the run epoch opened for this run.
    Epoch {
        incarnation: StoreIncarnation,
        epoch: RunEpoch,
    },
    /// A persistent session.
    Session {
        session: SessionId,
        client_id: SharedBytes,
        revision: Revision,
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
    /// A validated packet, its connection and its ingress order.
    Packet {
        connection: ConnectionGeneration,
        ingress_order: u64,
        packet: InboundPacket,
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
    /// Propose a frozen command.
    Persist { command: Command },
    /// Reconcile an operation and, if possible, establish a safe fence. It
    /// has its own `EffectId` while targeting the same `OperationId`.
    ResolveAndFence { operation: OperationId },
    /// Read a coherent bounded page of existing obligations.
    LoadWork { session: SessionId, limit: u32 },
    /// Emit one precise frame on one connection generation.
    EmitFrame {
        connection: ConnectionKey,
        frame: OutboundFrame,
    },
    /// Close the designated connection.
    CloseConnection {
        connection: ConnectionGeneration,
        reason: CloseReason,
    },
    /// Arm a deadline token.
    ArmDeadline {
        token: DeadlineToken,
        at: LogicalTime,
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

/// Slots reserved in an effect box before the mutation that fills them.
///
/// It must be either filled through [`Outbox::publish`] or given back with
/// [`Outbox::release`]; dropping it leaves its slots reserved.
#[must_use]
#[derive(Debug)]
pub struct EffectReservation {
    slots: usize,
}

impl EffectReservation {
    /// Slots not yet filled.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.slots
    }
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
#[derive(Debug)]
pub struct Outbox {
    queue: BoundedQueue<OwnedEffect>,
    reserved: usize,
}

impl Outbox {
    /// Preallocates a box of exactly `capacity` slots.
    pub fn with_capacity(capacity: EffectSlots) -> Result<Self, AllocationFailed> {
        let capacity = usize::try_from(capacity.get()).map_err(|_| AllocationFailed)?;
        Ok(Self {
            queue: BoundedQueue::with_capacity(capacity)?,
            reserved: 0,
        })
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

    /// The waiting effects, oldest first.
    pub fn pending(&self) -> impl Iterator<Item = &OwnedEffect> {
        self.queue.iter()
    }

    /// Reserves `slots` places, or states what is missing. Nothing changes
    /// on refusal.
    pub fn reserve(&mut self, slots: usize) -> Result<EffectReservation, CapacityNeed> {
        let free = self.free_slots();
        if slots > free {
            return Err(CapacityNeed {
                resource: Resource::EffectSlots,
                amount: (slots - free) as u64,
                wake: WakeCondition::EffectAccepted,
            });
        }
        self.reserved += slots;
        Ok(EffectReservation { slots })
    }

    /// Puts an effect into a reserved slot. It only fails, returning the
    /// effect, when the reservation is already used up, which is a fault of
    /// the calling transition.
    pub fn publish(
        &mut self,
        reservation: &mut EffectReservation,
        effect: OwnedEffect,
    ) -> Result<(), OwnedEffect> {
        if reservation.slots == 0 {
            return Err(effect);
        }
        self.queue.try_push_back(effect)?;
        reservation.slots -= 1;
        self.reserved -= 1;
        Ok(())
    }

    /// Gives back the unfilled slots of a reservation.
    pub fn release(&mut self, reservation: EffectReservation) {
        self.reserved -= reservation.slots;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{EffectSeq, LogicalOwnerId};

    fn effect(sequence: u64) -> OwnedEffect {
        OwnedEffect::new(
            EffectId {
                epoch: RunEpoch::new(1),
                owner: LogicalOwnerId::new(0),
                sequence: EffectSeq::new(sequence),
            },
            Effect::LoadWork {
                session: SessionId::new(sequence),
                limit: 8,
            },
        )
    }

    fn filled(capacity: u64, count: u64) -> Outbox {
        let mut outbox = Outbox::with_capacity(EffectSlots::new(capacity)).expect("small");
        let mut reservation = outbox.reserve(count as usize).expect("room");
        for sequence in 0..count {
            outbox
                .publish(&mut reservation, effect(sequence))
                .expect("reserved");
        }
        outbox.release(reservation);
        outbox
    }

    fn sequences(outbox: &Outbox) -> Vec<u64> {
        outbox.pending().map(|e| e.id().sequence.get()).collect()
    }

    /// Accepts a fixed number of effects, then reports a full queue.
    struct LimitedSink {
        room: usize,
        taken: Vec<OwnedEffect>,
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
        let need = outbox.reserve(2).expect_err("only one slot is free");
        assert_eq!(
            need,
            CapacityNeed {
                resource: Resource::EffectSlots,
                amount: 1,
                wake: WakeCondition::EffectAccepted,
            }
        );
        assert_eq!(outbox.free_slots(), 1);
        assert_eq!(sequences(&outbox), vec![0, 1]);
    }

    #[test]
    fn reserved_slots_cannot_be_taken_by_someone_else() {
        let mut outbox = Outbox::with_capacity(EffectSlots::new(2)).expect("small");
        let mut first = outbox.reserve(2).expect("room");
        assert_eq!(outbox.free_slots(), 0);
        assert!(outbox.reserve(1).is_err());
        outbox.publish(&mut first, effect(0)).expect("reserved");
        assert_eq!(first.remaining(), 1);
        assert_eq!(outbox.free_slots(), 0);
        outbox.release(first);
        assert_eq!(outbox.free_slots(), 1);
    }

    #[test]
    fn publishing_past_a_reservation_returns_the_effect() {
        let mut outbox = Outbox::with_capacity(EffectSlots::new(2)).expect("small");
        let mut reservation = outbox.reserve(1).expect("room");
        outbox
            .publish(&mut reservation, effect(0))
            .expect("reserved");
        let refused = outbox
            .publish(&mut reservation, effect(1))
            .expect_err("reservation is used up");
        assert_eq!(refused, effect(1));
        assert_eq!(sequences(&outbox), vec![0]);
        assert_eq!(outbox.free_slots(), 1);
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
