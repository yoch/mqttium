//! Inputs, effects and the ownership of effects (DEM-007, DEM-008).
//!
//! Inputs and effects are closed unions of plain data: no future, no socket,
//! no closure, no mutable pointer to another owner. Each owner keeps a
//! bounded effect box that survives scheduler turns; nothing in it is dropped
//! because a downstream queue is full.
//!
//! The box is volatile: a crash destroys it. Safety after a crash rests on
//! durable facts and protocol replay, never on this memory.

use core::num::NonZeroUsize;

use crate::capacity::{
    AllocationFailed, BoundedQueue, BoundedVec, CapacityNeed, EffectSlots, Resource, WakeCondition,
    WorkUnits,
};
use crate::command::Command;
use crate::ids::{
    ConnectionGeneration, ConnectionKey, ContinuationId, DeadlineToken, EffectId, ExchangeId,
    InboundPacketId, LogicalDuration, LogicalOwnerId, LogicalTime, OperationId, PublicationId,
    Revision, RunEpoch, SessionId, StoreIncarnation,
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

/// Identity of an effect box: its owner and the run epoch that created it.
///
/// The identity is supplied by whoever creates the box. It tells apart the
/// boxes of different owners and epochs; it does not tell apart two boxes
/// created with the same pair. This primitive therefore has a
/// precondition: one box per owner and per epoch.
///
/// Lot L0 leaves that precondition to the creator on purpose. Lot L1 must
/// turn it into an established invariant, with a creation authority (an
/// owner factory, an arena or a construction token that cannot be
/// duplicated) and its tests; see the open conditions.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct OutboxId {
    pub owner: LogicalOwnerId,
    pub epoch: RunEpoch,
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
    pub fn with_capacity(
        id: OutboxId,
        capacity: EffectSlots,
        max_reservations: u32,
    ) -> Result<Self, AllocationFailed> {
        let capacity = usize::try_from(capacity.get()).map_err(|_| AllocationFailed)?;
        let table = usize::try_from(max_reservations).map_err(|_| AllocationFailed)?;
        let mut records = BoundedVec::with_capacity(table)?;
        for _ in 0..table {
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
    pub fn reserve(&mut self, slots: NonZeroUsize) -> Result<EffectReservation, CapacityNeed> {
        let slots = slots.get();
        let free = self.free_slots();
        if slots > free {
            return Err(CapacityNeed {
                resource: Resource::EffectSlots,
                amount: (slots - free) as u64,
                wake: WakeCondition::EffectAccepted,
            });
        }
        let Some(index) = self
            .records
            .as_slice()
            .iter()
            .position(|record| !record.held && !record.spent)
        else {
            return Err(CapacityNeed {
                resource: Resource::Reservations,
                amount: 1,
                wake: WakeCondition::ResourceReleased,
            });
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
    pub fn publish(
        &mut self,
        key: EffectReservation,
        effect: OwnedEffect,
    ) -> Result<(), PublishRefused> {
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
        if record.remaining == 0 {
            self.retire(index);
        }
        Ok(())
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
    use crate::ids::EffectSeq;

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

    fn slots(count: usize) -> NonZeroUsize {
        NonZeroUsize::new(count).expect("at least one slot")
    }

    fn outbox_of(owner: u64, capacity: u64) -> Outbox {
        let id = OutboxId {
            owner: LogicalOwnerId::new(owner),
            epoch: RunEpoch::new(1),
        };
        Outbox::with_capacity(id, EffectSlots::new(capacity), 4).expect("small")
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
            CapacityNeed {
                resource: Resource::EffectSlots,
                amount: 1,
                wake: WakeCondition::EffectAccepted,
            }
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
    fn two_boxes_with_one_identity_are_not_told_apart() {
        // The stated limit of this primitive: one box per owner and epoch
        // is a precondition until lot L1 provides a creation authority.
        let mut first = outbox_of(1, 2);
        let mut twin = outbox_of(1, 2);
        let key = first.reserve(slots(1)).expect("room");
        let _own = twin.reserve(slots(1)).expect("room");
        assert_eq!(
            twin.publish(key, effect(0)),
            Ok(()),
            "the twin cannot detect the foreign key"
        );
        assert_invariant(&twin);
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
        let id = OutboxId {
            owner: LogicalOwnerId::new(0),
            epoch: RunEpoch::new(1),
        };
        let mut outbox = Outbox::with_capacity(id, EffectSlots::new(8), 2).expect("small");
        let first = outbox.reserve(slots(1)).expect("room");
        let _second = outbox.reserve(slots(1)).expect("room");
        let before = snapshot(&outbox);
        assert_eq!(
            outbox.reserve(slots(1)),
            Err(CapacityNeed {
                resource: Resource::Reservations,
                amount: 1,
                wake: WakeCondition::ResourceReleased,
            })
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

    #[test]
    fn a_deadline_is_dispatched_only_after_the_persist_it_watches() {
        use crate::ids::{DeadlineToken, OperationId, OperationSeq, StoreIncarnation};

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
