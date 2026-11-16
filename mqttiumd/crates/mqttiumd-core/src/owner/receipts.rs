//! Bounded producer confirmations. Business objects do not retain receipts.
use super::*;
use crate::contract::receipts::{
    MAX_COLLECTION_ROWS, RECEIPT_COLLECTION_TRIGGER, RECEIPT_DATA_WINDOW, RECEIPT_WINDOW,
    ReceiptClass, ReceiptClosure, ReceiptCollection, ReceiptCollectionReply, ReceiptOrigin,
};

/// Targeted reception mutant: closing a live unknown hole is never permitted.
const MUTANT_CONFIRM_UNKNOWN: bool = false;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Entry {
    boundary: ReceiptClosure,
    consumed: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Flight {
    collection: ReceiptCollection,
    answers: Answers,
    effect: EffectId,
    deadline: DeadlineToken,
}

#[derive(Debug)]
pub(super) struct State {
    pub(super) enabled: bool,
    registered: bool,
    acknowledged: ReceiptClosure,
    entries: BoundedVec<Entry>,
    pending: Option<Flight>,
    sweep: bool,
}

#[expect(
    clippy::large_enum_variant,
    reason = "an allocation-free plan carries the bounded retry history by value"
)]
#[derive(Debug)]
pub(super) enum Change {
    Submit { flight: Flight, activate: bool },
    Confirm { closure: ReceiptClosure, more: bool },
}

#[cfg(test)]
#[derive(PartialEq, Eq, Debug)]
pub(super) struct Snapshot {
    enabled: bool,
    registered: bool,
    acknowledged: ReceiptClosure,
    entries: alloc::vec::Vec<Entry>,
    pending: Option<Flight>,
    sweep: bool,
}

impl State {
    #[cfg(test)]
    pub(super) fn test_request_sweep(&mut self) {
        self.sweep = true;
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> Snapshot {
        Snapshot {
            enabled: self.enabled,
            registered: self.registered,
            acknowledged: self.acknowledged,
            entries: self.entries.as_slice().to_vec(),
            pending: self.pending,
            sweep: self.sweep,
        }
    }

    pub(super) fn new(origin: ReceiptOrigin) -> Result<Self, AllocationFailed> {
        Ok(Self {
            enabled: false,
            registered: false,
            acknowledged: ReceiptClosure::empty(origin),
            entries: BoundedVec::with_capacity(RECEIPT_WINDOW)?,
            pending: None,
            sweep: false,
        })
    }

    fn check(&self, class: ReceiptClass) -> Result<(), Stop> {
        if !self.enabled {
            return Ok(());
        }
        let bound = match class {
            ReceiptClass::Business => RECEIPT_DATA_WINDOW,
            ReceiptClass::Control => RECEIPT_WINDOW,
        };
        if !self.registered || self.entries.len() >= bound {
            return Err(Stop::Need(CapacityNeed {
                resource: Resource::ReceiptSlots,
                amount: 1,
                wake: WakeCondition::ResultDelivered,
            }));
        }
        Ok(())
    }

    fn next_collection(&self) -> Option<ReceiptCollection> {
        let mut closure = self.acknowledged;
        let mut consumed = 0;
        for entry in self
            .entries
            .as_slice()
            .iter()
            .take(usize::from(MAX_COLLECTION_ROWS))
        {
            if !entry.consumed && !MUTANT_CONFIRM_UNKNOWN {
                break;
            }
            closure = entry.boundary;
            consumed += 1;
        }
        let pressured = self.entries.len() >= RECEIPT_DATA_WINDOW;
        if self.registered
            && !self.sweep
            && consumed < RECEIPT_COLLECTION_TRIGGER
            && !(pressured && consumed > 0)
        {
            return None;
        }
        Some(ReceiptCollection {
            closure,
            limit: MAX_COLLECTION_ROWS,
        })
    }

    pub(super) fn apply_issued(&mut self, entry: Option<Entry>) -> Result<(), CoreFault> {
        if let Some(entry) = entry {
            self.entries
                .try_push(entry)
                .map_err(|_| CoreFault::ReservationOverrun)?;
        }
        Ok(())
    }

    pub(super) fn consume(&mut self, operation: Option<OperationId>) -> Result<(), CoreFault> {
        if !self.enabled {
            return Ok(());
        }
        if let Some(operation) = operation {
            let Some(entry) = self.entries.as_mut_slice().iter_mut().find(|entry| {
                entry.boundary.through == operation.sequence()
                    && entry.boundary.origin == ReceiptOrigin::of(operation)
            }) else {
                return Err(CoreFault::ImpossibleInput);
            };
            entry.consumed = true;
        }
        Ok(())
    }

    pub(super) fn apply(&mut self, change: Change) -> Result<(), CoreFault> {
        match change {
            Change::Submit { flight, activate } => {
                self.enabled |= activate;
                self.pending = Some(flight);
            }
            Change::Confirm { closure, more } => {
                let remove = self
                    .entries
                    .as_slice()
                    .iter()
                    .take_while(|entry| entry.boundary.through <= closure.through)
                    .count();
                self.entries.as_mut_slice().rotate_left(remove);
                for _ in 0..remove {
                    let _ = self.entries.pop();
                }
                self.acknowledged = closure;
                self.registered = true;
                self.pending = None;
                self.sweep = more;
            }
        }
        Ok(())
    }

    pub(super) fn work(&self, input: &Input) -> u64 {
        if !self.enabled {
            return 0;
        }
        match input {
            Input::StoreResult { .. } => u64::try_from(self.entries.len()).unwrap_or(u64::MAX),
            Input::MaintainReceipts => u64::from(MAX_COLLECTION_ROWS),
            Input::ReceiptsCollected { .. } => u64::from(MAX_COLLECTION_ROWS)
                .saturating_add(u64::try_from(self.entries.len()).unwrap_or(u64::MAX)),
            _ => 0,
        }
    }

    pub(super) fn retired(&self, operation: OperationId) -> bool {
        self.registered
            && ReceiptOrigin::of(operation) == self.acknowledged.origin
            && operation.sequence() <= self.acknowledged.through
    }

    pub(super) fn priority(&self) -> Option<(EffectId, DeadlineToken)> {
        self.pending.map(|flight| (flight.effect, flight.deadline))
    }

    pub(super) fn maintenance_input(&self, input: &Input) -> bool {
        matches!(
            input,
            Input::MaintainReceipts | Input::ReceiptsCollected { .. }
        ) || matches!(input,Input::Deadline {token,..} if self.pending.is_some_and(|f|f.deadline==*token))
    }
}

impl Owner {
    /// Whether the current origin's empty prefix was durably registered.
    #[must_use]
    pub fn collection_ready(&self) -> bool {
        self.receipts.enabled && self.receipts.registered
    }

    /// Bounded transport correlation for the current collector attempt and
    /// its retained retries. The adapter never creates another retry loop.
    #[must_use]
    pub fn collection_reply_is_live(
        &self,
        effect: EffectId,
        collection: ReceiptCollection,
    ) -> bool {
        self.receipts
            .pending
            .is_some_and(|flight| flight.collection == collection && flight.answers.names(effect))
    }

    /// Absolute durable frontier, unconfirmed sequence span and unresolved
    /// holes. Counts are observations, never reusable credit grants.
    #[must_use]
    pub fn receipt_status(&self) -> Option<(ReceiptClosure, usize, usize)> {
        self.receipts.enabled.then(|| {
            (
                self.receipts.acknowledged,
                self.receipts.entries.len(),
                self.receipts
                    .entries
                    .as_slice()
                    .iter()
                    .filter(|entry| !entry.consumed)
                    .count(),
            )
        })
    }

    /// Only a live unresolved command or collector retains its current timer.
    #[must_use]
    pub fn deadline_is_live(&self, token: DeadlineToken) -> bool {
        self.receipts
            .pending
            .is_some_and(|flight| flight.deadline == token)
            || self
                .slots
                .as_slice()
                .iter()
                .filter_map(Slot::in_flight)
                .any(|flight| flight.deadline == token)
            || self.pubsub.deadline_is_live(token)
    }

    fn receipt_flight(
        &self,
        collection: ReceiptCollection,
        previous: Option<Flight>,
        draft: &mut Draft,
    ) -> Result<Flight, Stop> {
        let effect = draft.emit(Effect::CollectReceipts { collection })?;
        let deadline = draw(&mut draft.counters.deadline, DeadlineToken::checked_next)?;
        draft.emit(Effect::ArmDeadline {
            token: deadline,
            after: self.resolution_delay,
        })?;
        Ok(Flight {
            collection,
            effect,
            deadline,
            answers: previous.map_or_else(|| Answers::new(effect), |old| old.answers.with(effect)),
        })
    }

    pub(super) fn plan_receipt_maintenance(
        &self,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        if self.receipts.pending.is_some() {
            return Ok(super::Change::Nothing);
        }
        if !self.receipts.enabled {
            if self.counters.operation != Some(OperationSeq::new(1)) {
                return Err(IMPOSSIBLE);
            }
            if self.outbox.capacity() < MAX_TRANSITION_SLOTS + 2 {
                return Err(Stop::Fatal(CoreFault::BoxTooSmall {
                    requested: MAX_TRANSITION_SLOTS + 2,
                    capacity: self.outbox.capacity(),
                }));
            }
        }
        let Some(collection) = self.receipts.next_collection() else {
            return Ok(super::Change::Nothing);
        };
        let flight = self.receipt_flight(collection, None, draft)?;
        Ok(super::Change::Receipts(Change::Submit {
            flight,
            activate: !self.receipts.enabled,
        }))
    }

    pub(super) fn plan_receipt_result(
        &self,
        effect: EffectId,
        collection: ReceiptCollection,
        reply: ReceiptCollectionReply,
    ) -> Result<super::Change, Stop> {
        if effect.epoch != self.epoch
            || effect.owner != self.id
            || !issued(effect.sequence, self.counters.effect)
        {
            return Err(IMPOSSIBLE);
        }
        let Some(flight) = self.receipts.pending else {
            return Ok(super::Change::Nothing);
        };
        if collection != flight.collection || !flight.answers.names(effect) {
            return Ok(super::Change::Nothing);
        }
        match reply {
            ReceiptCollectionReply::PendingOrUnknown => Ok(super::Change::Nothing),
            ReceiptCollectionReply::Refused(_) => Err(IMPOSSIBLE),
            ReceiptCollectionReply::Applied { closure, more, .. } => {
                if closure != collection.closure {
                    return Err(IMPOSSIBLE);
                }
                Ok(super::Change::Receipts(Change::Confirm { closure, more }))
            }
        }
    }

    pub(super) fn plan_receipt_deadline(
        &self,
        token: DeadlineToken,
        draft: &mut Draft,
    ) -> Result<Option<super::Change>, Stop> {
        let Some(previous) = self
            .receipts
            .pending
            .filter(|flight| flight.deadline == token)
        else {
            return Ok(None);
        };
        let flight = self.receipt_flight(previous.collection, Some(previous), draft)?;
        Ok(Some(super::Change::Receipts(Change::Submit {
            flight,
            activate: false,
        })))
    }

    pub(super) fn prepare_receipt_entry(
        &self,
        recipe: &Recipe,
        draft: &mut Draft,
    ) -> Result<(), Stop> {
        self.receipts.check(ReceiptClass::of(recipe))?;
        if self.receipts.enabled {
            let operation = recipe.operation();
            draft.receipt_issued = Some(Entry {
                boundary: ReceiptClosure {
                    origin: ReceiptOrigin::of(operation),
                    through: operation.sequence(),
                    publication_through: PublicationSeq::new(
                        draft
                            .counters
                            .publication
                            .map_or(u64::MAX, |next| next.get() - 1),
                    ),
                    payload_through: PayloadSeq::new(
                        draft
                            .counters
                            .payload
                            .map_or(u64::MAX, |next| next.get() - 1),
                    ),
                },
                consumed: false,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> State {
        let mut state = State::new(ReceiptOrigin {
            incarnation: StoreIncarnation::new(9),
            epoch: RunEpoch::new(2),
            owner: LogicalOwnerId::new(3),
        })
        .unwrap();
        state.enabled = true;
        state.registered = true;
        state
    }

    fn entry(state: &State, sequence: u64, consumed: bool) -> Entry {
        Entry {
            boundary: ReceiptClosure {
                through: OperationSeq::new(sequence),
                publication_through: PublicationSeq::new(sequence / 2),
                payload_through: PayloadSeq::new(sequence / 2),
                ..state.acknowledged
            },
            consumed,
        }
    }

    #[test]
    fn a_live_unknown_hole_prevents_closing_later_consumed_results() {
        let mut state = state();
        for sequence in 1..=RECEIPT_WINDOW as u64 {
            state
                .apply_issued(Some(entry(&state, sequence, sequence != 1)))
                .unwrap();
        }
        assert!(state.next_collection().is_none(), "closure_crossed_unknown");
        assert!(state.check(ReceiptClass::Business).is_err());
        assert!(state.check(ReceiptClass::Control).is_err());
        let first = OperationId::new(
            state.acknowledged.origin.incarnation,
            state.acknowledged.origin.epoch,
            state.acknowledged.origin.owner,
            OperationSeq::new(1),
        );
        state.consume(Some(first)).unwrap();
        let collection = state.next_collection().unwrap();
        assert_eq!(
            collection.closure.through.get(),
            u64::from(MAX_COLLECTION_ROWS)
        );
        assert_eq!(
            collection.closure.publication_through.get(),
            u64::from(MAX_COLLECTION_ROWS) / 2,
            "identity boundary must be taken at this pass, never at the last issued operation"
        );
        state
            .apply(Change::Confirm {
                closure: collection.closure,
                more: false,
            })
            .unwrap();
        assert_eq!(
            state.entries.len(),
            RECEIPT_WINDOW - usize::from(MAX_COLLECTION_ROWS)
        );
        assert!(state.check(ReceiptClass::Business).is_err());
        assert!(state.check(ReceiptClass::Control).is_ok());
    }

    #[test]
    fn business_cannot_spend_the_continuation_window() {
        let mut state = state();
        for sequence in 1..=RECEIPT_DATA_WINDOW as u64 {
            state
                .apply_issued(Some(entry(&state, sequence, false)))
                .unwrap();
        }
        assert!(state.check(ReceiptClass::Business).is_err());
        assert!(state.check(ReceiptClass::Control).is_ok());
        assert!(state.next_collection().is_none());
    }

    #[test]
    fn empty_hole_blocked_and_below_threshold_producers_do_not_spin() {
        let mut state = state();
        assert!(state.next_collection().is_none());
        for sequence in 1..RECEIPT_COLLECTION_TRIGGER as u64 {
            state
                .apply_issued(Some(entry(&state, sequence, true)))
                .unwrap();
        }
        assert!(state.next_collection().is_none());
        state.sweep = true;
        let collection = state.next_collection().unwrap();
        state
            .apply(Change::Confirm {
                closure: collection.closure,
                more: false,
            })
            .unwrap();
        assert!(state.entries.is_empty());
        assert!(state.next_collection().is_none());
    }

    #[test]
    fn pressure_can_close_a_short_known_prefix_without_crossing_its_next_hole() {
        let mut state = state();
        for sequence in 1..=RECEIPT_DATA_WINDOW as u64 {
            state
                .apply_issued(Some(entry(&state, sequence, sequence == 1)))
                .unwrap();
        }
        let collection = state.next_collection().unwrap();
        assert_eq!(collection.closure.through.get(), 1);
        state
            .apply(Change::Confirm {
                closure: collection.closure,
                more: false,
            })
            .unwrap();
        assert!(state.check(ReceiptClass::Business).is_ok());
        assert!(state.next_collection().is_none());
    }
}
