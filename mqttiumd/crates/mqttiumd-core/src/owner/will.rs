//! One bounded durable-intent cursor and one admission/resolution flight.
//!
//! Source-session cleanup never removes this work. The store authorizes and
//! consumes the exact Pending intent in the same transaction as publication.
use super::*;
use crate::contract::will::{WillIntent, WillPublication, WillState};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WillDeferredReason {
    Fanout,
    ReceiptCapacity,
    PayloadLimit,
    StoreRefusal,
}
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct WillStatus {
    pub reading: bool,
    pub admission_pending: bool,
    pub deferred: Option<(OperationId, WillDeferredReason)>,
}
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Read {
    effect: EffectId,
    after: Option<OperationId>,
}
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Flight {
    will: OperationId,
    publication: PublicationId,
    next: Option<OperationId>,
    in_flight: InFlight,
}
#[derive(Debug)]
pub(super) struct State {
    after: Option<OperationId>,
    read: Option<Read>,
    flight: Option<Flight>,
    deferred: Option<(OperationId, WillDeferredReason)>,
}
impl State {
    pub(super) const fn new() -> Self {
        Self {
            after: None,
            read: None,
            flight: None,
            deferred: None,
        }
    }
    pub(super) fn deadline_is_live(&self, token: DeadlineToken) -> bool {
        self.flight
            .is_some_and(|flight| flight.in_flight.deadline == token)
    }
}
#[derive(Debug)]
pub(super) enum Change {
    Scan(Read),
    Page {
        next: Option<OperationId>,
        deferred: Option<(OperationId, WillDeferredReason)>,
    },
    Submit(Flight),
    Await {
        phase: CommandPhase,
        resolution: Option<(DeadlineToken, EffectId)>,
    },
    Done {
        finished: Finished,
        deferred: bool,
    },
}
fn changed(change: Change) -> super::Change {
    super::Change::Will(change)
}
impl Owner {
    #[must_use]
    pub fn will_status(&self) -> WillStatus {
        WillStatus {
            reading: self.wills.read.is_some(),
            admission_pending: self.wills.flight.is_some(),
            deferred: self.wills.deferred,
        }
    }
    #[must_use]
    pub fn will_dispatch(&self) -> Option<(OperationId, DeadlineToken)> {
        self.wills
            .flight
            .map(|flight| (flight.in_flight.operation, flight.in_flight.deadline))
    }
    #[must_use]
    pub fn will_operation_is_live(&self, operation: OperationId) -> bool {
        self.wills
            .flight
            .is_some_and(|flight| flight.in_flight.operation == operation)
    }
    pub(super) fn will_work_bound(&self, input: &Input) -> u64 {
        if !matches!(input, Input::WillPage { .. }) {
            return 0;
        }
        capacity_of(self.routes.capacity())
            .saturating_mul(capacity_of(self.sessions.capacity()).saturating_add(17))
            .saturating_add(256)
    }
    pub(super) fn plan_will_discover(&self, draft: &mut Draft) -> Result<super::Change, Stop> {
        if self.wills.read.is_some()
            || self.wills.flight.is_some()
            || self.lifecycle.is_some()
            || self.pubsub.control_pending()
        {
            return Ok(super::Change::Nothing);
        }
        let effect = draft.emit(Effect::LoadWills {
            after: self.wills.after,
            limit: 1,
        })?;
        Ok(changed(Change::Scan(Read {
            effect,
            after: self.wills.after,
        })))
    }
    pub(super) fn plan_will_page(
        &self,
        effect: EffectId,
        items: &[WillIntent],
        next: Option<OperationId>,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        let Some(read) = self.wills.read.filter(|read| read.effect == effect) else {
            return Ok(super::Change::Nothing);
        };
        if items.len() > 1 || (items.is_empty() && next.is_some()) {
            return Err(IMPOSSIBLE);
        }
        let Some(intent) = items.first() else {
            return Ok(changed(Change::Page {
                next: None,
                deferred: None,
            }));
        };
        if intent.state != WillState::Pending
            || intent.id.incarnation() != self.incarnation
            || intent.id.epoch().get() == 0
            || intent.id.epoch() > self.epoch
            || intent.id.sequence().get() == 0
            || intent.session.get() == 0
            || read.after.is_some_and(|after| intent.id <= after)
            || next.is_some_and(|next| next != intent.id)
            || intent.client_id.is_empty()
            || intent.client_id.len() > crate::contract::session::MAX_SESSION_CLIENT_BYTES
            || intent.client_id.as_slice().contains(&0)
            || core::str::from_utf8(intent.client_id.as_slice()).is_err()
            || intent.spec.check().is_err()
        {
            return Err(IMPOSSIBLE);
        }
        let defer = |reason| {
            changed(Change::Page {
                next,
                deferred: Some((intent.id, reason)),
            })
        };
        let limits = self.limits.ok_or(IMPOSSIBLE)?;
        if intent.spec.topic.len() > limits.max_topic_bytes as usize
            || intent.spec.payload.len() as u64 > limits.max_payload_bytes.get()
        {
            return Ok(defer(WillDeferredReason::PayloadLimit));
        }
        if !self.receipts.control_available() {
            return Ok(defer(WillDeferredReason::ReceiptCapacity));
        }
        let selected = match super::routing::select_client(
            self.routes.as_slice(),
            self.sessions.as_slice(),
            Some(intent.client_id.as_slice()),
            intent.spec.topic.as_slice(),
            Qos::AtLeastOnce,
            limits.max_targets,
        ) {
            Ok(selected) => selected,
            Err(super::routing::Refusal::Capacity) => return Ok(defer(WillDeferredReason::Fanout)),
            Err(super::routing::Refusal::Revision) => return Err(IMPOSSIBLE),
        };
        let operation = self.operation(draft)?;
        let publication = PublicationId::new(
            self.incarnation,
            self.epoch,
            self.id,
            draw(
                &mut draft.counters.publication,
                PublicationSeq::checked_next,
            )?,
        );
        let payload_id = PayloadId::new(
            self.incarnation,
            self.epoch,
            self.id,
            draw(&mut draft.counters.payload, PayloadSeq::checked_next)?,
        );
        let mut targets = BoundedVec::with_capacity(selected.len())?;
        for (index, target) in selected.iter().enumerate() {
            targets
                .try_push(Target {
                    session: target.session,
                    session_revision: target.revision,
                    delivery: DeliveryId {
                        publication,
                        index: u32::try_from(index).map_err(|_| IMPOSSIBLE)?,
                    },
                    effective_qos: target.qos,
                    cause: DeliveryCause::LiveExact,
                })
                .map_err(|_| IMPOSSIBLE)?;
        }
        let length = PayloadBytes::new(intent.spec.payload.len() as u64);
        let recipe = Recipe::WillPublication(WillPublication {
            operation,
            will: intent.id,
            publication,
            route_revision: self.route_revision.ok_or(IMPOSSIBLE)?,
            topic: intent.spec.topic.clone(),
            payload: PayloadRef {
                id: payload_id,
                length,
            },
            costs: Costs {
                payload_bytes: length,
                references: References::new(selected.len() as u64),
                exchange_slots: ExchangeSlots::ZERO,
                receipt_slots: ReceiptSlots::new(1),
            },
            targets,
        });
        let in_flight =
            self.plan_submission(operation, recipe, Some(intent.spec.payload.clone()), draft)?;
        Ok(changed(Change::Submit(Flight {
            will: intent.id,
            publication,
            next,
            in_flight,
        })))
    }
    pub(super) fn plan_will_result(
        &self,
        effect: EffectId,
        operation: OperationId,
        reply: &StoreReply,
        draft: &mut Draft,
    ) -> Result<Option<super::Change>, Stop> {
        let Some(flight) = self
            .wills
            .flight
            .filter(|flight| flight.in_flight.operation == operation)
        else {
            return Ok(None);
        };
        if !flight.in_flight.answers.names(effect) {
            return Ok(Some(super::Change::Nothing));
        }
        let outcome = match reply {
            StoreReply::Outcome(outcome) => *outcome,
            StoreReply::Violation(StoreViolation::RecipeMismatch) => {
                return Err(Stop::Fatal(CoreFault::RecipeMismatch { operation }));
            }
        };
        let verdict = flight
            .in_flight
            .phase
            .on(CommandEvent::StoreResult(OutcomeClass::of(&outcome)));
        let (phase, facts, deferred) = match (verdict, outcome) {
            (Verdict::Transition(CommandPhase::Committed), StoreOutcome::Committed(facts)) => {
                if facts
                    != (CommitFacts::WillPublished {
                        will: flight.will,
                        publication: flight.publication,
                    })
                {
                    return Err(Stop::Fatal(CoreFault::ContradictoryOutcome { operation }));
                }
                (CommandPhase::Consumed, Some(facts), false)
            }
            (
                Verdict::Transition(CommandPhase::RejectedCertain),
                StoreOutcome::RejectedCertain(_),
            ) => (CommandPhase::RejectedCertain, None, true),
            (Verdict::Transition(CommandPhase::Indeterminate), _) => {
                return Ok(Some(changed(Change::Await {
                    phase: CommandPhase::Indeterminate,
                    resolution: None,
                })));
            }
            // A receipt cannot retire before this owner consumed its known result.
            _ => return Err(IMPOSSIBLE),
        };
        draft.receipt_consumed = Some(operation);
        Ok(Some(changed(Change::Done {
            finished: Finished {
                operation,
                answers: flight.in_flight.answers,
                phase,
                facts,
            },
            deferred,
        })))
    }
    pub(super) fn plan_will_deadline(
        &self,
        token: DeadlineToken,
        draft: &mut Draft,
    ) -> Result<Option<super::Change>, Stop> {
        let Some(flight) = self
            .wills
            .flight
            .filter(|flight| flight.in_flight.deadline == token)
        else {
            return Ok(None);
        };
        let Verdict::Transition(phase) = flight.in_flight.phase.on(CommandEvent::ResultTimeout)
        else {
            return Err(IMPOSSIBLE);
        };
        let effect = draft.emit(Effect::ResolveAndFence {
            operation: flight.in_flight.operation,
        })?;
        let deadline = draw(&mut draft.counters.deadline, DeadlineToken::checked_next)?;
        draft.emit(Effect::ArmDeadline {
            token: deadline,
            after: self.resolution_delay,
        })?;
        Ok(Some(changed(Change::Await {
            phase,
            resolution: Some((deadline, effect)),
        })))
    }
    pub(super) fn apply_will(&mut self, change: Change) -> Result<(), CoreFault> {
        match change {
            Change::Scan(read) => self.wills.read = Some(read),
            Change::Page { next, deferred } => {
                self.wills.read = None;
                self.wills.after = next;
                self.wills.deferred = deferred;
            }
            Change::Submit(flight) => {
                self.wills.read = None;
                self.wills.flight = Some(flight);
                self.wills.deferred = None;
            }
            Change::Await { phase, resolution } => {
                let flight = self.wills.flight.as_mut().ok_or(CoreFault::StalePlan)?;
                flight.in_flight.phase = phase;
                if let Some((deadline, effect)) = resolution {
                    flight.in_flight.deadline = deadline;
                    flight.in_flight.answers = flight.in_flight.answers.with(effect);
                }
            }
            Change::Done { finished, deferred } => {
                let flight = self.wills.flight.take().ok_or(CoreFault::StalePlan)?;
                self.wills.after = flight.next;
                self.wills.deferred =
                    deferred.then_some((flight.will, WillDeferredReason::StoreRefusal));
                remember(&mut self.finished, finished)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::command::StartupNonce;
    use crate::contract::effects::SubmitResult;
    use crate::contract::ids::LogicalTime;
    use crate::contract::outcome::{EpochOpened, EpochOpening, OpeningOutcome, OpeningReply};
    use crate::contract::pubsub::SubscriptionOptions;
    use crate::contract::session::{SessionAction, SessionChange, SessionExpiry};
    use crate::contract::will::WillSpec;
    use alloc::vec::Vec;

    const TURN: TurnGrant = TurnGrant {
        work_limit: WorkUnits::new(100_000),
    };
    struct Taking(Vec<(EffectId, Effect)>);
    impl EffectSink for Taking {
        fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
            self.0.push(effect.into_parts());
            SubmitResult::Accepted
        }
    }
    fn nz(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).unwrap()
    }
    fn bytes(value: &[u8]) -> SharedBytes {
        SharedBytes::copy_from(value, 128).unwrap()
    }
    fn apply(owner: &mut Owner, input: Input) -> Vec<(EffectId, Effect)> {
        let result = owner.step(input, TURN);
        assert!(matches!(result, StepOutcome::Applied { .. }), "{result:?}");
        let mut sink = Taking(Vec::new());
        owner.dispatch(&mut sink);
        sink.0
    }
    fn world() -> Owner {
        let incarnation = StoreIncarnation::new(1);
        let epoch = RunEpoch::new(2);
        let mut opening = EpochOpening::new(incarnation, StartupNonce::new([8; 16]));
        let command = opening.submit(RunEpoch::new(1)).unwrap();
        let _ = opening
            .observe(OpeningReply {
                command,
                outcome: OpeningOutcome::Committed(EpochOpened { incarnation, epoch }),
            })
            .unwrap();
        let mut authority =
            OwnerAuthority::from_grant(opening.hand_over().unwrap(), nz(1)).unwrap();
        let outbox = authority
            .create_outbox(
                LogicalOwnerId::new(4),
                NonZeroUsize::new(32).unwrap(),
                nz(8),
            )
            .unwrap();
        let mut owner = Owner::new(
            OwnerConfig {
                max_sessions: nz(24),
                max_routes: nz(24),
                max_exchanges: nz(8),
                account: AccountId::new(1),
                payload_bytes: PayloadBytes::new(1024),
                resolution_delay: LogicalDuration::new(1),
                retained_operations: nz(32),
                remembered_connections: nz(4),
            },
            outbox,
        )
        .unwrap();
        let mut facts = Vec::new();
        facts.push(RecoveredFact::Epoch { incarnation, epoch });
        facts.push(RecoveredFact::Limits {
            max_targets_per_admission: 16,
            max_topic_bytes: 128,
            max_payload_bytes: PayloadBytes::new(128),
        });
        facts.push(RecoveredFact::RouteTable {
            revision: Revision::new(0),
        });
        for target in 1..=2 {
            facts.push(RecoveredFact::Route {
                topic: bytes(b"atelier/#"),
                target: SessionId::new(target),
                target_revision: Revision::new(1),
                max_qos: Qos::AtLeastOnce,
                options: if target == 2 {
                    SubscriptionOptions::from_bits(4).unwrap()
                } else {
                    SubscriptionOptions::DEFAULT
                },
            });
        }
        for (target, client_id) in [(1, b"dashboard".as_slice()), (2, b"capteur".as_slice())] {
            facts.push(RecoveredFact::Session {
                session: SessionId::new(target),
                client_id: bytes(client_id),
                revision: Revision::new(1),
                expiry: SessionExpiry::Never,
                opened_by: None,
                detached: None,
            });
        }
        assert!(
            apply(
                &mut owner,
                Input::Recovery {
                    facts: BoundedVec::from_slice(&facts, facts.len()).unwrap(),
                    last: true
                }
            )
            .is_empty()
        );
        owner
    }
    fn intent(sequence: u64) -> WillIntent {
        WillIntent {
            id: OperationId::new(
                StoreIncarnation::new(1),
                RunEpoch::new(1),
                LogicalOwnerId::new(4),
                OperationSeq::new(sequence),
            ),
            session: SessionId::new(99),
            client_id: bytes(b"capteur"),
            spec: WillSpec {
                topic: bytes(b"atelier/capteur/status"),
                payload: bytes(b"offline"),
            },
            state: WillState::Pending,
        }
    }
    fn page(owner: &mut Owner, value: &WillIntent) -> Vec<(EffectId, Effect)> {
        let read = apply(owner, Input::DiscoverWills);
        assert!(matches!(
            read.as_slice(),
            [(_, Effect::LoadWills { limit: 1, .. })]
        ));
        apply(
            owner,
            Input::WillPage {
                effect: read[0].0,
                items: BoundedVec::from_slice(core::slice::from_ref(value), 1).unwrap(),
                next: None,
            },
        )
    }
    fn submitted(effects: &[(EffectId, Effect)]) -> (EffectId, OperationId, CommitFacts) {
        let (effect, Effect::Persist { command }) = &effects[0] else {
            panic!("Will admission expected");
        };
        let Recipe::WillPublication(value) = command.recipe.get() else {
            panic!("wrong recipe");
        };
        (
            *effect,
            value.operation,
            CommitFacts::WillPublished {
                will: value.will,
                publication: value.publication,
            },
        )
    }
    #[test]
    fn pending_will_admits_after_source_cleanup_without_a_fabricated_exchange() {
        let mut owner = world();
        let value = intent(9);
        let effects = page(&mut owner, &value);
        let (_, operation, _) = submitted(&effects);
        let Effect::Persist { command } = &effects[0].1 else {
            panic!();
        };
        let Recipe::WillPublication(publication) = command.recipe.get() else {
            panic!();
        };
        assert_eq!(publication.will, value.id);
        assert_eq!(
            publication.targets.len(),
            1,
            "no_local_must_compare_frozen_client_id_after_source_cleanup"
        );
        assert_eq!(publication.targets.as_slice()[0].session, SessionId::new(1));
        assert_eq!(publication.costs.exchange_slots, ExchangeSlots::ZERO);
        assert_eq!(publication.costs.references, References::new(1));
        assert_eq!(command.payload.as_ref().unwrap().as_slice(), b"offline");
        assert!(
            owner.slots.is_empty(),
            "will_must_not_create_an_inbound_exchange"
        );
        assert!(owner.will_operation_is_live(operation));
        assert!(effects.iter().all(|(_, effect)| !matches!(
            effect,
            Effect::EmitFrame { .. } | Effect::EmitPublish { .. }
        )));
    }
    #[test]
    fn will_unknown_result_resolves_the_same_operation_and_consumes_once() {
        let mut owner = world();
        let effects = page(&mut owner, &intent(9));
        let (effect, operation, facts) = submitted(&effects);
        assert!(
            apply(
                &mut owner,
                Input::StoreResult {
                    effect,
                    operation,
                    reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
                }
            )
            .is_empty()
        );
        assert!(
            apply(&mut owner, Input::DiscoverWills).is_empty(),
            "unknown_will_admission_must_not_restart"
        );
        let (_, deadline) = owner.will_dispatch().unwrap();
        let retry = apply(
            &mut owner,
            Input::Deadline {
                token: deadline,
                now: LogicalTime::new(100),
            },
        );
        let (resolve, repeated) = retry
            .iter()
            .find_map(|(effect, value)| match value {
                Effect::ResolveAndFence { operation } => Some((*effect, *operation)),
                _ => None,
            })
            .expect("will_resolution_missing_for_unknown_admission");
        assert_eq!(
            repeated, operation,
            "will_resolution_changed_operation_identity"
        );
        assert_eq!(
            retry[0].0, resolve,
            "will_resolution_deadline_preceded_submission"
        );
        assert!(owner.deadline_is_live(owner.will_dispatch().unwrap().1));
        assert!(
            apply(
                &mut owner,
                Input::StoreResult {
                    effect: resolve,
                    operation,
                    reply: StoreReply::Outcome(StoreOutcome::Committed(facts))
                }
            )
            .is_empty()
        );
        assert!(!owner.will_status().admission_pending);
        assert!(
            apply(
                &mut owner,
                Input::StoreResult {
                    effect,
                    operation,
                    reply: StoreReply::Outcome(StoreOutcome::Committed(facts))
                }
            )
            .is_empty(),
            "duplicate_will_result_readmitted"
        );
        assert_eq!(
            owner
                .finished
                .iter()
                .filter(|done| done.operation == operation)
                .count(),
            1
        );
    }
    #[test]
    fn an_old_page_cannot_create_a_second_admission_after_its_result() {
        let mut owner = world();
        let value = intent(9);
        let read = apply(&mut owner, Input::DiscoverWills)[0].0;
        let effects = apply(
            &mut owner,
            Input::WillPage {
                effect: read,
                items: BoundedVec::from_slice(core::slice::from_ref(&value), 1).unwrap(),
                next: None,
            },
        );
        let (effect, operation, facts) = submitted(&effects);
        let _ = apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(facts)),
            },
        );
        assert!(
            apply(
                &mut owner,
                Input::WillPage {
                    effect: read,
                    items: BoundedVec::from_slice(&[value], 1).unwrap(),
                    next: None
                }
            )
            .is_empty(),
            "stale_will_page_created_admission"
        );
    }
    #[test]
    fn fanout_refusal_keeps_the_durable_intent_and_advances_the_bounded_cursor() {
        let mut owner = world();
        owner.routes.as_mut_slice()[1].options = SubscriptionOptions::DEFAULT;
        owner.limits.as_mut().unwrap().max_targets = 1;
        let value = intent(9);
        let read = apply(&mut owner, Input::DiscoverWills)[0].0;
        assert!(
            apply(
                &mut owner,
                Input::WillPage {
                    effect: read,
                    items: BoundedVec::from_slice(core::slice::from_ref(&value), 1).unwrap(),
                    next: Some(value.id)
                }
            )
            .is_empty()
        );
        assert_eq!(
            owner.will_status().deferred,
            Some((value.id, WillDeferredReason::Fanout)),
            "will_fanout_was_truncated_or_dropped"
        );
        assert!(
            matches!(apply(&mut owner, Input::DiscoverWills).as_slice(), [(_, Effect::LoadWills { after: Some(after), limit: 1 })] if *after == value.id)
        );
    }
    #[test]
    fn empty_will_inventory_has_no_self_rearming_busy_loop() {
        let mut owner = world();
        let read = apply(&mut owner, Input::DiscoverWills)[0].0;
        assert!(
            apply(
                &mut owner,
                Input::WillPage {
                    effect: read,
                    items: BoundedVec::with_capacity(0).unwrap(),
                    next: None
                }
            )
            .is_empty()
        );
        assert!(!owner.will_status().reading);
        assert_eq!(owner.will_status().deferred, None);
    }
    #[test]
    fn certain_will_refusal_does_not_mean_the_intent_is_cancelled() {
        let mut owner = world();
        let effects = page(&mut owner, &intent(9));
        let (effect, operation, _) = submitted(&effects);
        let _ = apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(
                    RejectReason::CapacityExceeded(Resource::References),
                )),
            },
        );
        assert_eq!(
            owner.will_status().deferred,
            Some((intent(9).id, WillDeferredReason::StoreRefusal))
        );
        let next = page(&mut owner, &intent(9));
        let (_, next_operation, _) = submitted(&next);
        assert!(
            next_operation > operation,
            "only_certain_refusal_allows_a_new_will_admission_operation"
        );
    }
    #[test]
    fn will_open_waits_for_known_installation_and_closed_transport_keeps_its_disposition() {
        let mut owner = world();
        let connection = ConnectionGeneration::new(30);
        let effects = apply(
            &mut owner,
            Input::OpenSessionWillAt {
                connection,
                client_id: bytes(b"new-sensor"),
                clean_start: false,
                expiry: SessionExpiry::OnDisconnect,
                now: 10,
                will: Some(intent(9).spec),
            },
        );
        assert!(
            effects
                .iter()
                .all(|(_, effect)| !matches!(effect, Effect::EmitFrame { .. })),
            "connack_before_durable_will_install"
        );
        let effect = effects[0].0;
        let operation = owner.lifecycle_dispatch().unwrap().0;
        assert!(matches!(
            owner.lifecycle_action(),
            Some(SessionAction::OpenWillAt { will: Some(_), .. })
        ));
        let _ = apply(&mut owner, Input::ConnectionClosed { connection });
        let known = apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                    SessionChange::Opened {
                        session: SessionId::new(3),
                        revision: Revision::new(1),
                        session_present: false,
                        expiry: SessionExpiry::OnDisconnect,
                        retired: None,
                        route_revision: Revision::new(0),
                        cancelled: Default::default(),
                    },
                ))),
            },
        );
        assert!(
            matches!(known.as_slice(), [(_, Effect::SessionChanged { .. })]),
            "closed_open_invented_will_close_or_success_connack"
        );
        assert!(
            owner
                .sessions
                .as_slice()
                .iter()
                .find(|s| s.id == SessionId::new(3))
                .unwrap()
                .will_managed
        );
        let end = apply(
            &mut owner,
            Input::EndSessionWill {
                connection: ConnectionKey {
                    session: SessionId::new(3),
                    generation: connection,
                },
                opened_by: Some(operation),
                expiry: SessionExpiry::OnDisconnect,
                observed_at: 11,
                publish: false,
            },
        );
        assert!(
            matches!(&end[0].1, Effect::Persist { command } if matches!(command.recipe.get(), Recipe::SessionLifecycle(crate::contract::session::SessionLifecycle { action: SessionAction::EndWill { publish: false, guard, .. }, .. }) if guard.opened_by == Some(operation))),
            "normal_close_lost_captured_disposition"
        );
    }
    #[test]
    fn an_old_will_close_cannot_cancel_a_new_attachment() {
        let mut owner = world();
        let session = &mut owner.sessions.as_mut_slice()[0];
        session.opened_by = Some(intent(10).id);
        session.connection = Some(ConnectionGeneration::new(30));
        session.will_managed = true;
        assert!(
            apply(
                &mut owner,
                Input::EndSessionWill {
                    connection: ConnectionKey {
                        session: SessionId::new(1),
                        generation: ConnectionGeneration::new(29)
                    },
                    opened_by: Some(intent(9).id),
                    expiry: SessionExpiry::OnDisconnect,
                    observed_at: 11,
                    publish: true
                }
            )
            .is_empty(),
            "old_close_affected_new_will_attachment"
        );
    }
    #[test]
    fn will_admission_reserves_all_effects_before_drawing_any_identity_or_losing_the_page() {
        let mut owner = world();
        let read = apply(&mut owner, Input::DiscoverWills)[0].0;
        let value = intent(9);
        let input = Input::WillPage {
            effect: read,
            items: BoundedVec::from_slice(&[value], 1).unwrap(),
            next: None,
        };
        let counters = owner.counters;
        let revision = owner.revision;
        let status = owner.will_status();
        let reserved = owner
            .outbox
            .reserve(NonZeroUsize::new(owner.outbox.free_slots() - 1).unwrap())
            .unwrap();
        let outcome = owner.step(input, TURN);
        let StepOutcome::NeedCapacity { input, need, .. } = outcome else {
            panic!("{outcome:?}");
        };
        assert_eq!(need.resource, Resource::EffectSlots);
        assert_eq!(
            owner.counters, counters,
            "will_reservation_refusal_burned_identity"
        );
        assert_eq!(owner.revision, revision);
        assert_eq!(
            owner.will_status(),
            status,
            "will_reservation_refusal_lost_pending_read"
        );
        owner.outbox.release(reserved).unwrap();
        let effects = apply(&mut owner, input);
        assert!(matches!(
            effects.as_slice(),
            [(_, Effect::Persist { .. }), (_, Effect::ArmDeadline { .. })]
        ));
    }
    #[test]
    fn will_receipt_hole_blocks_retirement_and_known_result_closes_its_identity_spaces() {
        use crate::contract::receipts::ReceiptCollectionReply;
        let mut owner = world();
        let registration = apply(&mut owner, Input::MaintainReceipts);
        let (registration_effect, Effect::CollectReceipts { collection }) = registration[0] else {
            panic!();
        };
        let _ = apply(
            &mut owner,
            Input::ReceiptsCollected {
                effect: registration_effect,
                collection,
                reply: ReceiptCollectionReply::Applied {
                    closure: collection.closure,
                    removed: 0,
                    retained: 0,
                    more: false,
                },
            },
        );
        let effects = page(&mut owner, &intent(9));
        let (effect, operation, facts) = submitted(&effects);
        let (_, span, holes) = owner.receipt_status().unwrap();
        assert_eq!(
            (span, holes),
            (1, 1),
            "unknown_will_result_was_prematurely_consumed"
        );
        owner.receipts.test_request_sweep();
        let maintenance = apply(&mut owner, Input::MaintainReceipts);
        let (maintenance_effect, Effect::CollectReceipts { collection }) = maintenance[0] else {
            panic!();
        };
        assert_eq!(
            collection.closure.through,
            OperationSeq::new(0),
            "closure_crossed_unknown_will"
        );
        let _ = apply(
            &mut owner,
            Input::ReceiptsCollected {
                effect: maintenance_effect,
                collection,
                reply: ReceiptCollectionReply::Applied {
                    closure: collection.closure,
                    removed: 0,
                    retained: 1,
                    more: false,
                },
            },
        );
        let _ = apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(facts)),
            },
        );
        assert_eq!(owner.receipt_status().unwrap().2, 0);
        owner.receipts.test_request_sweep();
        let maintenance = apply(&mut owner, Input::MaintainReceipts);
        let (_, Effect::CollectReceipts { collection }) = maintenance[0] else {
            panic!();
        };
        assert_eq!(collection.closure.through, operation.sequence());
        assert_eq!(
            collection.closure.publication_through,
            PublicationSeq::new(1),
            "will_publication_identity_not_protected_after_collection"
        );
        assert_eq!(
            collection.closure.payload_through,
            PayloadSeq::new(1),
            "will_payload_identity_not_protected_after_collection"
        );
    }
    struct ControlsOnly {
        taken: Vec<(EffectId, Effect)>,
        refuse_persist: bool,
    }
    impl EffectSink for ControlsOnly {
        fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
            if matches!(effect.effect(), Effect::EmitFrame { .. })
                || (self.refuse_persist && matches!(effect.effect(), Effect::Persist { .. }))
            {
                SubmitResult::NotSubmitted {
                    effect,
                    reason: crate::contract::effects::SubmitBlock::QueueFull,
                }
            } else {
                self.taken.push(effect.into_parts());
                SubmitResult::Accepted
            }
        }
    }
    fn block_network_head(owner: &mut Owner) -> EffectId {
        let id = EffectId {
            epoch: owner.epoch,
            owner: owner.id,
            sequence: draw(&mut owner.counters.effect, EffectSeq::checked_next).unwrap(),
        };
        let reservation = owner.outbox.reserve(NonZeroUsize::new(1).unwrap()).unwrap();
        owner
            .outbox
            .publish(
                reservation,
                OwnedEffect::new(
                    id,
                    Effect::EmitFrame {
                        connection: ConnectionKey {
                            session: SessionId::new(1),
                            generation: ConnectionGeneration::new(60),
                        },
                        frame: OutboundFrame::Pingresp,
                        publish_order: None,
                    },
                ),
            )
            .unwrap();
        id
    }
    #[test]
    fn will_query_admission_and_resolution_bypass_a_retained_network_head_without_reordering_their_deadline()
     {
        let mut owner = world();
        let blocked = block_network_head(&mut owner);
        let mut sink = ControlsOnly {
            taken: Vec::new(),
            refuse_persist: false,
        };
        assert!(matches!(
            owner.step(Input::DiscoverWills, TURN),
            StepOutcome::Applied { .. }
        ));
        let report = owner.dispatch(&mut sink);
        assert_eq!(
            report.accepted, 1,
            "blocked_network_pinned_durable_will_discovery"
        );
        let (read, Effect::LoadWills { .. }) = sink.taken.pop().unwrap() else {
            panic!();
        };
        assert!(matches!(
            owner.step(
                Input::WillPage {
                    effect: read,
                    items: BoundedVec::from_slice(&[intent(9)], 1).unwrap(),
                    next: None
                },
                TURN
            ),
            StepOutcome::Applied { .. }
        ));
        sink.refuse_persist = true;
        let report = owner.dispatch(&mut sink);
        assert_eq!(report.accepted, 0);
        assert!(
            sink.taken.is_empty(),
            "will_deadline_armed_before_persist_acceptance"
        );
        sink.refuse_persist = false;
        let report = owner.dispatch(&mut sink);
        assert_eq!(report.accepted, 2, "blocked_network_pinned_will_admission");
        assert!(matches!(
            sink.taken.as_slice(),
            [(_, Effect::Persist { .. }), (_, Effect::ArmDeadline { .. })]
        ));
        let (operation, deadline) = owner.will_dispatch().unwrap();
        sink.taken.clear();
        assert!(matches!(
            owner.step(
                Input::Deadline {
                    token: deadline,
                    now: LogicalTime::new(100)
                },
                TURN
            ),
            StepOutcome::Applied { .. }
        ));
        let report = owner.dispatch(&mut sink);
        assert_eq!(
            report.accepted, 2,
            "blocked_network_pinned_unknown_will_resolution"
        );
        assert!(
            matches!(sink.taken.as_slice(), [(_, Effect::ResolveAndFence { operation: resolved }), (_, Effect::ArmDeadline { .. })] if *resolved == operation)
        );
        assert_eq!(owner.outbox.len(), 1);
        let mut all = Taking(Vec::new());
        owner.dispatch(&mut all);
        assert_eq!(
            all.0[0].0, blocked,
            "will_control_scan_lost_or_replaced_retained_network_effect"
        );
    }
}
