//! Receipted session incarnations; transport loss alone is not business completion.

use super::*;
use crate::contract::session::{
    SessionAction, SessionChange, SessionEndReason, SessionExpiry, SessionGuard, SessionLifecycle,
};

/// Bounded facts about owner-held state; no transport/cache authority is inferred.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SessionStatus {
    pub sessions: usize,
    pub bindings: usize,
    pub retired_flights: usize,
    pub inbound: usize,
    /// Durable delivery metadata plus volatile delivery permissions.
    pub outgoing: usize,
    pub closed_generations: usize,
    pub lifecycle_pending: bool,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) struct Flight {
    pub(super) command: SessionLifecycle,
    pub(super) client_id: SharedBytes,
    pub(super) connection: ConnectionGeneration,
    pub(super) connection_closed: bool,
    pub(super) in_flight: InFlight,
}

#[derive(Debug)]
pub(super) enum Change {
    Submit(Flight),
    Await {
        phase: CommandPhase,
        resolution: Option<(DeadlineToken, EffectId)>,
    },
    TransportClosed,
    Applied {
        finished: Finished,
        change: SessionChange,
        followup: Option<Flight>,
    },
    Refused {
        finished: Finished,
        followup: Option<Flight>,
    },
}

fn changed(change: Change) -> super::Change {
    super::Change::Session(change)
}
fn waiting() -> Stop {
    Stop::Need(CapacityNeed {
        resource: Resource::ResultSlots,
        amount: 1,
        wake: WakeCondition::ResultDelivered,
    })
}

impl Owner {
    /// The one accepted opening request, including a transport already closed.
    /// This is an observation for bounded adapter follow-up, not a session grant.
    #[must_use]
    pub fn opening_connection(&self) -> Option<ConnectionGeneration> {
        self.lifecycle.as_ref().and_then(|f| {
            matches!(f.command.action, SessionAction::Open { .. }).then_some(f.connection)
        })
    }
    #[must_use]
    pub fn lifecycle_dispatch(&self) -> Option<(OperationId, DeadlineToken)> {
        self.lifecycle
            .as_ref()
            .map(|f| (f.in_flight.operation, f.in_flight.deadline))
    }
    #[must_use]
    pub fn lifecycle_operation_is_live(&self, operation: OperationId) -> bool {
        self.lifecycle
            .as_ref()
            .is_some_and(|f| f.in_flight.operation == operation)
    }
    #[must_use]
    pub fn session_status(&self) -> SessionStatus {
        SessionStatus {
            sessions: self.sessions.len(),
            bindings: self
                .sessions
                .as_slice()
                .iter()
                .filter(|s| s.connection.is_some())
                .count(),
            retired_flights: self
                .slots
                .as_slice()
                .iter()
                .filter(|s| s.retired && s.in_flight().is_some())
                .count()
                + self.pubsub.retired_flights(),
            inbound: self.slots.len(),
            outgoing: self.pubsub.deliveries.len() + self.pubsub.volatile.len(),
            closed_generations: self.closed.len(),
            lifecycle_pending: self.lifecycle.is_some(),
        }
    }
    pub(super) fn session_work_bound(&self, input: &Input) -> u64 {
        if !matches!(input, Input::OpenSession { .. } | Input::EndSession { .. })
            && !self.lifecycle_enabled
        {
            return 0;
        }
        capacity_of(self.sessions.capacity())
            .saturating_mul(4)
            .saturating_add(capacity_of(self.slots.capacity()).saturating_mul(4))
            .saturating_add(capacity_of(self.routes.capacity()).saturating_mul(2))
            .saturating_add(capacity_of(self.pubsub.deliveries.capacity()).saturating_mul(4))
            .saturating_add(capacity_of(self.pubsub.volatile.capacity()).saturating_mul(2))
    }
    fn session_flight(
        &self,
        action: SessionAction,
        client_id: SharedBytes,
        connection: ConnectionGeneration,
        connection_closed: bool,
        draft: &mut Draft,
    ) -> Result<Flight, Stop> {
        let operation = self.operation(draft)?;
        let command = SessionLifecycle { operation, action };
        let in_flight = self.plan_submission(
            operation,
            Recipe::SessionLifecycle(command.clone()),
            None,
            draft,
        )?;
        Ok(Flight {
            command,
            client_id,
            connection,
            connection_closed,
            in_flight,
        })
    }
    pub(super) fn plan_session_open(
        &self,
        connection: ConnectionGeneration,
        client_id: &SharedBytes,
        clean_start: bool,
        expiry: SessionExpiry,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        if self.closed.iter().any(|g| *g == connection) {
            return Ok(super::Change::Nothing);
        }
        if self.lifecycle.is_some() || self.pubsub.control_pending() {
            return Err(waiting());
        }
        if self.session_of(connection).is_some() {
            return Self::plan_close(
                connection,
                self.session_of(connection),
                CloseReason::ProtocolError,
                draft,
            );
        }
        let known = self
            .sessions
            .as_slice()
            .iter()
            .find(|s| s.client_id == *client_id);
        if let Some(previous) = known.and_then(|s| s.connection) {
            draft.close(previous, CloseReason::SessionTakenOver)?;
        }
        let expected = known.map(|s| SessionGuard {
            session: s.id,
            opened_by: s.opened_by,
        });
        let flight = self.session_flight(
            SessionAction::Open {
                client_id: client_id.clone(),
                expected,
                clean_start,
                expiry,
            },
            client_id.clone(),
            connection,
            false,
            draft,
        )?;
        Ok(changed(Change::Submit(flight)))
    }
    pub(super) fn plan_session_end(
        &self,
        connection: ConnectionKey,
        opened_by: Option<OperationId>,
        reason: SessionEndReason,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        let Some(session) = self
            .sessions
            .as_slice()
            .iter()
            .find(|s| s.id == connection.session)
        else {
            return Ok(super::Change::Nothing);
        };
        if session.opened_by != opened_by
            || session
                .connection
                .is_some_and(|g| g != connection.generation)
        {
            return Ok(super::Change::Nothing);
        }
        if self.lifecycle.is_some() || self.pubsub.control_pending() {
            return Err(waiting());
        }
        draft.end(connection.generation)?;
        let flight = self.session_flight(
            SessionAction::Close {
                guard: SessionGuard {
                    session: session.id,
                    opened_by,
                },
                reason,
            },
            session.client_id.clone(),
            connection.generation,
            true,
            draft,
        )?;
        Ok(changed(Change::Submit(flight)))
    }
    pub(super) fn plan_session_transport_closed(
        &self,
        connection: ConnectionGeneration,
        draft: &mut Draft,
    ) -> Result<Option<super::Change>, Stop> {
        if self.lifecycle.as_ref().is_some_and(|f| {
            f.connection == connection && matches!(f.command.action, SessionAction::Open { .. })
        }) {
            draft.end(connection)?;
            return Ok(Some(changed(Change::TransportClosed)));
        }
        let Some(session) = self
            .sessions
            .as_slice()
            .iter()
            .find(|s| s.connection == Some(connection))
        else {
            return Ok(None);
        };
        if session.expiry != SessionExpiry::OnDisconnect {
            return Ok(None);
        }
        if let Some(flight) = &self.lifecycle {
            let covered = match &flight.command.action {
                SessionAction::Open { expected, .. } => expected
                    .is_some_and(|g| g.session == session.id && g.opened_by == session.opened_by),
                SessionAction::Close { guard, .. } => {
                    guard.session == session.id && guard.opened_by == session.opened_by
                }
            };
            if covered {
                return Ok(None);
            }
            return Err(waiting());
        }
        self.plan_session_end(
            ConnectionKey {
                session: session.id,
                generation: connection,
            },
            session.opened_by,
            SessionEndReason::TransportLost,
            draft,
        )
        .map(Some)
    }
    fn followup_for_closed_open(
        &self,
        flight: &Flight,
        change: SessionChange,
        draft: &mut Draft,
    ) -> Result<Option<Flight>, Stop> {
        let SessionChange::Opened {
            session,
            expiry: SessionExpiry::OnDisconnect,
            ..
        } = change
        else {
            return Ok(None);
        };
        if !flight.connection_closed && !self.closed.iter().any(|g| *g == flight.connection) {
            return Ok(None);
        }
        if !self.receipts.control_available() {
            return Ok(None);
        }
        self.session_flight(
            SessionAction::Close {
                guard: SessionGuard {
                    session,
                    opened_by: Some(flight.in_flight.operation),
                },
                reason: SessionEndReason::TransportLost,
            },
            flight.client_id.clone(),
            flight.connection,
            true,
            draft,
        )
        .map(Some)
    }
    fn followup_for_refused_takeover(
        &self,
        flight: &Flight,
        draft: &mut Draft,
    ) -> Result<Option<Flight>, Stop> {
        let SessionAction::Open {
            expected: Some(guard),
            ..
        } = flight.command.action
        else {
            return Ok(None);
        };
        let Some(previous) = self.sessions.as_slice().iter().find(|s| {
            s.id == guard.session
                && s.opened_by == guard.opened_by
                && s.expiry == SessionExpiry::OnDisconnect
                && s.connection.is_none()
        }) else {
            return Ok(None);
        };
        // The earlier takeover closed this ephemeral transport before Open.
        // A refused Open cannot make that old incarnation persistent again.
        // Keep the result unapplied if even the reserved close lane is full.
        if !self.receipts.control_available() {
            return Err(waiting());
        }
        self.session_flight(
            SessionAction::Close {
                guard,
                reason: SessionEndReason::TransportLost,
            },
            previous.client_id.clone(),
            flight.connection,
            true,
            draft,
        )
        .map(Some)
    }
    pub(super) fn plan_session_result(
        &self,
        effect: EffectId,
        operation: OperationId,
        reply: &StoreReply,
        draft: &mut Draft,
    ) -> Result<Option<super::Change>, Stop> {
        let Some(flight) = self
            .lifecycle
            .as_ref()
            .filter(|f| f.in_flight.operation == operation)
        else {
            return Ok(None);
        };
        if !flight.in_flight.answers.names(effect) {
            return Ok(Some(super::Change::Nothing));
        }
        let outcome = match reply {
            StoreReply::Outcome(o) => *o,
            StoreReply::Violation(StoreViolation::RecipeMismatch) => {
                return Err(Stop::Fatal(CoreFault::RecipeMismatch { operation }));
            }
        };
        let verdict = flight
            .in_flight
            .phase
            .on(CommandEvent::StoreResult(OutcomeClass::of(&outcome)));
        match (verdict, outcome) {
            (
                Verdict::Transition(CommandPhase::Committed),
                StoreOutcome::Committed(CommitFacts::SessionLifecycle(change)),
            ) => {
                if !self.valid_session_change(flight, change) {
                    return Err(Stop::Fatal(CoreFault::ContradictoryOutcome { operation }));
                }
                draft.receipt_consumed = Some(operation);
                let finished = Finished {
                    operation,
                    answers: flight.in_flight.answers,
                    phase: CommandPhase::Consumed,
                    facts: Some(CommitFacts::SessionLifecycle(change)),
                };
                draft.emit(Effect::SessionChanged {
                    operation,
                    connection: Some(flight.connection),
                    client_id: flight.client_id.clone(),
                    change,
                })?;
                let followup = self.followup_for_closed_open(flight, change, draft)?;
                if let SessionChange::Opened {
                    session,
                    session_present,
                    ..
                } = change
                    && !flight.connection_closed
                    && !self.closed.iter().any(|g| *g == flight.connection)
                {
                    draft.frame(
                        ConnectionKey {
                            session,
                            generation: flight.connection,
                        },
                        OutboundFrame::Connack { session_present },
                    )?;
                }
                Ok(Some(changed(Change::Applied {
                    finished,
                    change,
                    followup,
                })))
            }
            (
                Verdict::Transition(CommandPhase::RejectedCertain),
                StoreOutcome::RejectedCertain(reason),
            ) => {
                draft.receipt_consumed = Some(operation);
                let finished = Finished {
                    operation,
                    answers: flight.in_flight.answers,
                    phase: CommandPhase::RejectedCertain,
                    facts: None,
                };
                if matches!(flight.command.action, SessionAction::Open { .. })
                    && !flight.connection_closed
                {
                    draft.emit(Effect::SessionRefused {
                        connection: flight.connection,
                        reason: if reason == RejectReason::SessionCapacityExceeded {
                            0x97
                        } else {
                            0x80
                        },
                    })?;
                }
                let followup = self.followup_for_refused_takeover(flight, draft)?;
                Ok(Some(changed(Change::Refused { finished, followup })))
            }
            (Verdict::Transition(CommandPhase::Indeterminate), _) => {
                Ok(Some(changed(Change::Await {
                    phase: CommandPhase::Indeterminate,
                    resolution: None,
                })))
            }
            _ => Err(IMPOSSIBLE),
        }
    }
    fn valid_session_change(&self, flight: &Flight, change: SessionChange) -> bool {
        let Some(current_revision) = self.route_revision else {
            return false;
        };
        match (&flight.command.action, change) {
            (
                SessionAction::Open {
                    expected,
                    clean_start,
                    expiry,
                    ..
                },
                SessionChange::Opened {
                    session,
                    revision,
                    session_present,
                    expiry: reported,
                    retired,
                    route_revision,
                    ..
                },
            ) => {
                let old = expected.and_then(|guard| {
                    self.sessions
                        .as_slice()
                        .iter()
                        .find(|s| s.id == guard.session && s.opened_by == guard.opened_by)
                });
                let resumes =
                    old.is_some_and(|s| !*clean_start && s.expiry == SessionExpiry::Never);
                (expected.is_none() || old.is_some())
                    && reported == *expiry
                    && session_present == resumes
                    && route_revision >= current_revision
                    && if resumes {
                        old.is_some_and(|s| s.id == session && s.revision == revision)
                            && retired.is_none()
                    } else {
                        retired == old.map(|s| s.id)
                            && (retired.is_some() || self.sessions.len() < self.sessions.capacity())
                            && revision == Revision::new(1)
                            && session.get() > 0
                            && !self.sessions.as_slice().iter().any(|s| s.id == session)
                    }
            }
            (
                SessionAction::Close { guard, .. },
                SessionChange::Closed {
                    session,
                    route_revision,
                    ..
                },
            ) => {
                session == guard.session
                    && route_revision >= current_revision
                    && self
                        .sessions
                        .as_slice()
                        .iter()
                        .any(|s| s.id == guard.session && s.opened_by == guard.opened_by)
            }
            _ => false,
        }
    }
    pub(super) fn plan_session_deadline(
        &self,
        token: DeadlineToken,
        draft: &mut Draft,
    ) -> Result<Option<super::Change>, Stop> {
        let Some(flight) = self
            .lifecycle
            .as_ref()
            .filter(|f| f.in_flight.deadline == token)
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
    pub(super) fn apply_session(&mut self, change: Change) -> Result<(), CoreFault> {
        match change {
            Change::Submit(flight) => {
                self.lifecycle_enabled = true;
                self.lifecycle = Some(flight);
            }
            Change::TransportClosed => {
                self.lifecycle
                    .as_mut()
                    .ok_or(CoreFault::StalePlan)?
                    .connection_closed = true;
            }
            Change::Await { phase, resolution } => {
                let flight = self.lifecycle.as_mut().ok_or(CoreFault::StalePlan)?;
                flight.in_flight.phase = phase;
                if let Some((deadline, effect)) = resolution {
                    flight.in_flight.deadline = deadline;
                    flight.in_flight.answers = flight.in_flight.answers.with(effect);
                }
            }
            Change::Applied {
                finished,
                change,
                followup,
            } => {
                let flight = self.lifecycle.take().ok_or(CoreFault::StalePlan)?;
                match change {
                    SessionChange::Opened {
                        session,
                        revision,
                        expiry,
                        retired,
                        route_revision,
                        ..
                    } => {
                        if let Some(old) = retired {
                            self.retire_session(old)?;
                        }
                        let connection = (!flight.connection_closed
                            && !self.closed.iter().any(|g| *g == flight.connection))
                        .then_some(flight.connection);
                        let value = Session {
                            id: session,
                            client_id: flight.client_id,
                            revision,
                            connection,
                            expiry,
                            opened_by: Some(finished.operation),
                        };
                        if let Some(index) = self
                            .sessions
                            .as_slice()
                            .iter()
                            .position(|s| s.id == session)
                        {
                            self.sessions.as_mut_slice()[index] = value;
                        } else {
                            self.sessions
                                .try_push(value)
                                .map_err(|_| CoreFault::StalePlan)?;
                        }
                        self.route_revision = Some(route_revision);
                    }
                    SessionChange::Closed {
                        session,
                        route_revision,
                        ..
                    } => {
                        self.retire_session(session)?;
                        self.route_revision = Some(route_revision);
                    }
                }
                remember(&mut self.finished, finished)?;
                self.lifecycle = followup;
            }
            Change::Refused { finished, followup } => {
                self.lifecycle.take().ok_or(CoreFault::StalePlan)?;
                remember(&mut self.finished, finished)?;
                self.lifecycle = followup;
            }
        }
        Ok(())
    }
    fn retire_session(&mut self, session: SessionId) -> Result<(), CoreFault> {
        if let Some(index) = self
            .sessions
            .as_slice()
            .iter()
            .position(|s| s.id == session)
        {
            if let Some(generation) = self.sessions.as_slice()[index].connection {
                self.pubsub.close(generation);
            }
            let last = self.sessions.len() - 1;
            self.sessions.as_mut_slice().swap(index, last);
            let _ = self.sessions.pop();
        }
        let mut index = 0;
        while index < self.routes.len() {
            if self.routes.as_slice()[index].target == session {
                self.routes.as_mut_slice()[index..].rotate_left(1);
                let _ = self.routes.pop();
            } else {
                index += 1;
            }
        }
        let mut index = 0;
        while index < self.slots.len() {
            if self.slots.as_slice()[index].session != session {
                index += 1;
                continue;
            }
            if self.slots.as_slice()[index].in_flight().is_some() {
                self.slots.as_mut_slice()[index].retired = true;
                index += 1;
                continue;
            }
            let held = self.slots.as_slice()[index];
            if self.leases.release(held.lease) != Ok(ReleaseOutcome::Released) {
                return Err(CoreFault::StalePlan);
            }
            if let Some(done) = held.admission {
                remember(&mut self.finished, done)?;
            }
            let last = self.slots.len() - 1;
            self.slots.as_mut_slice().swap(index, last);
            let _ = self.slots.pop();
        }
        self.pubsub.retire_session(session);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::contract::effects::{OwnerAuthority, SubmitResult};
    use crate::contract::opening::{
        EpochOpened, EpochOpening, OpeningOutcome, OpeningReply, StartupNonce,
    };
    use crate::contract::session::{
        SessionAction, SessionCancellations, SessionChange, SessionExpiry, SessionLifecycle,
    };
    use alloc::vec::Vec;

    const TURN: TurnGrant = TurnGrant {
        work_limit: WorkUnits::new(u64::MAX),
    };
    struct Taking(Vec<OwnedEffect>);
    impl EffectSink for Taking {
        fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
            self.0.push(effect);
            SubmitResult::Accepted
        }
    }
    fn bound(n: u32) -> NonZeroU32 {
        NonZeroU32::new(n).unwrap()
    }
    fn bytes(value: &[u8]) -> SharedBytes {
        SharedBytes::copy_from(value, 128).unwrap()
    }
    fn drain(owner: &mut Owner) -> Vec<(EffectId, Effect)> {
        let mut sink = Taking(Vec::new());
        owner.dispatch(&mut sink);
        sink.0.into_iter().map(OwnedEffect::into_parts).collect()
    }
    fn apply(owner: &mut Owner, input: Input) -> Vec<(EffectId, Effect)> {
        let result = owner.step(input, TURN);
        assert!(
            matches!(result, StepOutcome::Applied { .. }),
            "session_lifecycle_not_applied: {result:?}"
        );
        drain(owner)
    }
    fn world() -> Owner {
        let incarnation = StoreIncarnation::new(1);
        let epoch = RunEpoch::new(2);
        let mut opening = EpochOpening::new(incarnation, StartupNonce::new([7; 16]));
        let command = opening.submit(RunEpoch::new(1)).unwrap();
        let _ = opening
            .observe(OpeningReply {
                command,
                outcome: OpeningOutcome::Committed(EpochOpened { incarnation, epoch }),
            })
            .unwrap();
        let mut authority =
            OwnerAuthority::from_grant(opening.hand_over().unwrap(), bound(1)).unwrap();
        let outbox = authority
            .create_outbox(
                LogicalOwnerId::new(4),
                NonZeroUsize::new(32).unwrap(),
                bound(8),
            )
            .unwrap();
        let mut owner = Owner::new(
            OwnerConfig {
                max_sessions: bound(1),
                max_routes: bound(4),
                max_exchanges: bound(8),
                account: AccountId::new(1),
                payload_bytes: PayloadBytes::new(1024),
                resolution_delay: LogicalDuration::new(1),
                retained_operations: bound(16),
                remembered_connections: bound(4),
            },
            outbox,
        )
        .unwrap();
        let facts = [
            RecoveredFact::Epoch { incarnation, epoch },
            RecoveredFact::Limits {
                max_targets_per_admission: 2,
                max_topic_bytes: 128,
                max_payload_bytes: PayloadBytes::new(128),
            },
            RecoveredFact::RouteTable {
                revision: Revision::new(0),
            },
        ];
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
    fn known(
        owner: &mut Owner,
        effects: &[(EffectId, Effect)],
        change: SessionChange,
    ) -> (OperationId, Vec<(EffectId, Effect)>) {
        let (effect, command) = effects
            .iter()
            .find_map(|(effect, item)| match item {
                Effect::Persist { command } => Some((*effect, command)),
                _ => None,
            })
            .expect("a canonical session command");
        let operation = command.recipe.operation();
        (
            operation,
            apply(
                owner,
                Input::StoreResult {
                    effect,
                    operation,
                    reply: StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::SessionLifecycle(change),
                    )),
                },
            ),
        )
    }
    fn opened(session: u64, expiry: SessionExpiry) -> SessionChange {
        SessionChange::Opened {
            session: SessionId::new(session),
            revision: Revision::new(1),
            session_present: false,
            expiry,
            retired: None,
            route_revision: Revision::new(0),
            cancelled: SessionCancellations::default(),
        }
    }
    fn submit(
        owner: &mut Owner,
        connection: u64,
        client_id: &[u8],
        expiry: SessionExpiry,
    ) -> Vec<(EffectId, Effect)> {
        apply(
            owner,
            Input::OpenSession {
                connection: ConnectionGeneration::new(connection),
                client_id: bytes(client_id),
                clean_start: false,
                expiry,
            },
        )
    }
    #[test]
    fn refused_takeover_still_closes_the_disconnected_ephemeral_incarnation() {
        let mut owner = world();
        let first = submit(&mut owner, 1, b"ephemeral", SessionExpiry::OnDisconnect);
        let (opened_by, _) = known(&mut owner, &first, opened(1, SessionExpiry::OnDisconnect));
        let takeover = submit(&mut owner, 2, b"ephemeral", SessionExpiry::Never);
        assert!(takeover.iter().any(|(_, e)| matches!(e, Effect::CloseConnection { connection, .. } if *connection == ConnectionGeneration::new(1))));
        let flight = owner.lifecycle.clone().unwrap();
        let effects = apply(
            &mut owner,
            Input::StoreResult {
                effect: flight.in_flight.answers.persist,
                operation: flight.command.operation,
                reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(RejectReason::Fenced)),
            },
        );
        assert!(effects.iter().any(|(_, e)| matches!(e, Effect::SessionRefused { connection, .. } if *connection == ConnectionGeneration::new(2))));
        assert!(effects.iter().any(|(_, e)| matches!(e, Effect::Persist { command } if matches!(command.recipe.get(), Recipe::SessionLifecycle(SessionLifecycle { action: SessionAction::Close { guard, .. }, .. }) if guard.session == SessionId::new(1) && guard.opened_by == Some(opened_by)))), "disconnected_ephemeral_takeover_refusal_leaks_capacity");
    }
    #[test]
    fn impossible_committed_creation_at_full_capacity_is_refused_before_mutation() {
        let mut owner = world();
        let first = submit(&mut owner, 1, b"persistent", SessionExpiry::Never);
        known(&mut owner, &first, opened(1, SessionExpiry::Never));
        submit(&mut owner, 2, b"other", SessionExpiry::Never);
        let flight = owner.lifecycle.clone().unwrap();
        let previous = owner.sessions.as_slice().to_vec();
        let input = Input::StoreResult {
            effect: flight.in_flight.answers.persist,
            operation: flight.command.operation,
            reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                opened(2, SessionExpiry::Never),
            ))),
        };
        assert!(
            matches!(
                owner.step(input, TURN),
                StepOutcome::Fatal(CoreFault::ContradictoryOutcome { .. })
            ),
            "committed_creation_exceeds_owner_capacity"
        );
        assert_eq!(owner.sessions.as_slice(), previous.as_slice());
        assert_eq!(owner.lifecycle, Some(flight));
        assert!(drain(&mut owner).is_empty());
    }
    #[test]
    fn retiring_a_session_preserves_the_order_of_other_targets_routes() {
        let mut owner = world();
        for (topic, target) in [
            (b"a".as_slice(), 1),
            (b"b".as_slice(), 2),
            (b"c".as_slice(), 3),
        ] {
            owner
                .routes
                .try_push(Route {
                    topic: bytes(topic),
                    target: SessionId::new(target),
                    target_revision: Revision::new(1),
                    max_qos: Qos::AtLeastOnce,
                })
                .unwrap();
        }
        owner.retire_session(SessionId::new(1)).unwrap();
        assert_eq!(owner.routes.as_slice()[0].topic, bytes(b"b"));
    }
    #[test]
    fn a_new_ephemeral_session_ends_and_releases_the_owner_slot() {
        let mut owner = world();
        let connection = ConnectionGeneration::new(1);
        let opened = apply(
            &mut owner,
            Input::OpenSession {
                connection,
                client_id: bytes(b"ephemeral"),
                clean_start: false,
                expiry: SessionExpiry::OnDisconnect,
            },
        );
        assert_eq!(
            owner.sessions.len(),
            0,
            "creation is not known at submission"
        );
        let (_, frames) = known(
            &mut owner,
            &opened,
            SessionChange::Opened {
                session: SessionId::new(1),
                revision: Revision::new(1),
                session_present: false,
                expiry: SessionExpiry::OnDisconnect,
                retired: None,
                route_revision: Revision::new(0),
                cancelled: SessionCancellations::default(),
            },
        );
        assert!(
            frames.iter().any(|(_, effect)| matches!(
                effect,
                Effect::EmitFrame {
                    frame: OutboundFrame::Connack {
                        session_present: false
                    },
                    ..
                }
            )),
            "known durable creation must precede CONNACK"
        );
        let close = apply(&mut owner, Input::ConnectionClosed { connection });
        assert_eq!(
            owner.sessions.len(),
            1,
            "closure is not known at submission"
        );
        known(
            &mut owner,
            &close,
            SessionChange::Closed {
                session: SessionId::new(1),
                route_revision: Revision::new(0),
                cancelled: SessionCancellations::default(),
            },
        );
        assert!(
            owner.sessions.is_empty(),
            "terminal_session_owner_history_retained"
        );
        assert!(owner.slots.is_empty());
    }
}
