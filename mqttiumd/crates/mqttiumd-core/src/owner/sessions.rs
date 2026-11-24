//! Receipted session incarnations; transport loss alone is not business completion.

use super::*;
use crate::contract::session::{
    SessionAction, SessionChange, SessionDetachment, SessionEndReason, SessionExpiry, SessionGuard,
    SessionLifecycle,
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
    pub(super) fn session_metadata_valid(
        &self,
        expiry: SessionExpiry,
        detached: Option<SessionDetachment>,
    ) -> bool {
        if matches!(expiry, SessionExpiry::After(0 | u32::MAX)) {
            return false;
        }
        detached.is_none_or(|d| {
            d.operation.incarnation() == self.incarnation
                && d.operation.epoch().get() > 0
                && d.operation.epoch() <= self.epoch
                && d.valid_for(expiry)
        })
    }
    /// The one accepted opening request, including a transport already closed.
    /// This is an observation for bounded adapter follow-up, not a session grant.
    #[must_use]
    pub fn opening_connection(&self) -> Option<ConnectionGeneration> {
        self.lifecycle.as_ref().and_then(|f| {
            matches!(
                f.command.action,
                SessionAction::Open { .. }
                    | SessionAction::OpenAt { .. }
                    | SessionAction::OpenWillAt { .. }
            )
            .then_some(f.connection)
        })
    }
    #[must_use]
    pub fn lifecycle_dispatch(&self) -> Option<(OperationId, DeadlineToken)> {
        self.lifecycle
            .as_ref()
            .map(|f| (f.in_flight.operation, f.in_flight.deadline))
    }
    /// Observe the current correlated lifecycle request without granting
    /// authority to apply a result or fabricate a retry identity.
    #[must_use]
    pub fn lifecycle_action(&self) -> Option<&SessionAction> {
        self.lifecycle.as_ref().map(|flight| &flight.command.action)
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
            bindings: self.session_by_conn.len(),
            retired_flights: self
                .slots
                .as_slice()
                .iter()
                .filter(|s| s.retired && s.in_flight().is_some())
                .count()
                + self.retired_flights(),
            inbound: self.slots.len(),
            outgoing: self.deliveries.len() + self.volatile.len(),
            closed_generations: self.closed.len(),
            lifecycle_pending: self.lifecycle.is_some(),
        }
    }
    pub(super) fn session_work_bound(&self, input: &Input) -> u64 {
        let is_session_input = matches!(
            input,
            Input::OpenSession { .. }
                | Input::OpenSessionAt { .. }
                | Input::OpenSessionWillAt { .. }
                | Input::EndSessionWill { .. }
                | Input::EndSession { .. }
                | Input::DetachSession { .. }
                | Input::ExpireSession { .. }
        );
        if !is_session_input && !self.lifecycle_enabled {
            return 0;
        }
        let base = capacity_of(self.sessions.capacity()).min(32);
        if is_session_input {
            base.saturating_mul(2)
        } else {
            base
        }
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
    #[expect(
        clippy::too_many_arguments,
        reason = "one shared planner retains legacy opening semantics and the explicit Will-aware variant"
    )]
    pub(super) fn plan_session_open(
        &self,
        connection: ConnectionGeneration,
        client_id: &SharedBytes,
        clean_start: bool,
        expiry: SessionExpiry,
        now: Option<u64>,
        will: Option<Option<&crate::contract::will::WillSpec>>,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        if self.is_closed(connection) {
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
            .session_by_client
            .get(client_id)
            .and_then(|&idx| self.sessions.as_slice().get(idx));
        if now.is_none() && known.is_some_and(|s| matches!(s.expiry, SessionExpiry::After(_))) {
            // An old untimed interface cannot turn an unobserved expiry into
            // a resume, even when the requested replacement policy is Never.
            return Err(IMPOSSIBLE);
        }
        if let Some(previous) = known.and_then(|s| s.connection) {
            draft.close(previous, CloseReason::SessionTakenOver)?;
        }
        let expected = known.map(|s| SessionGuard {
            session: s.id,
            opened_by: s.opened_by,
        });
        let action = match (now, will) {
            (Some(now), Some(will)) => SessionAction::OpenWillAt {
                client_id: client_id.clone(),
                expected,
                clean_start,
                expiry,
                now,
                will: will.cloned(),
            },
            (Some(now), None) => SessionAction::OpenAt {
                client_id: client_id.clone(),
                expected,
                clean_start,
                expiry,
                now,
            },
            (None, None) => SessionAction::Open {
                client_id: client_id.clone(),
                expected,
                clean_start,
                expiry,
            },
            (None, Some(_)) => return Err(IMPOSSIBLE),
        };
        let flight = self.session_flight(action, client_id.clone(), connection, false, draft)?;
        Ok(changed(Change::Submit(flight)))
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "a CONNECT supplies policy, observation and its bounded optional Will"
    )]
    pub(super) fn plan_session_will_open(
        &self,
        connection: ConnectionGeneration,
        client_id: &SharedBytes,
        clean_start: bool,
        expiry: SessionExpiry,
        now: u64,
        will: Option<&crate::contract::will::WillSpec>,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        self.plan_session_open(
            connection,
            client_id,
            clean_start,
            expiry,
            Some(now),
            Some(will),
            draft,
        )
    }

    pub(super) fn plan_session_will_end(
        &self,
        connection: ConnectionKey,
        opened_by: Option<OperationId>,
        expiry: SessionExpiry,
        observed_at: u64,
        publish: bool,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        let Some(session) = self
            .session_by_id
            .get(&connection.session)
            .and_then(|&idx| self.sessions.as_slice().get(idx))
        else {
            return Ok(super::Change::Nothing);
        };
        if opened_by.is_none()
            || session.opened_by != opened_by
            || session.detached.is_some()
            || session
                .connection
                .is_some_and(|g| g != connection.generation)
        {
            return Ok(super::Change::Nothing);
        }
        if expiry.deadline(observed_at).is_none() {
            return Err(IMPOSSIBLE);
        }
        if self.lifecycle.is_some() || self.pubsub.control_pending() {
            return Err(waiting());
        }
        draft.end(connection.generation)?;
        let flight = self.session_flight(
            SessionAction::EndWill {
                guard: SessionGuard {
                    session: session.id,
                    opened_by,
                },
                expiry,
                observed_at,
                publish,
            },
            session.client_id.clone(),
            connection.generation,
            true,
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
            .session_by_id
            .get(&connection.session)
            .and_then(|&idx| self.sessions.as_slice().get(idx))
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
    pub(super) fn plan_session_detach(
        &self,
        connection: ConnectionKey,
        opened_by: Option<OperationId>,
        expiry: SessionExpiry,
        observed_at: u64,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        let Some(session) = self
            .session_by_id
            .get(&connection.session)
            .and_then(|&idx| self.sessions.as_slice().get(idx))
        else {
            return Ok(super::Change::Nothing);
        };
        if opened_by.is_none()
            || session.opened_by != opened_by
            || session.detached.is_some()
            || session
                .connection
                .is_some_and(|g| g != connection.generation)
        {
            return Ok(super::Change::Nothing);
        }
        if expiry == SessionExpiry::OnDisconnect || expiry.deadline(observed_at).is_none() {
            return Err(IMPOSSIBLE);
        }
        if self.lifecycle.is_some() || self.pubsub.control_pending() {
            return Err(waiting());
        }
        draft.end(connection.generation)?;
        let flight = self.session_flight(
            SessionAction::Detach {
                guard: SessionGuard {
                    session: session.id,
                    opened_by,
                },
                expiry,
                observed_at,
            },
            session.client_id.clone(),
            connection.generation,
            true,
            draft,
        )?;
        Ok(changed(Change::Submit(flight)))
    }
    pub(super) fn plan_session_expire(
        &self,
        guard: SessionGuard,
        detached: SessionDetachment,
        now: u64,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        let Some(session) = self
            .session_by_id
            .get(&guard.session)
            .and_then(|&idx| self.sessions.as_slice().get(idx))
        else {
            return Ok(super::Change::Nothing);
        };
        if session.opened_by != guard.opened_by
            || session.connection.is_some()
            || session.detached != Some(detached)
            || !detached.deadline.is_some_and(|deadline| now >= deadline)
        {
            return Ok(super::Change::Nothing);
        }
        if self.lifecycle.is_some() || self.pubsub.control_pending() {
            return Err(waiting());
        }
        // This transportless flight never emits a frame/close or exposes the
        // placeholder generation; its SessionChanged effect carries None.
        let flight = self.session_flight(
            SessionAction::Expire {
                guard,
                detached,
                now,
            },
            session.client_id.clone(),
            ConnectionGeneration::new(0),
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
            f.connection == connection
                && matches!(
                    f.command.action,
                    SessionAction::Open { .. }
                        | SessionAction::OpenAt { .. }
                        | SessionAction::OpenWillAt { .. }
                )
        }) {
            draft.end(connection)?;
            return Ok(Some(changed(Change::TransportClosed)));
        }
        let Some(session) = self
            .session_of(connection)
            .and_then(|idx| self.sessions.as_slice().get(idx))
        else {
            return Ok(None);
        };
        if session.will_managed || session.expiry != SessionExpiry::OnDisconnect {
            return Ok(None);
        }
        if let Some(flight) = &self.lifecycle {
            let covered = match &flight.command.action {
                SessionAction::Open { expected, .. }
                | SessionAction::OpenAt { expected, .. }
                | SessionAction::OpenWillAt { expected, .. } => expected
                    .is_some_and(|g| g.session == session.id && g.opened_by == session.opened_by),
                SessionAction::Close { guard, .. }
                | SessionAction::Detach { guard, .. }
                | SessionAction::Expire { guard, .. }
                | SessionAction::EndWill { guard, .. } => {
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
        // The runtime captured normal/abnormal intent before any delayed Open
        // result. Only its EndSessionWill may decide this attachment's Will.
        if matches!(flight.command.action, SessionAction::OpenWillAt { .. }) {
            return Ok(None);
        }
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
        if matches!(flight.command.action, SessionAction::OpenWillAt { .. }) {
            return Ok(None);
        }
        let (SessionAction::Open {
            expected: Some(guard),
            ..
        }
        | SessionAction::OpenAt {
            expected: Some(guard),
            ..
        }) = flight.command.action
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
                    connection: if matches!(flight.command.action, SessionAction::Expire { .. }) {
                        None
                    } else {
                        Some(flight.connection)
                    },
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
                if matches!(
                    flight.command.action,
                    SessionAction::Open { .. }
                        | SessionAction::OpenAt { .. }
                        | SessionAction::OpenWillAt { .. }
                ) && !flight.connection_closed
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
                }
                | SessionAction::OpenAt {
                    expected,
                    clean_start,
                    expiry,
                    ..
                }
                | SessionAction::OpenWillAt {
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
                let now = match flight.command.action {
                    SessionAction::OpenAt { now, .. } | SessionAction::OpenWillAt { now, .. } => {
                        Some(now)
                    }
                    _ => None,
                };
                let resumes = old.is_some_and(|s| {
                    !*clean_start
                        && s.expiry != SessionExpiry::OnDisconnect
                        && !s
                            .detached
                            .and_then(|d| d.deadline)
                            .is_some_and(|deadline| now.is_some_and(|now| now >= deadline))
                });
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
                SessionAction::Detach {
                    guard,
                    expiry,
                    observed_at,
                }
                | SessionAction::EndWill {
                    guard,
                    expiry,
                    observed_at,
                    ..
                },
                SessionChange::Detached {
                    session,
                    expiry: reported,
                    detached,
                },
            ) => {
                session == guard.session
                    && *expiry != SessionExpiry::OnDisconnect
                    && reported == *expiry
                    && expiry.deadline(*observed_at) == Some(detached.deadline)
                    && detached.operation == flight.command.operation
                    && self.sessions.as_slice().iter().any(|s| {
                        s.id == guard.session
                            && s.opened_by == guard.opened_by
                            && s.detached.is_none()
                            && s.connection.is_none_or(|g| g == flight.connection)
                    })
            }
            (
                SessionAction::Close { guard, .. }
                | SessionAction::EndWill {
                    guard,
                    expiry: SessionExpiry::OnDisconnect,
                    ..
                },
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
            (
                SessionAction::Expire {
                    guard,
                    detached,
                    now,
                },
                SessionChange::Closed {
                    session,
                    route_revision,
                    ..
                },
            ) => {
                session == guard.session
                    && route_revision >= current_revision
                    && self.sessions.as_slice().iter().any(|s| {
                        s.id == guard.session
                            && s.opened_by == guard.opened_by
                            && s.connection.is_none()
                            && s.detached == Some(*detached)
                            && detached.deadline.is_some_and(|deadline| *now >= deadline)
                    })
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
                            && !self.is_closed(flight.connection))
                        .then_some(flight.connection);
                        let value = Session {
                            id: session,
                            client_id: flight.client_id,
                            revision,
                            connection,
                            expiry,
                            opened_by: Some(finished.operation),
                            detached: None,
                            will_managed: matches!(
                                flight.command.action,
                                SessionAction::OpenWillAt { .. }
                            ),
                        };
                        if let Some(&index) = self.session_by_id.get(&session) {
                            self.replace_session(index, value)?;
                        } else {
                            self.push_session(value)?;
                        }
                        self.route_revision = Some(route_revision);
                    }
                    SessionChange::Detached {
                        session,
                        expiry,
                        detached,
                    } => {
                        let &index = self
                            .session_by_id
                            .get(&session)
                            .ok_or(CoreFault::StalePlan)?;
                        self.set_session_connection(index, None)?;
                        let held = &mut self.sessions.as_mut_slice()[index];
                        held.expiry = expiry;
                        held.detached = Some(detached);
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
                self.remember_finished(finished)?;
                self.lifecycle = followup;
            }
            Change::Refused { finished, followup } => {
                self.lifecycle.take().ok_or(CoreFault::StalePlan)?;
                self.remember_finished(finished)?;
                self.lifecycle = followup;
            }
        }
        Ok(())
    }
    fn retire_session(&mut self, session: SessionId) -> Result<(), CoreFault> {
        if let Some(&index) = self.session_by_id.get(&session) {
            if let Some(generation) = self.sessions.as_slice()[index].connection {
                self.close_outgoing(generation);
            }
            let _ = self.remove_session_at(index)?;
        }
        let mut removed_route = false;
        let mut index = 0;
        while index < self.routes.len() {
            if self.routes.as_slice()[index].target == session {
                self.routes.as_mut_slice()[index..].rotate_left(1);
                let _ = self.routes.pop();
                removed_route = true;
            } else {
                index += 1;
            }
        }
        if removed_route {
            self.rebuild_route_index();
        }
        let slot_indices: alloc::vec::Vec<usize> = self
            .slots_by_session
            .get(&session)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();
        // Process in descending index order so swap-removals preserve lower indices.
        for index in slot_indices.into_iter().rev() {
            if self.slots.as_slice()[index].in_flight().is_some() {
                self.slots.as_mut_slice()[index].retired = true;
                continue;
            }
            let held = self.slots.as_slice()[index];
            if self.leases.release(held.lease) != Ok(ReleaseOutcome::Released) {
                return Err(CoreFault::StalePlan);
            }
            if let Some(done) = held.admission {
                self.remember_finished(done)?;
            }
            let _ = self.remove_slot_at(index)?;
        }
        self.retire_outgoing(session);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::contract::effects::{OwnerAuthority, SubmitResult};
    use crate::contract::ids::LogicalTime;
    use crate::contract::opening::{
        EpochOpened, EpochOpening, OpeningOutcome, OpeningReply, StartupNonce,
    };
    use crate::contract::session::{
        SessionAction, SessionCancellations, SessionChange, SessionDetachment, SessionEndReason,
        SessionExpiry, SessionGuard, SessionLifecycle,
    };
    use alloc::string::ToString;
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
        let result = owner.step_checked(input, TURN);
        assert!(
            matches!(result, StepOutcome::Applied { .. }),
            "session_lifecycle_not_applied: {result:?}"
        );
        drain(owner)
    }
    fn world() -> Owner {
        world_with_outbox(32)
    }
    fn recovering_world_with_outbox(capacity: usize) -> Owner {
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
                NonZeroUsize::new(capacity).unwrap(),
                bound(8),
            )
            .unwrap();
        Owner::new(
            OwnerConfig {
                max_sessions: bound(1),
                max_routes: bound(4),
                max_exchanges: bound(8),
                session_qos2_exchanges: NonZeroU32::MAX,
                account: AccountId::new(1),
                payload_bytes: PayloadBytes::new(1024),
                resolution_delay: LogicalDuration::new(1),
                retained_operations: bound(16),
                remembered_connections: bound(4),
            },
            outbox,
        )
        .unwrap()
    }
    fn world_with_outbox(capacity: usize) -> Owner {
        let mut owner = recovering_world_with_outbox(capacity);
        let incarnation = owner.incarnation;
        let epoch = owner.epoch;
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
    fn closed(session: u64) -> SessionChange {
        SessionChange::Closed {
            session: SessionId::new(session),
            route_revision: Revision::new(0),
            cancelled: SessionCancellations::default(),
        }
    }
    fn answer(owner: &mut Owner, reply: StoreOutcome) -> Vec<(EffectId, Effect)> {
        let flight = owner.lifecycle.as_ref().unwrap();
        let input = Input::StoreResult {
            effect: flight.in_flight.answers.persist_id(),
            operation: flight.command.operation,
            reply: StoreReply::Outcome(reply),
        };
        apply(owner, input)
    }
    #[test]
    fn five_hundred_twelve_ephemeral_lifecycles_leave_only_bounded_rings() {
        let mut owner = world();
        for sid in 1..=512 {
            let first = submit(
                &mut owner,
                sid,
                sid.to_string().as_bytes(),
                SessionExpiry::OnDisconnect,
            );
            known(&mut owner, &first, opened(sid, SessionExpiry::OnDisconnect));
            let close = apply(
                &mut owner,
                Input::ConnectionClosed {
                    connection: ConnectionGeneration::new(sid),
                },
            );
            known(&mut owner, &close, closed(sid));
            let status = owner.session_status();
            assert_eq!(
                (
                    status.sessions,
                    status.bindings,
                    status.inbound,
                    status.outgoing,
                    status.retired_flights
                ),
                (0, 0, 0, 0, 0)
            );
            assert!(!status.lifecycle_pending);
            assert!(status.closed_generations <= 4);
            assert!(owner.finished.len() <= 16);
        }
        assert!(owner.routes.is_empty());
    }
    #[test]
    fn full_capacity_refuses_a_new_name_but_clean_start_replaces_in_one_slot() {
        let mut owner = world();
        let first = submit(&mut owner, 1, b"persistent", SessionExpiry::Never);
        known(&mut owner, &first, opened(1, SessionExpiry::Never));
        submit(&mut owner, 2, b"other", SessionExpiry::Never);
        let refusal = answer(
            &mut owner,
            StoreOutcome::RejectedCertain(RejectReason::SessionCapacityExceeded),
        );
        assert!(
            refusal
                .iter()
                .any(|(_, e)| matches!(e, Effect::SessionRefused { reason: 0x97, .. }))
        );
        assert_eq!(owner.sessions.as_slice()[0].id, SessionId::new(1));
        let replacement = apply(
            &mut owner,
            Input::OpenSession {
                connection: ConnectionGeneration::new(3),
                client_id: bytes(b"persistent"),
                clean_start: true,
                expiry: SessionExpiry::Never,
            },
        );
        let mut facts = opened(2, SessionExpiry::Never);
        if let SessionChange::Opened { retired, .. } = &mut facts {
            *retired = Some(SessionId::new(1));
        }
        known(&mut owner, &replacement, facts);
        assert_eq!(owner.sessions.len(), 1);
        assert_eq!(
            owner.connection_of(SessionId::new(2)),
            Some(ConnectionGeneration::new(3))
        );
        assert!(owner.connection_of(SessionId::new(1)).is_none());
    }
    #[test]
    fn closing_an_unknown_open_survives_eviction_from_the_closed_generation_ring() {
        let mut owner = world();
        let opening = submit(&mut owner, 1, b"ephemeral", SessionExpiry::OnDisconnect);
        let original = owner.lifecycle.clone().unwrap();
        answer(&mut owner, StoreOutcome::PendingOrUnknown);
        apply(
            &mut owner,
            Input::ConnectionClosed {
                connection: ConnectionGeneration::new(1),
            },
        );
        for generation in 2..=10 {
            apply(
                &mut owner,
                Input::ConnectionClosed {
                    connection: ConnectionGeneration::new(generation),
                },
            );
        }
        assert!(
            !owner
                .closed
                .iter()
                .any(|g| *g == ConnectionGeneration::new(1))
        );
        assert_eq!(
            owner.opening_connection(),
            Some(ConnectionGeneration::new(1))
        );
        let (operation, followup) =
            known(&mut owner, &opening, opened(1, SessionExpiry::OnDisconnect));
        assert!(!followup.iter().any(|(_, e)| matches!(
            e,
            Effect::EmitFrame {
                frame: OutboundFrame::Connack { .. },
                ..
            }
        )));
        assert!(
            matches!(owner.lifecycle.as_ref().unwrap().command.action, SessionAction::Close { guard, .. } if guard.session == SessionId::new(1) && guard.opened_by == Some(operation))
        );
        known(&mut owner, &followup, closed(1));
        let effects = apply(
            &mut owner,
            Input::StoreResult {
                effect: original.in_flight.answers.persist_id(),
                operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                    opened(1, SessionExpiry::OnDisconnect),
                ))),
            },
        );
        assert!(effects.is_empty());
        assert_eq!(owner.session_status().sessions, 0);
    }
    #[test]
    fn resuming_an_infinite_session_with_zero_expiry_rebinds_before_later_closure() {
        let mut owner = world();
        let first = submit(&mut owner, 1, b"persistent", SessionExpiry::Never);
        let (old_token, _) = known(&mut owner, &first, opened(1, SessionExpiry::Never));
        owner
            .routes
            .try_push(Route {
                topic: bytes(b"kept"),
                target: SessionId::new(1),
                target_revision: Revision::new(1),
                max_qos: Qos::AtLeastOnce,
                options: crate::contract::pubsub::SubscriptionOptions::DEFAULT,
            })
            .unwrap();
        let resume = submit(&mut owner, 2, b"persistent", SessionExpiry::OnDisconnect);
        let mut facts = opened(1, SessionExpiry::OnDisconnect);
        if let SessionChange::Opened {
            session_present, ..
        } = &mut facts
        {
            *session_present = true;
        }
        let (new_token, frames) = known(&mut owner, &resume, facts);
        assert!(frames.iter().any(|(_, e)| matches!(
            e,
            Effect::EmitFrame {
                frame: OutboundFrame::Connack {
                    session_present: true
                },
                ..
            }
        )));
        assert_eq!(
            owner.routes.len(),
            1,
            "zero expiry applies on closure, not on resume"
        );
        assert!(
            apply(
                &mut owner,
                Input::EndSession {
                    connection: ConnectionKey {
                        session: SessionId::new(1),
                        generation: ConnectionGeneration::new(1)
                    },
                    opened_by: Some(old_token),
                    reason: SessionEndReason::Disconnect,
                }
            )
            .is_empty(),
            "old_session_close_rebound"
        );
        assert_eq!(
            owner.connection_of(SessionId::new(1)),
            Some(ConnectionGeneration::new(2))
        );
        let close = apply(
            &mut owner,
            Input::ConnectionClosed {
                connection: ConnectionGeneration::new(2),
            },
        );
        assert!(
            matches!(owner.lifecycle.as_ref().unwrap().command.action, SessionAction::Close { guard, .. } if guard.opened_by == Some(new_token))
        );
        known(&mut owner, &close, closed(1));
        assert!(owner.routes.is_empty());
    }
    #[test]
    fn unknown_closure_keeps_capacity_and_resolves_the_same_operation() {
        let mut owner = world();
        let first = submit(&mut owner, 1, b"ephemeral", SessionExpiry::OnDisconnect);
        known(&mut owner, &first, opened(1, SessionExpiry::OnDisconnect));
        apply(
            &mut owner,
            Input::ConnectionClosed {
                connection: ConnectionGeneration::new(1),
            },
        );
        let original = owner.lifecycle.clone().unwrap();
        answer(&mut owner, StoreOutcome::PendingOrUnknown);
        assert_eq!(
            owner.sessions.len(),
            1,
            "session_capacity_released_before_close_commit"
        );
        let result = owner.step_checked(
            Input::OpenSession {
                connection: ConnectionGeneration::new(2),
                client_id: bytes(b"other"),
                clean_start: false,
                expiry: SessionExpiry::Never,
            },
            TURN,
        );
        assert!(matches!(result, StepOutcome::NeedCapacity { .. }));
        let resolve = apply(
            &mut owner,
            Input::Deadline {
                token: original.in_flight.deadline,
                now: LogicalTime::new(100),
            },
        );
        let (effect, operation) = resolve
            .iter()
            .find_map(|(id, e)| match e {
                Effect::ResolveAndFence { operation } => Some((*id, *operation)),
                _ => None,
            })
            .unwrap();
        assert_eq!(operation, original.command.operation);
        apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                    closed(1),
                ))),
            },
        );
        assert_eq!(owner.sessions.len(), 0);
        assert!(
            apply(
                &mut owner,
                Input::StoreResult {
                    effect: original.in_flight.answers.persist_id(),
                    operation,
                    reply: StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::SessionLifecycle(closed(1))
                    ))
                }
            )
            .is_empty()
        );
        assert_eq!(owner.sessions.len(), 0);
    }
    #[test]
    fn a_two_slot_legacy_outbox_refuses_a_three_effect_takeover_without_mutation() {
        let mut owner = world_with_outbox(2);
        let first = submit(&mut owner, 1, b"persistent", SessionExpiry::Never);
        known(&mut owner, &first, opened(1, SessionExpiry::Never));
        let before = owner.sessions.as_slice().to_vec();
        let counters = owner.counters;
        let result = owner.step_checked(
            Input::OpenSession {
                connection: ConnectionGeneration::new(2),
                client_id: bytes(b"persistent"),
                clean_start: true,
                expiry: SessionExpiry::Never,
            },
            TURN,
        );
        assert!(
            matches!(
                result,
                StepOutcome::Fatal(CoreFault::BoxTooSmall {
                    requested: 3,
                    capacity: 2
                })
            ),
            "{result:?}"
        );
        assert_eq!(owner.sessions.as_slice(), before.as_slice());
        assert_eq!(owner.counters, counters);
        assert!(!owner.session_status().lifecycle_pending);
        assert!(drain(&mut owner).is_empty());
    }
    #[test]
    fn reused_client_id_does_not_retarget_old_lifecycle_or_transport_facts() {
        let mut owner = world();
        let first = submit(&mut owner, 1, b"reusable", SessionExpiry::OnDisconnect);
        let original = owner.lifecycle.clone().unwrap();
        let (old_token, _) = known(&mut owner, &first, opened(1, SessionExpiry::OnDisconnect));
        let close = apply(
            &mut owner,
            Input::ConnectionClosed {
                connection: ConnectionGeneration::new(1),
            },
        );
        let old_close = owner.lifecycle.clone().unwrap();
        known(&mut owner, &close, closed(1));
        let next = submit(&mut owner, 2, b"reusable", SessionExpiry::Never);
        let (new_token, _) = known(&mut owner, &next, opened(2, SessionExpiry::Never));
        let old_inputs = [
            Input::EndSession {
                connection: ConnectionKey {
                    session: SessionId::new(1),
                    generation: ConnectionGeneration::new(1),
                },
                opened_by: Some(old_token),
                reason: SessionEndReason::Disconnect,
            },
            Input::ConnectionClosed {
                connection: ConnectionGeneration::new(1),
            },
            Input::StoreResult {
                effect: original.in_flight.answers.persist_id(),
                operation: old_token,
                reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                    opened(1, SessionExpiry::OnDisconnect),
                ))),
            },
            Input::StoreResult {
                effect: old_close.in_flight.answers.persist_id(),
                operation: old_close.command.operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                    closed(1),
                ))),
            },
        ];
        for input in old_inputs {
            assert!(
                apply(&mut owner, input).is_empty(),
                "old_incarnation_fact_retargeted"
            );
            assert_eq!(owner.sessions.len(), 1);
            assert_eq!(owner.sessions.as_slice()[0].id, SessionId::new(2));
            assert_eq!(owner.sessions.as_slice()[0].opened_by, Some(new_token));
            assert_eq!(
                owner.sessions.as_slice()[0].connection,
                Some(ConnectionGeneration::new(2))
            );
            assert!(owner.lifecycle.is_none());
        }
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
                effect: flight.in_flight.answers.persist_id(),
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
            effect: flight.in_flight.answers.persist_id(),
            operation: flight.command.operation,
            reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                opened(2, SessionExpiry::Never),
            ))),
        };
        assert!(
            matches!(
                owner.step_checked(input, TURN),
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
                    options: crate::contract::pubsub::SubscriptionOptions::DEFAULT,
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
    fn submit_at(
        owner: &mut Owner,
        generation: u64,
        name: &[u8],
        expiry: SessionExpiry,
        now: u64,
    ) -> Vec<(EffectId, Effect)> {
        apply(
            owner,
            Input::OpenSessionAt {
                connection: ConnectionGeneration::new(generation),
                client_id: bytes(name),
                clean_start: false,
                expiry,
                now,
            },
        )
    }
    fn detach_at(
        owner: &mut Owner,
        generation: u64,
        opened_by: OperationId,
        expiry: SessionExpiry,
        observed_at: u64,
    ) -> SessionDetachment {
        let effects = apply(
            owner,
            Input::DetachSession {
                connection: ConnectionKey {
                    session: SessionId::new(1),
                    generation: ConnectionGeneration::new(generation),
                },
                opened_by: Some(opened_by),
                expiry,
                observed_at,
            },
        );
        let detached = SessionDetachment {
            operation: owner.lifecycle.as_ref().unwrap().command.operation,
            deadline: expiry.deadline(observed_at).unwrap(),
        };
        let (_, emitted) = known(
            owner,
            &effects,
            SessionChange::Detached {
                session: SessionId::new(1),
                expiry,
                detached,
            },
        );
        assert!(
            emitted
                .iter()
                .all(|(_, e)| matches!(e, Effect::SessionChanged { .. }))
        );
        detached
    }
    #[test]
    fn finite_open_retains_the_admissible_time_in_its_canonical_recipe() {
        let mut owner = world();
        let effects = submit_at(&mut owner, 1, b"finite", SessionExpiry::After(7), 42);
        assert!(effects.iter().any(|(_, e)| matches!(e, Effect::Persist { command } if matches!(command.recipe.get(), Recipe::SessionLifecycle(SessionLifecycle { action: SessionAction::OpenAt { expiry: SessionExpiry::After(7), now: 42, .. }, .. })))), "finite_open_lost_authoritative_observation");
        known(&mut owner, &effects, opened(1, SessionExpiry::After(7)));
        assert_eq!(
            owner.connection_of(SessionId::new(1)),
            Some(ConnectionGeneration::new(1))
        );
    }
    #[test]
    fn finite_detachment_unknown_result_keeps_the_original_deadline_and_operation() {
        let mut owner = world();
        let first = submit_at(&mut owner, 1, b"finite", SessionExpiry::After(1), 9);
        let (opened_by, _) = known(&mut owner, &first, opened(1, SessionExpiry::After(1)));
        apply(
            &mut owner,
            Input::DetachSession {
                connection: ConnectionKey {
                    session: SessionId::new(1),
                    generation: ConnectionGeneration::new(1),
                },
                opened_by: Some(opened_by),
                expiry: SessionExpiry::After(1),
                observed_at: 123,
            },
        );
        let original = owner.lifecycle.as_ref().unwrap().clone();
        assert!(matches!(
            original.command.action,
            SessionAction::Detach {
                observed_at: 123,
                ..
            }
        ));
        answer(&mut owner, StoreOutcome::PendingOrUnknown);
        assert!(owner.sessions.as_slice()[0].detached.is_none());
        assert_eq!(
            owner.sessions.len(),
            1,
            "detach_unknown_freed_session_capacity"
        );
        let resolution = apply(
            &mut owner,
            Input::Deadline {
                token: original.in_flight.deadline,
                now: LogicalTime::new(10_000),
            },
        );
        let (effect, operation) = resolution
            .iter()
            .find_map(|(id, e)| match e {
                Effect::ResolveAndFence { operation } => Some((*id, *operation)),
                _ => None,
            })
            .unwrap();
        assert_eq!(operation, original.command.operation);
        assert_eq!(owner.lifecycle.as_ref().unwrap().command, original.command);
        let detached = SessionDetachment {
            operation,
            deadline: Some(1_000_000_123),
        };
        apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                    SessionChange::Detached {
                        session: SessionId::new(1),
                        expiry: SessionExpiry::After(1),
                        detached,
                    },
                ))),
            },
        );
        assert_eq!(owner.sessions.as_slice()[0].detached, Some(detached));
        assert!(
            apply(
                &mut owner,
                Input::ExpireSession {
                    guard: SessionGuard {
                        session: SessionId::new(1),
                        opened_by: Some(opened_by)
                    },
                    detached,
                    now: 1_000_000_122
                }
            )
            .is_empty()
        );
        let expiry = apply(
            &mut owner,
            Input::ExpireSession {
                guard: SessionGuard {
                    session: SessionId::new(1),
                    opened_by: Some(opened_by),
                },
                detached,
                now: 1_000_000_123,
            },
        );
        let (_, emitted) = known(&mut owner, &expiry, closed(1));
        assert!(
            matches!(
                emitted.as_slice(),
                [(
                    _,
                    Effect::SessionChanged {
                        connection: None,
                        ..
                    }
                )]
            ),
            "transportless_expiry_exposed_a_phantom_generation"
        );
        assert!(owner.sessions.is_empty());
    }
    #[test]
    fn time_aware_open_decides_expiry_even_when_the_timer_has_not_run() {
        for now in [999_999_999, 1_000_000_000, 1_000_000_001] {
            let mut owner = world();
            let first = submit_at(&mut owner, 1, b"finite", SessionExpiry::After(1), 0);
            let (opened_by, _) = known(&mut owner, &first, opened(1, SessionExpiry::After(1)));
            let detached = detach_at(&mut owner, 1, opened_by, SessionExpiry::After(1), 0);
            let next = submit_at(&mut owner, 2, b"finite", SessionExpiry::After(1), now);
            let resumes = now < 1_000_000_000;
            let mut change = opened(if resumes { 1 } else { 2 }, SessionExpiry::After(1));
            if let SessionChange::Opened {
                session_present,
                retired,
                ..
            } = &mut change
            {
                *session_present = resumes;
                *retired = (!resumes).then_some(SessionId::new(1));
            }
            let (_, frames) = known(&mut owner, &next, change);
            assert!(frames.iter().any(|(_, e)| matches!(e, Effect::EmitFrame { frame: OutboundFrame::Connack { session_present }, .. } if *session_present == resumes)), "resume_after_deadline_depended_on_timer_delivery");
            assert!(owner.sessions.as_slice()[0].detached.is_none());
            assert!(
                apply(
                    &mut owner,
                    Input::ExpireSession {
                        guard: SessionGuard {
                            session: SessionId::new(1),
                            opened_by: Some(opened_by)
                        },
                        detached,
                        now: u64::MAX
                    }
                )
                .is_empty(),
                "old_timer_rebound_after_open"
            );
            assert_eq!(owner.sessions.len(), 1);
            assert_eq!(
                owner.sessions.as_slice()[0].connection,
                Some(ConnectionGeneration::new(2))
            );
        }
    }
    #[test]
    fn a_detachment_and_timer_cannot_rearm_or_terminate_a_new_occurrence() {
        let mut owner = world();
        let first = submit_at(&mut owner, 1, b"finite", SessionExpiry::After(1), 0);
        let (old_open, _) = known(&mut owner, &first, opened(1, SessionExpiry::After(1)));
        let old_detached = detach_at(&mut owner, 1, old_open, SessionExpiry::After(1), 0);
        assert!(
            apply(
                &mut owner,
                Input::DetachSession {
                    connection: ConnectionKey {
                        session: SessionId::new(1),
                        generation: ConnectionGeneration::new(1)
                    },
                    opened_by: Some(old_open),
                    expiry: SessionExpiry::After(7),
                    observed_at: 17
                }
            )
            .is_empty(),
            "duplicate_detachment_rearmed_deadline"
        );
        let resume = submit_at(&mut owner, 2, b"finite", SessionExpiry::After(1), 1);
        let mut resumed = opened(1, SessionExpiry::After(1));
        if let SessionChange::Opened {
            session_present, ..
        } = &mut resumed
        {
            *session_present = true;
        }
        let (new_open, _) = known(&mut owner, &resume, resumed);
        let current_detached = detach_at(&mut owner, 2, new_open, SessionExpiry::After(1), 100);
        assert_ne!(old_detached.operation, current_detached.operation);
        assert!(
            apply(
                &mut owner,
                Input::ExpireSession {
                    guard: SessionGuard {
                        session: SessionId::new(1),
                        opened_by: Some(old_open)
                    },
                    detached: old_detached,
                    now: u64::MAX
                }
            )
            .is_empty()
        );
        assert_eq!(
            owner.sessions.as_slice()[0].detached,
            Some(current_detached)
        );
        let expiry = apply(
            &mut owner,
            Input::ExpireSession {
                guard: SessionGuard {
                    session: SessionId::new(1),
                    opened_by: Some(new_open),
                },
                detached: current_detached,
                now: 1_000_000_100,
            },
        );
        known(&mut owner, &expiry, closed(1));
        assert!(owner.sessions.is_empty());
    }
    #[test]
    fn finite_open_on_a_lost_transport_never_invents_a_later_close_instant() {
        let mut owner = world();
        let first = submit_at(&mut owner, 1, b"finite", SessionExpiry::After(1), 0);
        apply(
            &mut owner,
            Input::ConnectionClosed {
                connection: ConnectionGeneration::new(1),
            },
        );
        answer(&mut owner, StoreOutcome::PendingOrUnknown);
        let (opened_by, effects) = known(&mut owner, &first, opened(1, SessionExpiry::After(1)));
        assert!(
            effects
                .iter()
                .all(|(_, e)| matches!(e, Effect::SessionChanged { .. })),
            "closed_open_recomputed_a_detachment_instant"
        );
        assert!(owner.connection_of(SessionId::new(1)).is_none());
        assert!(owner.sessions.as_slice()[0].detached.is_none());
        let detached = detach_at(&mut owner, 1, opened_by, SessionExpiry::After(1), 123);
        assert_eq!(detached.deadline, Some(1_000_000_123));
    }
    #[test]
    fn an_active_finite_session_cannot_be_expired_and_never_override_has_no_deadline() {
        let mut owner = world();
        let first = submit_at(&mut owner, 1, b"finite", SessionExpiry::After(1), 0);
        let (opened_by, _) = known(&mut owner, &first, opened(1, SessionExpiry::After(1)));
        let invented = SessionDetachment {
            operation: opened_by,
            deadline: Some(1),
        };
        assert!(
            apply(
                &mut owner,
                Input::ExpireSession {
                    guard: SessionGuard {
                        session: SessionId::new(1),
                        opened_by: Some(opened_by)
                    },
                    detached: invented,
                    now: u64::MAX
                }
            )
            .is_empty(),
            "active_session_expired"
        );
        let detached = detach_at(&mut owner, 1, opened_by, SessionExpiry::Never, 3);
        assert_eq!(detached.deadline, None);
        assert!(
            apply(
                &mut owner,
                Input::ExpireSession {
                    guard: SessionGuard {
                        session: SessionId::new(1),
                        opened_by: Some(opened_by)
                    },
                    detached,
                    now: u64::MAX
                }
            )
            .is_empty()
        );
        let next = submit_at(&mut owner, 2, b"finite", SessionExpiry::Never, u64::MAX);
        let mut resumed = opened(1, SessionExpiry::Never);
        if let SessionChange::Opened {
            session_present, ..
        } = &mut resumed
        {
            *session_present = true;
        }
        known(&mut owner, &next, resumed);
        assert!(owner.sessions.as_slice()[0].detached.is_none());
        assert_eq!(owner.sessions.as_slice()[0].expiry, SessionExpiry::Never);
    }
    #[test]
    fn repeated_finite_rearms_keep_only_the_live_session_and_bounded_rings() {
        let mut owner = world();
        let first = submit_at(&mut owner, 1, b"finite", SessionExpiry::After(1), 0);
        let (mut token, _) = known(&mut owner, &first, opened(1, SessionExpiry::After(1)));
        for generation in 1..=64 {
            let detached = detach_at(
                &mut owner,
                generation,
                token,
                SessionExpiry::After(1),
                generation,
            );
            let next = submit_at(
                &mut owner,
                generation + 1,
                b"finite",
                SessionExpiry::After(1),
                generation + 1,
            );
            let mut resumed = opened(1, SessionExpiry::After(1));
            if let SessionChange::Opened {
                session_present, ..
            } = &mut resumed
            {
                *session_present = true;
            }
            let (new_token, _) = known(&mut owner, &next, resumed);
            assert!(
                apply(
                    &mut owner,
                    Input::ExpireSession {
                        guard: SessionGuard {
                            session: SessionId::new(1),
                            opened_by: Some(token)
                        },
                        detached,
                        now: u64::MAX
                    }
                )
                .is_empty()
            );
            token = new_token;
            assert_eq!(owner.sessions.len(), 1);
            assert!(owner.finished.len() <= 16);
            assert!(owner.closed.len() <= 4);
            assert!(!owner.session_status().lifecycle_pending);
        }
    }

    #[test]
    fn a_contradictory_detachment_deadline_cannot_become_durable_knowledge() {
        let mut owner = world();
        let first = submit_at(&mut owner, 1, b"finite", SessionExpiry::After(1), 0);
        let (opened_by, _) = known(&mut owner, &first, opened(1, SessionExpiry::After(1)));
        apply(
            &mut owner,
            Input::DetachSession {
                connection: ConnectionKey {
                    session: SessionId::new(1),
                    generation: ConnectionGeneration::new(1),
                },
                opened_by: Some(opened_by),
                expiry: SessionExpiry::After(1),
                observed_at: 123,
            },
        );
        let flight = owner.lifecycle.as_ref().unwrap().clone();
        let outcome = owner.step_checked(
            Input::StoreResult {
                effect: flight.in_flight.answers.persist_id(),
                operation: flight.command.operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                    SessionChange::Detached {
                        session: SessionId::new(1),
                        expiry: SessionExpiry::After(1),
                        detached: SessionDetachment {
                            operation: flight.command.operation,
                            deadline: Some(1_000_000_124),
                        },
                    },
                ))),
            },
            TURN,
        );
        assert!(
            matches!(outcome, StepOutcome::Fatal(CoreFault::ContradictoryOutcome { operation }) if operation == flight.command.operation),
            "detach_result_recalculated_deadline"
        );
        assert!(owner.sessions.as_slice()[0].detached.is_none());
        assert_eq!(owner.lifecycle.as_ref().unwrap().command, flight.command);
    }

    #[test]
    fn expiry_unknown_result_keeps_capacity_until_the_original_operation_resolves() {
        let mut owner = world();
        let first = submit_at(&mut owner, 1, b"finite", SessionExpiry::After(1), 0);
        let (opened_by, _) = known(&mut owner, &first, opened(1, SessionExpiry::After(1)));
        let detached = detach_at(&mut owner, 1, opened_by, SessionExpiry::After(1), 0);
        apply(
            &mut owner,
            Input::ExpireSession {
                guard: SessionGuard {
                    session: SessionId::new(1),
                    opened_by: Some(opened_by),
                },
                detached,
                now: 1_000_000_000,
            },
        );
        let flight = owner.lifecycle.as_ref().unwrap().clone();
        answer(&mut owner, StoreOutcome::PendingOrUnknown);
        assert_eq!(
            owner.sessions.len(),
            1,
            "expiry_unknown_freed_session_capacity"
        );
        assert_eq!(owner.sessions.as_slice()[0].detached, Some(detached));
        let resolution = apply(
            &mut owner,
            Input::Deadline {
                token: flight.in_flight.deadline,
                now: LogicalTime::new(10_000),
            },
        );
        let (effect, operation) = resolution
            .iter()
            .find_map(|(effect, e)| match e {
                Effect::ResolveAndFence { operation } => Some((*effect, *operation)),
                _ => None,
            })
            .unwrap();
        assert_eq!(operation, flight.command.operation);
        apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                    closed(1),
                ))),
            },
        );
        assert!(owner.sessions.is_empty());
        let next = submit_at(
            &mut owner,
            2,
            b"replacement",
            SessionExpiry::Never,
            2_000_000_000,
        );
        known(&mut owner, &next, opened(2, SessionExpiry::Never));
        assert!(
            apply(
                &mut owner,
                Input::StoreResult {
                    effect,
                    operation,
                    reply: StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::SessionLifecycle(closed(1))
                    ))
                }
            )
            .is_empty(),
            "old_expiry_result_ended_a_new_incarnation"
        );
        assert_eq!(
            owner.connection_of(SessionId::new(2)),
            Some(ConnectionGeneration::new(2))
        );
    }

    #[test]
    fn an_expired_open_cannot_claim_session_present_because_the_timer_was_delayed() {
        let mut owner = world();
        let first = submit_at(&mut owner, 1, b"finite", SessionExpiry::After(1), 0);
        let (opened_by, _) = known(&mut owner, &first, opened(1, SessionExpiry::After(1)));
        detach_at(&mut owner, 1, opened_by, SessionExpiry::After(1), 0);
        submit_at(
            &mut owner,
            2,
            b"finite",
            SessionExpiry::After(1),
            1_000_000_000,
        );
        let flight = owner.lifecycle.as_ref().unwrap().clone();
        let mut wrong = opened(1, SessionExpiry::After(1));
        if let SessionChange::Opened {
            session_present, ..
        } = &mut wrong
        {
            *session_present = true;
        }
        let outcome = owner.step_checked(
            Input::StoreResult {
                effect: flight.in_flight.answers.persist_id(),
                operation: flight.command.operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                    wrong,
                ))),
            },
            TURN,
        );
        assert!(
            matches!(outcome, StepOutcome::Fatal(CoreFault::ContradictoryOutcome { operation }) if operation == flight.command.operation),
            "expired_open_resumed_without_timer"
        );
        assert!(owner.sessions.as_slice()[0].detached.is_some());
    }
    #[test]
    fn recovery_refuses_a_finite_deadline_earlier_than_its_minimum_interval() {
        for seconds in [1, u32::MAX - 1] {
            let minimum = u64::from(seconds) * 1_000_000_000;
            for deadline in [0, minimum - 1, minimum, minimum + 1] {
                let mut owner = recovering_world_with_outbox(32);
                let operation = OperationId::new(
                    owner.incarnation,
                    RunEpoch::new(1),
                    LogicalOwnerId::new(4),
                    OperationSeq::new(1),
                );
                let facts = [
                    RecoveredFact::Epoch {
                        incarnation: owner.incarnation,
                        epoch: owner.epoch,
                    },
                    RecoveredFact::Limits {
                        max_targets_per_admission: 2,
                        max_topic_bytes: 128,
                        max_payload_bytes: PayloadBytes::new(128),
                    },
                    RecoveredFact::RouteTable {
                        revision: Revision::new(0),
                    },
                    RecoveredFact::Session {
                        session: SessionId::new(1),
                        client_id: bytes(b"finite"),
                        revision: Revision::new(1),
                        expiry: SessionExpiry::After(seconds),
                        opened_by: Some(operation),
                        detached: Some(SessionDetachment {
                            operation,
                            deadline: Some(deadline),
                        }),
                    },
                ];
                let outcome = owner.step_checked(
                    Input::Recovery {
                        facts: BoundedVec::from_slice(&facts, facts.len()).unwrap(),
                        last: true,
                    },
                    TURN,
                );
                if deadline < minimum {
                    assert!(
                        matches!(outcome, StepOutcome::Fatal(CoreFault::ImpossibleInput)),
                        "finite_recovery_accepted_impossible_deadline"
                    );
                    assert!(owner.is_recovering());
                    assert!(owner.sessions.is_empty());
                } else {
                    assert!(matches!(outcome, StepOutcome::Applied { .. }));
                    assert!(!owner.is_recovering());
                    assert_eq!(
                        owner.sessions.as_slice()[0].detached.unwrap().deadline,
                        Some(deadline)
                    );
                }
            }
        }
    }
}
