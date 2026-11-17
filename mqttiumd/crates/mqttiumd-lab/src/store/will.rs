//! Same bounded intent authority as SQLite; no source-session lifetime dependency.
use super::*;
use mqttiumd_contract::will::{
    MAX_WILL_BYTES, MAX_WILL_INTENTS, WillIntent, WillPublication, WillState,
};

pub(super) fn validate(state: &DurableState) -> Result<(), ReadError> {
    receipt_capacity(state, &[])?;
    if state.wills.len() > MAX_WILL_INTENTS {
        return Err(ReadError::LimitExceeded);
    }
    let mut bytes = 0u64;
    for (id, intent) in &state.wills {
        if *id != intent.id
            || id.incarnation() != state.ledger.incarnation()
            || id.epoch().get() == 0
            || id.epoch() > state.ledger.last_opened()
            || intent.session.get() == 0
            || intent.session >= state.next_session
            || intent.client_id.is_empty()
            || intent.client_id.len() > mqttiumd_contract::session::MAX_SESSION_CLIENT_BYTES
            || intent.client_id.as_slice().contains(&0)
            || std::str::from_utf8(intent.client_id.as_slice()).is_err()
            || intent.spec.check().is_err()
        {
            return Err(ReadError::InvalidData);
        }
        bytes = bytes
            .checked_add(intent.spec.payload.len() as u64)
            .ok_or(ReadError::LimitExceeded)?;
        if bytes > MAX_WILL_BYTES {
            return Err(ReadError::LimitExceeded);
        }
        if intent.state == WillState::Armed
            && !state.sessions.get(&intent.session).is_some_and(|s| {
                s.state == SessionState::Active && s.opened_by == Some(*id) && s.detached.is_none()
            })
        {
            return Err(ReadError::InvalidData);
        }
    }
    Ok(())
}
pub(super) fn page(
    state: &DurableState,
    after: Option<OperationId>,
    limit: usize,
    budget: ReadBudget,
) -> Result<WillPage, ReadError> {
    validate(state)?;
    if limit == 0 || limit > MAX_WILL_INTENTS || limit > budget.max_rows {
        return Err(ReadError::LimitExceeded);
    }
    let mut items = BoundedVec::with_capacity(limit).map_err(|_| ReadError::AllocationFailed)?;
    let mut meter = ReadMeter::new(budget);
    let mut more = false;
    for (id, intent) in &state.wills {
        if intent.state != WillState::Pending || after.is_some_and(|a| *id <= a) {
            continue;
        }
        if items.len() == limit {
            more = true;
            break;
        }
        let bytes = 128usize
            .checked_add(intent.client_id.len())
            .and_then(|n| n.checked_add(intent.spec.topic.len()))
            .and_then(|n| n.checked_add(intent.spec.payload.len()))
            .ok_or(ReadError::LimitExceeded)?;
        for field in [&intent.client_id, &intent.spec.topic, &intent.spec.payload] {
            budget.check_field(field.len())?;
        }
        meter.charge(bytes)?;
        items
            .try_push(intent.clone())
            .map_err(|_| ReadError::AllocationFailed)?;
    }
    let next = more
        .then(|| items.as_slice().last().map(|i| i.id))
        .flatten();
    Ok(WillPage { items, next })
}
pub(super) fn open(state: &mut DurableState, command: &SessionLifecycle) -> Guard<CommitFacts> {
    let failed = RejectReason::PreconditionFailed;
    let SessionAction::OpenWillAt {
        client_id,
        expected,
        clean_start,
        expiry,
        now,
        will,
    } = &command.action
    else {
        return Err(failed);
    };
    if state.wills.values().any(|i| i.client_id == *client_id) {
        return Err(failed);
    }
    if let Some(spec) = will {
        if spec.topic.len() > state.limits.max_topic_bytes
            || spec.payload.len() > state.limits.max_payload_bytes
        {
            return Err(failed);
        }
        let total = state
            .wills
            .values()
            .try_fold(0u64, |n, i| n.checked_add(i.spec.payload.len() as u64))
            .ok_or(failed)?;
        if state.wills.len() >= MAX_WILL_INTENTS
            || total
                .checked_add(spec.payload.len() as u64)
                .is_none_or(|n| n > MAX_WILL_BYTES)
        {
            return Err(RejectReason::CapacityExceeded(Resource::PayloadBytes));
        }
    }
    let legacy = SessionLifecycle {
        operation: command.operation,
        action: SessionAction::OpenAt {
            client_id: client_id.clone(),
            expected: *expected,
            clean_start: *clean_start,
            expiry: *expiry,
            now: *now,
        },
    };
    let facts = state.stage_session(&legacy)?;
    if let (Some(spec), CommitFacts::SessionLifecycle(SessionChange::Opened { session, .. })) =
        (will, facts)
    {
        state.wills.insert(
            command.operation,
            WillIntent {
                id: command.operation,
                session,
                client_id: client_id.clone(),
                spec: spec.clone(),
                state: WillState::Armed,
            },
        );
    }
    Ok(facts)
}
pub(super) fn end(state: &mut DurableState, command: &SessionLifecycle) -> Guard<CommitFacts> {
    let failed = RejectReason::PreconditionFailed;
    let SessionAction::EndWill {
        guard,
        expiry,
        observed_at,
        publish,
    } = command.action
    else {
        return Err(failed);
    };
    let row = state
        .sessions
        .get(&guard.session)
        .filter(|s| s.state == SessionState::Active && s.detached.is_none())
        .ok_or(failed)?;
    if row.opened_by.is_none() || row.opened_by != guard.opened_by {
        return Err(failed);
    }
    if let Some(intent) = state.wills.get_mut(&guard.opened_by.ok_or(failed)?) {
        if intent.session != guard.session || intent.state != WillState::Armed {
            return Err(failed);
        }
        if publish {
            intent.state = WillState::Pending;
        } else {
            state.wills.remove(&guard.opened_by.ok_or(failed)?);
        }
    }
    state.stage_session(&SessionLifecycle {
        operation: command.operation,
        action: SessionAction::Detach {
            guard,
            expiry,
            observed_at,
        },
    })
}
pub(super) fn publish(
    state: &mut DurableState,
    value: &WillPublication,
    payload: Option<&SharedBytes>,
) -> Guard<CommitFacts> {
    let failed = RejectReason::PreconditionFailed;
    let intent = state.wills.get(&value.will).ok_or(failed)?;
    if intent.state != WillState::Pending
        || value.topic != intent.spec.topic
        || payload != Some(&intent.spec.payload)
        || value.payload.length.get() != intent.spec.payload.len() as u64
    {
        return Err(failed);
    }
    let source_session = intent.session;
    state.stage_publication(
        Admission {
            expiry: None,
            operation: value.operation,
            request: None,
            will: Some(value.will),
            publication: value.publication,
            source_session,
            source_revision: Revision::new(0),
            inbound: None,
            route_revision: value.route_revision,
            qos: Qos::AtLeastOnce,
            retain: false,
            topic: &value.topic,
            payload: value.payload,
            costs: value.costs,
            targets: value.targets.as_slice(),
        },
        None,
        payload,
    )?;
    state.wills.remove(&value.will).ok_or(failed)?;
    Ok(CommitFacts::WillPublished {
        will: value.will,
        publication: value.publication,
    })
}

pub(super) fn receipt_capacity(state: &DurableState, canonical: &[u8]) -> Result<bool, ReadError> {
    let bytes = state
        .operations
        .values()
        .filter(|r| r.recipe.get(6) == Some(&8) && r.recipe.get(39) == Some(&5))
        .try_fold(0usize, |n, r| n.checked_add(r.recipe.len()))
        .ok_or(ReadError::LimitExceeded)?;
    const MAX_BYTES: usize = 8 * 1024 * 1024;
    if bytes > MAX_BYTES {
        return Err(ReadError::LimitExceeded);
    }
    let is_open = canonical.get(6) == Some(&8) && canonical.get(39) == Some(&5);
    Ok(!is_open
        || bytes
            .checked_add(canonical.len())
            .is_some_and(|n| n <= MAX_BYTES))
}
