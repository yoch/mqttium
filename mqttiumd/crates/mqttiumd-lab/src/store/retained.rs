//! Bounded retained ownership and transactional replay for the deterministic store.
use super::*;
use mqttiumd_contract::command::DeliveryCause;
use mqttiumd_contract::retained::{
    MAX_REPLAYS, MAX_RETAINED_BYTES, MAX_RETAINED_TOPICS, ReplaySubscriptions,
};

#[derive(Clone, Copy)]
pub(super) struct IdentityBounds {
    pub first: PublicationId,
    pub last: PublicationId,
    pub payload: Option<PayloadId>,
}
pub(super) fn bounds(recipe: &Recipe) -> Result<Option<IdentityBounds>, ReadError> {
    let one = |p, payload| {
        Some(IdentityBounds {
            first: p,
            last: p,
            payload: Some(payload),
        })
    };
    Ok(match recipe {
        Recipe::AdmitPublication(a) => one(a.publication, a.payload.id),
        Recipe::RetainedPublication(a) => one(a.admission.publication, a.admission.payload.id),
        Recipe::RetainedQos0(a) => one(a.publication, a.payload.id),
        Recipe::ReplaySubscriptions(a) => {
            let first = a.first_publication;
            Some(IdentityBounds {
                first,
                last: PublicationId::new(
                    first.incarnation(),
                    first.epoch(),
                    first.owner(),
                    PublicationSeq::new(
                        first
                            .sequence()
                            .get()
                            .checked_add(MAX_REPLAYS as u64 - 1)
                            .ok_or(ReadError::InvalidData)?,
                    ),
                ),
                payload: None,
            })
        }
        _ => None,
    })
}
pub(super) fn receipt_bounds(
    state: &DurableState,
    operation: OperationId,
    record: &OperationRecord,
) -> Result<Option<IdentityBounds>, ReadError> {
    let Decision::Committed(facts) = record.decision else {
        return Ok(None);
    };
    let recipe = Recipe::decode_canonical(
        &record.recipe,
        state.limits.max_targets_per_admission,
        state.limits.max_topic_bytes,
    )
    .map_err(|error| match error {
        RecipeError::AllocationFailed => ReadError::AllocationFailed,
        RecipeError::TooManyTargets { .. } | RecipeError::TopicTooLong(_) => {
            ReadError::LimitExceeded
        }
        _ => ReadError::InvalidData,
    })?;
    if recipe.operation() != operation {
        return Err(ReadError::InvalidData);
    }
    let Some(bounds) = bounds(&recipe)? else {
        return Ok(None);
    };
    let valid = match (&recipe, facts) {
        (
            Recipe::AdmitPublication(a),
            CommitFacts::PublicationAdmitted {
                publication,
                exchange,
            },
        ) => a.publication == publication && a.source.exchange == exchange,
        (
            Recipe::RetainedPublication(a),
            CommitFacts::PublicationAdmitted {
                publication,
                exchange,
            },
        ) => a.admission.publication == publication && a.admission.source.exchange == exchange,
        (Recipe::RetainedQos0(a), CommitFacts::RetainedQos0Stored { publication }) => {
            a.publication == publication
        }
        (Recipe::ReplaySubscriptions(a), CommitFacts::SubscriptionsChanged { session, .. }) => {
            a.update.session == session
        }
        _ => false,
    };
    let scope = |p: PublicationId| {
        (p.incarnation(), p.epoch(), p.owner())
            == (
                operation.incarnation(),
                operation.epoch(),
                operation.owner(),
            )
    };
    if !valid
        || !scope(bounds.first)
        || !scope(bounds.last)
        || bounds.payload.is_some_and(|p| {
            (p.incarnation(), p.epoch(), p.owner())
                != (
                    operation.incarnation(),
                    operation.epoch(),
                    operation.owner(),
                )
        })
    {
        return Err(ReadError::InvalidData);
    }
    Ok(Some(bounds))
}
pub(super) fn identity_was_used(state: &DurableState, recipe: &Recipe) -> Result<bool, ReadError> {
    let Some(incoming) = bounds(recipe)? else {
        return Ok(false);
    };
    let operation = recipe.operation();
    if state
        .receipt_frontiers
        .get(&ReceiptOrigin::of(operation))
        .is_some_and(|c| {
            incoming.first.sequence() <= c.publication_through
                || incoming
                    .payload
                    .is_some_and(|p| p.sequence() <= c.payload_through)
        })
    {
        return Ok(true);
    }
    if state
        .publications
        .range(incoming.first..=incoming.last)
        .next()
        .is_some()
        || incoming.payload.is_some_and(|p| {
            state.payloads.contains_key(&p) || state.publications.values().any(|r| r.payload == p)
        })
    {
        return Ok(true);
    }
    let id = |seq| {
        OperationId::new(
            operation.incarnation(),
            operation.epoch(),
            operation.owner(),
            OperationSeq::new(seq),
        )
    };
    let mut meter = ReadMeter::new(ReadBudget::SNAPSHOT);
    for (op, record) in state.operations.range(id(0)..=id(u64::MAX)) {
        meter.field(record.recipe.len(), ReadBudget::SNAPSHOT.max_field_bytes)?;
        meter.charge(
            256usize
                .checked_add(record.recipe.len())
                .ok_or(ReadError::LimitExceeded)?,
        )?;
        if let Some(old) = receipt_bounds(state, *op, record)?
            && (incoming.first <= old.last && old.first <= incoming.last
                || incoming.payload.is_some() && incoming.payload == old.payload)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn validate(state: &DurableState, budget: ReadBudget) -> Result<(), ReadError> {
    if state.retained.len() > MAX_RETAINED_TOPICS {
        return Err(ReadError::LimitExceeded);
    }
    let mut total = 0u64;
    let mut meter = ReadMeter::new(budget);
    for (topic, record) in &state.retained {
        meter.field(topic.len(), state.limits.max_topic_bytes)?;
        meter.field(record.producer_client_id.len(), MAX_CLIENT_ID_BYTES)?;
        meter.charge(
            256usize
                .checked_add(topic.len())
                .and_then(|n| n.checked_add(record.producer_client_id.len()))
                .ok_or(ReadError::LimitExceeded)?,
        )?;
        let publication = state
            .publications
            .get(&record.publication)
            .ok_or(ReadError::InvalidData)?;
        let payload = state
            .payloads
            .get(&publication.payload)
            .ok_or(ReadError::InvalidData)?;
        if topic.is_empty()
            || !mqttiumd_contract::topic::valid_filter(topic)
            || topic.iter().any(|b| matches!(*b, b'+' | b'#'))
            || record.producer_client_id.is_empty()
            || record.producer_client_id.contains(&0)
            || std::str::from_utf8(&record.producer_client_id).is_err()
            || publication.topic != *topic
            || !publication.retain
            || publication.retained_version.is_some()
            || payload.bytes.is_empty()
            || payload.references == 0
        {
            return Err(ReadError::InvalidData);
        }
        if payload.bytes.len() > state.limits.max_payload_bytes {
            return Err(ReadError::LimitExceeded);
        }
        total = total
            .checked_add(payload.bytes.len() as u64)
            .ok_or(ReadError::LimitExceeded)?;
        if total > MAX_RETAINED_BYTES {
            return Err(ReadError::LimitExceeded);
        }
    }
    Ok(())
}

pub(super) fn prepare_mutation(
    state: &mut DurableState,
    topic: &[u8],
    new_length: u64,
) -> Guard<()> {
    validate(state, ReadBudget::SNAPSHOT).map_err(|_| RejectReason::PreconditionFailed)?;
    if new_length > 0
        && !state.retained.contains_key(topic)
        && state.retained.len() >= MAX_RETAINED_TOPICS
    {
        return Err(RejectReason::CapacityExceeded(Resource::References));
    }
    let mut total = 0u64;
    for (name, record) in &state.retained {
        if name.as_slice() != topic {
            let publication = state
                .publications
                .get(&record.publication)
                .ok_or(RejectReason::PreconditionFailed)?;
            total = total
                .checked_add(
                    state
                        .payloads
                        .get(&publication.payload)
                        .ok_or(RejectReason::PreconditionFailed)?
                        .bytes
                        .len() as u64,
                )
                .ok_or(RejectReason::PreconditionFailed)?;
        }
    }
    if total
        .checked_add(new_length)
        .is_none_or(|n| n > MAX_RETAINED_BYTES)
    {
        return Err(RejectReason::CapacityExceeded(Resource::PayloadBytes));
    }
    if let Some(old) = state.retained.remove(topic) {
        let failed = RejectReason::PreconditionFailed;
        let id = state
            .publications
            .get(&old.publication)
            .ok_or(failed)?
            .payload;
        let payload = state.payloads.get_mut(&id).ok_or(failed)?;
        payload.references = payload.references.checked_sub(1).ok_or(failed)?;
        state.accounts.references = state
            .accounts
            .references
            .checked_sub(References::new(1))
            .ok_or(failed)?;
        if payload.references == 0 {
            state.accounts.payload_bytes = state
                .accounts
                .payload_bytes
                .checked_sub(PayloadBytes::new(payload.bytes.len() as u64))
                .ok_or(failed)?;
            state.payloads.remove(&id);
        }
        state.discard_completed_publication(old.publication);
    }
    Ok(())
}
pub(super) fn install(
    state: &mut DurableState,
    topic: &[u8],
    publication: PublicationId,
    source: SessionId,
) -> Guard<()> {
    let client = state
        .sessions
        .get(&source)
        .ok_or(RejectReason::PreconditionFailed)?
        .client_id
        .clone();
    if client.is_empty() || client.contains(&0) || std::str::from_utf8(&client).is_err() {
        return Err(RejectReason::PreconditionFailed);
    }
    state.retained.insert(
        topic.to_vec(),
        RetainedRecord {
            publication,
            producer_client_id: client,
        },
    );
    Ok(())
}

pub(super) fn subscribe(
    state: &mut DurableState,
    command: &ReplaySubscriptions,
) -> Guard<CommitFacts> {
    let update = &command.update;
    state.active_session(update.session, update.session_revision)?;
    validate(state, ReadBudget::SNAPSHOT).map_err(|_| RejectReason::PreconditionFailed)?;
    let client = state
        .sessions
        .get(&update.session)
        .ok_or(RejectReason::PreconditionFailed)?
        .client_id
        .clone();
    let mut plans = Vec::new();
    plans
        .try_reserve_exact(MAX_REPLAYS)
        .map_err(|_| RejectReason::CapacityExceeded(Resource::References))?;
    for (step, change) in update.changes.as_slice().iter().enumerate() {
        let existed = update.changes.as_slice()[..step]
            .iter()
            .rfind(|c| c.topic == change.topic)
            .map_or_else(
                || {
                    state
                        .routes
                        .get(change.topic.as_slice())
                        .is_some_and(|targets| targets.iter().any(|t| t.session == update.session))
                },
                |c| c.granted.is_some(),
            );
        if let Some(granted) = change.granted
            && (change.options.retain_handling() == 0
                || change.options.retain_handling() == 1 && !existed)
        {
            for (topic, value) in &state.retained {
                if mqttiumd_contract::topic::matches(change.topic.as_slice(), topic)
                    && !(change.options.no_local() && value.producer_client_id == client)
                {
                    plans.push((value.publication, granted));
                }
            }
        }
    }
    let facts = state.stage_updates(update)?;
    state.charge(Accounts {
        references: References::new(plans.len() as u64),
        ..Accounts::default()
    })?;
    let first = command.first_publication;
    for (index, (version, granted)) in plans.into_iter().enumerate() {
        let failed = RejectReason::PreconditionFailed;
        let publication = PublicationId::new(
            first.incarnation(),
            first.epoch(),
            first.owner(),
            PublicationSeq::new(
                first
                    .sequence()
                    .get()
                    .checked_add(index as u64)
                    .ok_or(failed)?,
            ),
        );
        let mut source = state.publications.get(&version).ok_or(failed)?.clone();
        let qos = source.qos.min(granted).min(Qos::AtLeastOnce);
        source.operation = update.operation;
        source.retained_version = Some(version);
        source.retain = true;
        let payload = state.payloads.get_mut(&source.payload).ok_or(failed)?;
        payload.references = payload.references.checked_add(1).ok_or(failed)?;
        if state.publications.insert(publication, source).is_some() {
            return Err(failed);
        }
        if state
            .obligations
            .insert(
                DeliveryId {
                    publication,
                    index: 0,
                },
                ObligationRecord {
                    retain: true,
                    target: update.session,
                    target_revision: update.session_revision,
                    qos,
                    cause: DeliveryCause::RetainedReplay,
                    phase: ObligationPhase::Pending,
                },
            )
            .is_some()
        {
            return Err(failed);
        }
    }
    Ok(facts)
}
