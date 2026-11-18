//! Schema8 retained ownership and bounded transactional replay.
use super::*;
use mqttiumd_contract::ids::PublicationSeq;
use mqttiumd_contract::retained::{
    MAX_REPLAYS, MAX_RETAINED_BYTES, MAX_RETAINED_TOPICS, ReplaySubscriptions,
};

#[derive(Clone, Copy)]
pub(super) struct IdentityBounds {
    pub first: PublicationId,
    pub last: PublicationId,
    pub payload: Option<PayloadId>,
}

pub(super) fn bounds(recipe: &Recipe) -> Result<Option<IdentityBounds>, Fault> {
    let one = |p, payload| {
        Some(IdentityBounds {
            first: p,
            last: p,
            payload: Some(payload),
        })
    };
    if let Recipe::WillPublication(a) = recipe {
        return Ok(one(a.publication, a.payload.id));
    }
    if let Some(a) = recipe.admission() {
        return Ok(one(a.publication, a.payload.id));
    }
    if let Some(a) = recipe.retained_qos0() {
        return Ok(one(a.publication, a.payload.id));
    }
    Ok(if let Some(a) = recipe.replay_subscriptions() {
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
                        .ok_or(Fault::Corrupt)?,
                ),
            ),
            payload: None,
        })
    } else {
        None
    })
}

pub(super) fn receipt_bounds(
    meta: &Meta,
    operation: OperationId,
    record: &OperationRecord,
) -> Result<Option<IdentityBounds>, Fault> {
    // A subscription burns its entire reserved causal range even when the
    // business decision is refused. Its receipt is the witness until the
    // compact publication frontier covers the range before physical deletion.
    if matches!(record.decision, Decision::Rejected(_))
        && !matches!(record.recipe.get(6), Some(12 | 14))
    {
        return Ok(None);
    }
    let recipe = Recipe::decode_canonical(
        &record.recipe,
        meta.limits.max_targets_per_admission,
        meta.limits.max_topic_bytes,
    )
    .map_err(|error| {
        Fault::Read(match error {
            RecipeError::AllocationFailed => ReadError::AllocationFailed,
            RecipeError::TooManyTargets { .. } | RecipeError::TopicTooLong(_) => {
                ReadError::LimitExceeded
            }
            _ => ReadError::InvalidData,
        })
    })?;
    if recipe.operation() != operation {
        return Err(Fault::Corrupt);
    }
    let Some(bounds) = bounds(&recipe)? else {
        return Ok(None);
    };
    let valid = match record.decision {
        Decision::Rejected(_) => recipe.replay_subscriptions().is_some(),
        Decision::Committed(CommitFacts::WillPublished { will, publication }) => {
            matches!(&recipe,Recipe::WillPublication(a) if a.will==will && a.publication==publication)
        }
        Decision::Committed(CommitFacts::PublicationAdmitted {
            publication,
            exchange,
        }) => recipe
            .admission()
            .is_some_and(|a| a.publication == publication && a.source.exchange == exchange),
        Decision::Committed(CommitFacts::RetainedQos0Stored { publication }) => recipe
            .retained_qos0()
            .is_some_and(|a| a.publication == publication),
        Decision::Committed(CommitFacts::SubscriptionsChanged { session, .. }) => recipe
            .replay_subscriptions()
            .is_some_and(|a| a.update.session == session),
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
        return Err(Fault::Corrupt);
    }
    Ok(Some(bounds))
}

pub(super) fn identity_was_used(
    conn: &Connection,
    meta: &Meta,
    operation: OperationId,
    first: PublicationId,
    last: PublicationId,
    payload: Option<PayloadId>,
) -> Result<bool, Fault> {
    if let Some(closure) = receipts::frontier(conn, meta, ReceiptOrigin::of(operation))?
        && ((!receipts::MUTANT_ALLOW_OLD_PUBLICATION
            && first.sequence() <= closure.publication_through)
            || (!receipts::MUTANT_ALLOW_OLD_PAYLOAD
                && payload.is_some_and(|p| p.sequence() <= closure.payload_through)))
    {
        return Ok(true);
    }
    let live: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM publications WHERE id>=?1 AND id<=?2 LIMIT 1)",
        params![
            first.to_be_bytes().as_slice(),
            last.to_be_bytes().as_slice()
        ],
        |r| r.get(0),
    )?;
    if live {
        return Ok(true);
    }
    if let Some(payload) = payload
        && (exists(
            conn,
            "SELECT 1 FROM payloads WHERE id=?1",
            &payload.to_be_bytes(),
        )? || exists(
            conn,
            "SELECT 1 FROM publications WHERE payload=?1",
            &payload.to_be_bytes(),
        )?)
    {
        return Ok(true);
    }
    let op = |seq| {
        OperationId::new(
            operation.incarnation(),
            operation.epoch(),
            operation.owner(),
            OperationSeq::new(seq),
        )
        .to_be_bytes()
    };
    let mut statement=conn.prepare_cached("SELECT id,recipe,decision,publication,exchange,reason,resource,facts FROM operations WHERE id>=?1 AND id<=?2 ORDER BY id")?;
    let mut rows = statement.query(params![op(0).as_slice(), op(u64::MAX).as_slice()])?;
    let mut meter = reader::Meter::new(ReadBudget::SNAPSHOT);
    while let Some(row) = rows.next()? {
        let bytes = reader::borrow_blob(row, 1, ReadBudget::SNAPSHOT.max_field_bytes)?;
        meter.charge(
            256usize
                .checked_add(bytes.len())
                .ok_or(ReadError::LimitExceeded)?,
        )?;
        let id = OperationId::from_be_slice(reader::borrow_blob(row, 0, 32)?)
            .map_err(|_| Fault::Corrupt)?;
        let record = OperationRecord {
            recipe: reader::copy_blob(row, 1, ReadBudget::SNAPSHOT.max_field_bytes)?,
            decision: decision_of(row, 2)?.decode()?,
        };
        if let Some(stored) = receipt_bounds(meta, id, &record)?
            && (first <= stored.last && stored.first <= last
                || payload.is_some() && payload == stored.payload)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Materialize only bounded metadata. Payload lengths and reference counts are
/// validated without copying payload bytes, and producer fields are checked first.
pub(super) fn snapshot(
    conn: &Connection,
    meta: &Meta,
    budget: ReadBudget,
) -> Result<BTreeMap<Vec<u8>, RetainedRecord>, Fault> {
    let mut out = BTreeMap::new();
    let mut meter = reader::Meter::new(budget);
    let mut total = 0u64;
    let mut stmt=conn.prepare("SELECT r.topic,r.publication,r.producer_client_id,p.topic,p.qos,p.retain,p.retained_version,y.length,y.refs,length(y.bytes),p.expiry_domain,p.expiry_tick,p.expiry_interval FROM retained r LEFT JOIN publications p ON p.id=r.publication LEFT JOIN payloads y ON y.id=p.payload ORDER BY r.topic LIMIT ?1")?;
    let mut rows =
        stmt.query([i64::try_from(MAX_RETAINED_TOPICS + 1).map_err(|_| Fault::Corrupt)?])?;
    while let Some(row) = rows.next()? {
        if out.len() >= MAX_RETAINED_TOPICS {
            return Err(ReadError::LimitExceeded.into());
        }
        let topic = reader::borrow_blob(
            row,
            0,
            meta.limits
                .max_topic_bytes
                .min(budget.max_field_bytes)
                .min(usize::from(u16::MAX)),
        )?;
        let client = reader::borrow_blob(row, 2, MAX_CLIENT_ID_BYTES.min(budget.max_field_bytes))?;
        meter.charge(
            256usize
                .checked_add(topic.len())
                .and_then(|n| n.checked_add(client.len()))
                .ok_or(ReadError::LimitExceeded)?,
        )?;
        if topic.is_empty()
            || topic.contains(&0)
            || std::str::from_utf8(topic).is_err()
            || topic.iter().any(|b| matches!(*b, b'+' | b'#'))
            || client.is_empty()
            || client.contains(&0)
            || std::str::from_utf8(client).is_err()
            || reader::borrow_blob(row, 3, meta.limits.max_topic_bytes)? != topic
            || row.get::<_, i64>(5)? != 1
            || !matches!(row.get_ref(6)?, rusqlite::types::ValueRef::Null)
        {
            return Err(Fault::Corrupt);
        }
        code_qos(row.get(4)?)?;
        message_expiry::decode(row, 10)?;
        let length = blob_u64(reader::borrow_blob(row, 7, 8)?)?;
        if length == 0
            || length > meta.limits.max_payload_bytes as u64
            || blob_u64(reader::borrow_blob(row, 8, 8)?)? == 0
            || u64::try_from(row.get::<_, i64>(9)?).map_err(|_| Fault::Corrupt)? != length
        {
            return Err(Fault::Corrupt);
        }
        total = total.checked_add(length).ok_or(ReadError::LimitExceeded)?;
        if total > MAX_RETAINED_BYTES {
            return Err(ReadError::LimitExceeded.into());
        }
        out.insert(
            reader::copy_blob(row, 0, meta.limits.max_topic_bytes)?,
            RetainedRecord {
                publication: PublicationId::from_be_slice(reader::borrow_blob(row, 1, 32)?)
                    .map_err(|_| Fault::Corrupt)?,
                producer_client_id: reader::copy_blob(row, 2, MAX_CLIENT_ID_BYTES)?,
            },
        );
    }
    Ok(out)
}

/// Drop one retained-owned reference, never an obligation or another owner's ref.
pub(super) fn release(conn: &Connection, meta: &mut Meta, publication: PublicationId) -> Guard<()> {
    let row=conn.prepare_cached("SELECT p.payload,y.refs,y.length FROM publications p JOIN payloads y ON y.id=p.payload WHERE p.id=?1")?.query_row([publication.to_be_bytes().as_slice()],|row|Ok((reader::copy_blob(row,0,32)?,reader::copy_blob(row,1,8)?,reader::copy_blob(row,2,8)?)))?;
    let refs = blob_u64(&row.1)?.checked_sub(1).ok_or(Abort::Fault)?;
    meta.accounts.references = meta
        .accounts
        .references
        .checked_sub(References::new(1))
        .ok_or(Abort::Fault)?;
    if refs == 0 {
        conn.prepare_cached("DELETE FROM payloads WHERE id=?1")?
            .execute([row.0.as_slice()])?;
        meta.accounts.payload_bytes = meta
            .accounts
            .payload_bytes
            .checked_sub(PayloadBytes::new(blob_u64(&row.2)?))
            .ok_or(Abort::Fault)?;
    } else {
        conn.prepare_cached("UPDATE payloads SET refs=?2 WHERE id=?1")?
            .execute(params![row.0, u64_blob(refs).as_slice()])?;
    }
    discard_completed_publication(conn, &publication.to_be_bytes())?;
    Ok(())
}

pub(super) fn prepare_mutation(
    conn: &Connection,
    meta: &mut Meta,
    topic: &[u8],
    new_length: u64,
) -> Guard<()> {
    let inventory = snapshot(conn, meta, ReadBudget::SNAPSHOT)?;
    if new_length > 0 && !inventory.contains_key(topic) && inventory.len() >= MAX_RETAINED_TOPICS {
        return refused(RejectReason::CapacityExceeded(Resource::References));
    }
    let mut bytes = 0u64;
    for (name, record) in &inventory {
        if name.as_slice() != topic {
            let length:Vec<u8>=conn.prepare_cached("SELECT y.length FROM publications p JOIN payloads y ON y.id=p.payload WHERE p.id=?1")?.query_row([record.publication.to_be_bytes().as_slice()],|row|reader::copy_blob(row,0,8))?;
            bytes = bytes.checked_add(blob_u64(&length)?).ok_or(Abort::Fault)?;
        }
    }
    if bytes
        .checked_add(new_length)
        .is_none_or(|n| n > MAX_RETAINED_BYTES)
    {
        return refused(RejectReason::CapacityExceeded(Resource::PayloadBytes));
    }
    if let Some(old) = inventory.get(topic) {
        conn.prepare_cached("DELETE FROM retained WHERE topic=?1")?
            .execute([topic])?;
        release(conn, meta, old.publication)?;
    }
    Ok(())
}

pub(super) fn install(
    conn: &Connection,
    topic: &[u8],
    publication: PublicationId,
    source: SessionId,
    will_source: Option<&[u8]>,
) -> Guard<()> {
    let session_client;
    let client = if let Some(client) = will_source {
        client
    } else {
        session_client = conn
            .prepare_cached("SELECT client_id FROM sessions WHERE id=?1")?
            .query_row([source.to_be_bytes().as_slice()], |row| {
                reader::copy_blob(row, 0, MAX_CLIENT_ID_BYTES)
            })?;
        &session_client
    };
    if client.is_empty() || client.contains(&0) || std::str::from_utf8(client).is_err() {
        return refused(RejectReason::PreconditionFailed);
    }
    conn.prepare_cached(
        "INSERT INTO retained(topic,publication,producer_client_id) VALUES(?1,?2,?3)",
    )?
    .execute(params![topic, publication.to_be_bytes().as_slice(), client])?;
    Ok(())
}

pub(super) fn subscribe(
    conn: &Connection,
    meta: &mut Meta,
    command: &ReplaySubscriptions,
    observed: Option<MessageTime>,
) -> Guard<CommitFacts> {
    let update = &command.update;
    if let Some(now) = observed {
        message_expiry::guard_observation(conn, now)?;
    }
    active_session(conn, update.session, update.session_revision)?;
    let first = command.first_publication;
    let last = PublicationId::new(
        first.incarnation(),
        first.epoch(),
        first.owner(),
        PublicationSeq::new(
            first
                .sequence()
                .get()
                .checked_add(MAX_REPLAYS as u64 - 1)
                .ok_or(Abort::Fault)?,
        ),
    );
    if identity_was_used(conn, meta, update.operation, first, last, None)? {
        return refused(RejectReason::PreconditionFailed);
    }
    let inventory = snapshot(conn, meta, ReadBudget::SNAPSHOT)?;
    let client = conn
        .prepare_cached("SELECT client_id FROM sessions WHERE id=?1")?
        .query_row([update.session.to_be_bytes().as_slice()], |row| {
            reader::copy_blob(row, 0, MAX_CLIENT_ID_BYTES)
        })?;
    let mut plans = Vec::new();
    plans
        .try_reserve_exact(MAX_REPLAYS)
        .map_err(|_| ReadError::AllocationFailed)?;
    // Presence is evaluated at each wire-ordered step, not from the final projection.
    for (step, change) in update.changes.as_slice().iter().enumerate() {
        let name = change.topic.as_slice();
        let exists = conn
            .prepare_cached("SELECT 1 FROM routes WHERE topic=?1 AND session=?2 LIMIT 1")?
            .query_row(
                params![name, update.session.to_be_bytes().as_slice()],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        // Prior removals need an explicit false state, so derive from previous steps.
        let existed = update.changes.as_slice()[..step]
            .iter()
            .rfind(|c| c.topic == change.topic)
            .map_or(exists, |c| c.granted.is_some());
        if let Some(granted) = change.granted
            && (change.options.retain_handling() == 0
                || change.options.retain_handling() == 1 && !existed)
        {
            for (topic, value) in &inventory {
                if mqttiumd_contract::topic::matches(name, topic)
                    && !(change.options.no_local() && value.producer_client_id == client)
                {
                    let expiry = message_expiry::publication(conn, value.publication)?;
                    let eligible = match (expiry, observed) {
                        (None, _) => true,
                        (Some(expiry), Some(now)) => matches!(
                            expiry
                                .state(now)
                                .map_err(|_| Abort::Refused(RejectReason::PreconditionFailed))?,
                            ExpiryState::Fresh { .. }
                        ),
                        (Some(_), None) => false,
                    };
                    if eligible {
                        plans.push((value.publication, granted));
                    }
                }
            }
        }
    }
    // Route update and every replay reference are one rollback-able decision.
    let facts = pubsub::updates(conn, meta, update)?;
    charge(
        meta,
        Accounts {
            references: References::new(plans.len() as u64),
            ..Accounts::default()
        },
    )?;
    for (index, (version, granted)) in plans.into_iter().enumerate() {
        let publication = PublicationId::new(
            first.incarnation(),
            first.epoch(),
            first.owner(),
            PublicationSeq::new(
                first
                    .sequence()
                    .get()
                    .checked_add(index as u64)
                    .ok_or(Abort::Fault)?,
            ),
        );
        let row = conn
            .prepare_cached("SELECT source,qos,payload,topic FROM publications WHERE id=?1")?
            .query_row([version.to_be_bytes().as_slice()], |row| {
                Ok((
                    reader::copy_blob(row, 0, 8)?,
                    row.get::<_, i64>(1)?,
                    reader::copy_blob(row, 2, 32)?,
                    reader::copy_blob(row, 3, meta.limits.max_topic_bytes)?,
                ))
            })?;
        let qos = code_qos(row.1)?.min(granted).min(Qos::AtLeastOnce);
        conn.prepare_cached("INSERT INTO publications(id,operation,source,qos,retain,payload,retained_version,expiry_domain,expiry_tick,expiry_interval,topic) SELECT ?1,?2,?3,?4,1,?5,?6,expiry_domain,expiry_tick,expiry_interval,?7 FROM publications WHERE id=?6")?.execute(params![publication.to_be_bytes().as_slice(),update.operation.to_be_bytes().as_slice(),row.0,row.1,row.2,version.to_be_bytes().as_slice(),row.3])?;
        conn.prepare_cached("INSERT INTO obligations(publication,idx,target,target_revision,qos,cause,phase,retain) VALUES(?1,0,?2,?3,?4,2,1,1)")?.execute(params![publication.to_be_bytes().as_slice(),update.session.to_be_bytes().as_slice(),update.session_revision.to_be_bytes().as_slice(),qos_code(qos)])?;
        let refs: Vec<u8> = conn
            .prepare_cached("SELECT refs FROM payloads WHERE id=?1")?
            .query_row([row.2.as_slice()], |row| reader::copy_blob(row, 0, 8))?;
        let refs = blob_u64(&refs)?.checked_add(1).ok_or(Abort::Fault)?;
        conn.prepare_cached("UPDATE payloads SET refs=?2 WHERE id=?1")?
            .execute(params![row.2, u64_blob(refs).as_slice()])?;
    }
    Ok(facts)
}
