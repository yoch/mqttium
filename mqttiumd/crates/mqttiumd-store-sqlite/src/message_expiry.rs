//! Immutable lifetime metadata and receiptless bounded maintenance.
use super::*;
use mqttiumd_contract::message_expiry::{ExpiryState, MessageExpiry, MessageTime};
use mqttiumd_store_api::{
    ExpiredDelivery, MESSAGE_EXPIRY_SWEEP_DELIVERIES, MESSAGE_EXPIRY_SWEEP_PUBLICATIONS,
    MessageExpirySweep,
};

/// Decode the complete nullable shape before using any temporal predicate.
pub(super) fn decode(
    row: &rusqlite::Row<'_>,
    first: usize,
) -> Result<Option<MessageExpiry>, Fault> {
    use rusqlite::types::ValueRef;
    if matches!(row.get_ref(first)?, ValueRef::Null) {
        if !matches!(row.get_ref(first + 1)?, ValueRef::Null)
            || !matches!(row.get_ref(first + 2)?, ValueRef::Null)
        {
            return Err(Fault::Corrupt);
        }
        return Ok(None);
    }
    let domain: [u8; 32] = reader::borrow_blob(row, first, 32)?
        .try_into()
        .map_err(|_| Fault::Corrupt)?;
    if domain == [0; 32] {
        return Err(Fault::Corrupt);
    }
    let interval = match row.get_ref(first + 2)? {
        ValueRef::Integer(value) => u32::try_from(value).map_err(|_| Fault::Corrupt)?,
        _ => return Err(Fault::Corrupt),
    };
    let expiry = MessageExpiry {
        observed: MessageTime {
            domain,
            tick: blob_u64(reader::borrow_blob(row, first + 1, 8)?)?,
        },
        interval,
    };
    expiry.deadline().map_err(|_| Fault::Corrupt)?;
    Ok(Some(expiry))
}

pub(super) fn started(row: &rusqlite::Row<'_>, column: usize) -> Result<bool, Fault> {
    match row.get_ref(column)? {
        rusqlite::types::ValueRef::Integer(0) => Ok(false),
        rusqlite::types::ValueRef::Integer(1) => Ok(true),
        _ => Err(Fault::Corrupt),
    }
}

pub(super) fn publication(
    conn: &Connection,
    id: PublicationId,
) -> Result<Option<MessageExpiry>, Fault> {
    let mut statement = conn.prepare_cached(
        "SELECT expiry_domain,expiry_tick,expiry_interval FROM publications WHERE id=?1",
    )?;
    let mut rows = statement.query([id.to_be_bytes().as_slice()])?;
    let row = rows.next()?.ok_or(Fault::Corrupt)?;
    decode(row, 0)
}

/// A command's observation can precede the latest checkpoint. The current boot
/// domain establishes its authority; the per-message comparison checks elapsed
/// time. Logical session deadlines cannot authorize message deletion.
pub(super) fn check_observation(conn: &Connection, now: MessageTime) -> Result<(), Fault> {
    let anchor = sessions::clock(conn)?.ok_or(Fault::Corrupt)?;
    if now.domain == [0; 32] || now.domain != anchor.reading.domain {
        return Err(Fault::Corrupt);
    }
    Ok(())
}

/// Preflight metadata before a cursor or QoS filter can hide corruption. No
/// payload BLOB is selected, and the inspection has a finite row ceiling.
pub(super) fn validate(conn: &Connection, target: Option<SessionId>) -> Result<(), Fault> {
    let target = target.map(|s| s.to_be_bytes());
    let mut statement = conn.prepare_cached(
        "SELECT p.expiry_domain,p.expiry_tick,p.expiry_interval FROM publications p WHERE ?1 IS NULL OR EXISTS(SELECT 1 FROM obligations o WHERE o.publication=p.id AND o.target=?1) LIMIT ?2",
    )?;
    let mut rows = statement.query(params![
        target.as_ref().map(|b| b.as_slice()),
        i64::try_from(MAX_RECOVERY_INVENTORY_ROWS + 1).map_err(|_| Fault::Corrupt)?
    ])?;
    let mut count = 0usize;
    while let Some(row) = rows.next()? {
        count = count.checked_add(1).ok_or(ReadError::LimitExceeded)?;
        if count > MAX_RECOVERY_INVENTORY_ROWS {
            return Err(ReadError::LimitExceeded.into());
        }
        decode(row, 0)?;
    }
    let invalid: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM outbound c LEFT JOIN obligations o ON o.publication=c.publication AND o.idx=c.idx WHERE (?1 IS NULL OR c.target=?1) AND (typeof(c.started)!='integer' OR c.started NOT IN (0,1) OR o.publication IS NULL OR o.target!=c.target OR o.target_revision!=c.target_revision OR o.qos!=1 OR o.cause NOT IN (1,2)) LIMIT 1)",
        [target.as_ref().map(|b| b.as_slice())], |row| row.get(0),
    )?;
    if invalid {
        return Err(Fault::Corrupt);
    }
    Ok(())
}

fn guard_failure(error: Abort<RejectReason>) -> Fault {
    match error {
        Abort::Read(error) => Fault::Read(error),
        Abort::Fault | Abort::Refused(_) => Fault::Corrupt,
    }
}

/// Inspect one selected publication and its bounded responsibilities before any
/// freshness filter. The payload remains borrowed by SQLite; only topic keys
/// needed for a conditional retained deletion are copied.
fn candidate(
    conn: &Connection,
    meta: &Meta,
    id: PublicationId,
) -> Result<(Option<MessageExpiry>, Option<Vec<u8>>), Fault> {
    let mut statement = conn.prepare_cached("SELECT qos,retain,retained_version,topic,payload,expiry_domain,expiry_tick,expiry_interval FROM publications WHERE id=?1")?;
    let mut rows = statement.query([id.to_be_bytes().as_slice()])?;
    let row = rows.next()?.ok_or(Fault::Corrupt)?;
    code_qos(row.get(0)?)?;
    let retain = match row.get::<_, i64>(1)? {
        0 => false,
        1 => true,
        _ => return Err(Fault::Corrupt),
    };
    let version = reader::optional_blob(row, 2, 32)?
        .as_deref()
        .map(PublicationId::from_be_slice)
        .transpose()
        .map_err(|_| Fault::Corrupt)?;
    let topic = reader::borrow_blob(row, 3, meta.limits.max_topic_bytes)?;
    if topic.is_empty()
        || topic.contains(&0)
        || topic.iter().any(|b| matches!(b, b'+' | b'#'))
        || std::str::from_utf8(topic).is_err()
    {
        return Err(Fault::Corrupt);
    }
    let payload =
        PayloadId::from_be_slice(reader::borrow_blob(row, 4, 32)?).map_err(|_| Fault::Corrupt)?;
    let expiry = decode(row, 5)?;
    let mut retained_topic = None;
    let mut retained = conn.prepare_cached(
        "SELECT topic,producer_client_id FROM retained WHERE publication=?1 LIMIT 2",
    )?;
    let mut entries = retained.query([id.to_be_bytes().as_slice()])?;
    if let Some(row) = entries.next()? {
        let name = reader::borrow_blob(row, 0, meta.limits.max_topic_bytes)?;
        let client = reader::borrow_blob(row, 1, MAX_CLIENT_ID_BYTES)?;
        if name != topic
            || !retain
            || version.is_some()
            || client.is_empty()
            || client.contains(&0)
            || std::str::from_utf8(client).is_err()
        {
            return Err(Fault::Corrupt);
        }
        retained_topic = Some(reader::copy_blob(row, 0, meta.limits.max_topic_bytes)?);
        if entries.next()?.is_some() {
            return Err(Fault::Corrupt);
        }
    }
    let orphan: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM outbound c LEFT JOIN obligations o ON o.publication=c.publication AND o.idx=c.idx WHERE c.publication=?1 AND o.publication IS NULL LIMIT 1)", [id.to_be_bytes().as_slice()], |row| row.get(0))?;
    if orphan {
        return Err(Fault::Corrupt);
    }
    let mut obligations = conn.prepare_cached("SELECT o.idx,o.target,o.target_revision,o.qos,o.cause,o.phase,o.retain,c.target,c.target_revision,c.packet_id,c.claim,c.started,s.revision,s.state FROM obligations o LEFT JOIN outbound c ON c.publication=o.publication AND c.idx=o.idx LEFT JOIN sessions s ON s.id=o.target WHERE o.publication=?1 ORDER BY o.idx LIMIT ?2")?;
    let mut obligations = obligations.query(params![
        id.to_be_bytes().as_slice(),
        MESSAGE_EXPIRY_SWEEP_DELIVERIES as i64 + 1
    ])?;
    let mut count = 0usize;
    while let Some(row) = obligations.next()? {
        count += 1;
        if count > MESSAGE_EXPIRY_SWEEP_DELIVERIES {
            return Err(ReadError::LimitExceeded.into());
        }
        u32::try_from(row.get::<_, i64>(0)?).map_err(|_| Fault::Corrupt)?;
        let target = SessionId::from_be_slice(reader::borrow_blob(row, 1, 8)?)
            .map_err(|_| Fault::Corrupt)?;
        let revision = blob_u64(reader::borrow_blob(row, 2, 8)?)?;
        let qos = code_qos(row.get(3)?)?;
        let cause = code_cause(row.get(4)?)?;
        code_obligation_phase(row.get(5)?)?;
        let kept_retain = match row.get::<_, i64>(6)? {
            0 => false,
            1 => true,
            _ => return Err(Fault::Corrupt),
        };
        if (kept_retain && !retain)
            || (cause == mqttiumd_contract::command::DeliveryCause::LiveExact && version.is_some())
            || (cause == mqttiumd_contract::command::DeliveryCause::RetainedReplay
                && (!kept_retain
                    || !matches!(qos, Qos::AtMostOnce | Qos::AtLeastOnce)
                    || version.is_none()
                    || version == Some(id)))
            || code_session_state(row.get(13)?)? != SessionState::Active
            || blob_u64(reader::borrow_blob(row, 12, 8)?)? != revision
        {
            return Err(Fault::Corrupt);
        }
        if !matches!(row.get_ref(9)?, rusqlite::types::ValueRef::Null) {
            let packet = u16::try_from(row.get::<_, i64>(9)?)
                .ok()
                .and_then(OutboundPacketId::new)
                .ok_or(Fault::Corrupt)?;
            let claim = OperationId::from_be_slice(reader::borrow_blob(row, 10, 32)?)
                .map_err(|_| Fault::Corrupt)?;
            if qos != Qos::AtLeastOnce
                || claim.incarnation() != meta.ledger.incarnation()
                || reader::borrow_blob(row, 7, 8)? != target.to_be_bytes()
                || blob_u64(reader::borrow_blob(row, 8, 8)?)? != revision
            {
                return Err(Fault::Corrupt);
            }
            let _ = packet;
            started(row, 11)?;
        } else if !matches!(row.get_ref(7)?, rusqlite::types::ValueRef::Null)
            || !matches!(row.get_ref(8)?, rusqlite::types::ValueRef::Null)
            || !matches!(row.get_ref(10)?, rusqlite::types::ValueRef::Null)
            || !matches!(row.get_ref(11)?, rusqlite::types::ValueRef::Null)
        {
            return Err(Fault::Corrupt);
        }
    }
    if count != 0 || retained_topic.is_some() {
        let mut statement =
            conn.prepare_cached("SELECT refs,length,length(bytes) FROM payloads WHERE id=?1")?;
        let mut rows = statement.query([payload.to_be_bytes().as_slice()])?;
        let row = rows.next()?.ok_or(Fault::Corrupt)?;
        let refs = blob_u64(reader::borrow_blob(row, 0, 8)?)?;
        let length = blob_u64(reader::borrow_blob(row, 1, 8)?)?;
        if refs < count as u64 + u64::from(retained_topic.is_some())
            || refs > meta.limits.references.get()
            || length > meta.limits.max_payload_bytes as u64
            || u64::try_from(row.get::<_, i64>(2)?).map_err(|_| Fault::Corrupt)? != length
            || (retained_topic.is_some() && length == 0)
        {
            return Err(Fault::Corrupt);
        }
    }
    Ok((expiry, retained_topic))
}

pub(super) fn sweep(
    conn: &Connection,
    meta: &mut Meta,
    now: MessageTime,
    after: Option<PublicationId>,
    limit: u32,
) -> Result<MessageExpirySweep, Fault> {
    if limit == 0 || limit > MESSAGE_EXPIRY_SWEEP_PUBLICATIONS {
        return Err(ReadError::LimitExceeded.into());
    }
    check_observation(conn, now)?;
    let mut candidates =
        BoundedVec::with_capacity(limit as usize).map_err(|_| ReadError::AllocationFailed)?;
    {
        let mut statement = conn.prepare_cached(
            "SELECT id FROM publications WHERE ?1 IS NULL OR id>?1 ORDER BY id LIMIT ?2",
        )?;
        let mut rows = statement.query(params![
            after.map(|id| id.to_be_bytes().to_vec()),
            i64::from(limit)
        ])?;
        while let Some(row) = rows.next()? {
            let id = PublicationId::from_be_slice(reader::borrow_blob(row, 0, 32)?)
                .map_err(|_| Fault::Corrupt)?;
            let (expiry, retained_topic) = candidate(conn, meta, id)?;
            candidates
                .try_push((id, expiry, retained_topic))
                .map_err(|_| ReadError::LimitExceeded)?;
        }
    }
    let last = candidates.as_slice().last().map(|(id, _, _)| *id);
    let more = if let Some(last) = last {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM publications WHERE id>?1 LIMIT 1)",
            [last.to_be_bytes().as_slice()],
            |row| row.get(0),
        )?
    } else {
        false
    };
    let mut result = MessageExpirySweep {
        removed: BoundedVec::with_capacity(MESSAGE_EXPIRY_SWEEP_DELIVERIES)
            .map_err(|_| ReadError::AllocationFailed)?,
        more,
        next: if more { last } else { None },
        ..MessageExpirySweep::default()
    };
    for (id, expiry, topic) in candidates.as_slice() {
        let Some(expiry) = expiry else {
            continue;
        };
        match expiry.state(now).map_err(|_| Fault::Corrupt)? {
            ExpiryState::Fresh { .. } => (),
            ExpiryState::TemporalUncertain => {
                result.quarantined += 1;
            }
            ExpiryState::Expired => {
                let uncertain: i64 = conn.query_row("SELECT count(*) FROM (SELECT 1 FROM outbound WHERE publication=?1 AND started=0 LIMIT ?2)", params![id.to_be_bytes().as_slice(), MESSAGE_EXPIRY_SWEEP_DELIVERIES as i64 + 1], |row| row.get(0))?;
                let uncertain = u32::try_from(uncertain).map_err(|_| Fault::Corrupt)?;
                if uncertain as usize > MESSAGE_EXPIRY_SWEEP_DELIVERIES {
                    return Err(ReadError::LimitExceeded.into());
                }
                result.quarantined = result
                    .quarantined
                    .checked_add(uncertain)
                    .ok_or(ReadError::LimitExceeded)?;
                if let Some(topic) = topic {
                    // The topic and exact version are checked in this transaction.
                    let removed = conn
                        .prepare_cached("DELETE FROM retained WHERE topic=?1 AND publication=?2")?
                        .execute(params![topic, id.to_be_bytes().as_slice()])?;
                    if removed == 1 {
                        retained::release(conn, meta, *id).map_err(guard_failure)?;
                        result.retained_removed += 1;
                    }
                }
                let mut deliveries = BoundedVec::with_capacity(MESSAGE_EXPIRY_SWEEP_DELIVERIES)
                    .map_err(|_| ReadError::AllocationFailed)?;
                {
                    let mut statement = conn.prepare_cached("SELECT o.idx,o.target FROM obligations o WHERE o.publication=?1 AND NOT EXISTS(SELECT 1 FROM outbound c WHERE c.publication=o.publication AND c.idx=o.idx) ORDER BY o.idx LIMIT ?2")?;
                    let mut rows = statement.query(params![
                        id.to_be_bytes().as_slice(),
                        MESSAGE_EXPIRY_SWEEP_DELIVERIES as i64 + 1
                    ])?;
                    while let Some(row) = rows.next()? {
                        let delivery = DeliveryId {
                            publication: *id,
                            index: u32::try_from(row.get::<_, i64>(0)?)
                                .map_err(|_| Fault::Corrupt)?,
                        };
                        let target = SessionId::from_be_slice(reader::borrow_blob(row, 1, 8)?)
                            .map_err(|_| Fault::Corrupt)?;
                        deliveries
                            .try_push(ExpiredDelivery { delivery, target })
                            .map_err(|_| ReadError::LimitExceeded)?;
                    }
                }
                for item in deliveries.as_slice() {
                    result
                        .removed
                        .try_push(*item)
                        .map_err(|_| ReadError::LimitExceeded)?;
                    end_obligation(
                        conn,
                        meta,
                        &id.to_be_bytes(),
                        i64::from(item.delivery.index),
                    )
                    .map_err(|error| match error {
                        Abort::Read(error) => Fault::Read(error),
                        Abort::Fault | Abort::Refused(_) => Fault::Corrupt,
                    })?;
                }
            }
        }
    }
    Ok(result)
}
