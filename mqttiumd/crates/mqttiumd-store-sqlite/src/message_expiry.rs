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
    validate(conn, None)?;
    retained::snapshot(conn, meta, ReadBudget::SNAPSHOT)?;
    let mut candidates =
        BoundedVec::with_capacity(limit as usize + 1).map_err(|_| ReadError::AllocationFailed)?;
    {
        let mut statement = conn.prepare_cached(
            "SELECT id,expiry_domain,expiry_tick,expiry_interval FROM publications WHERE ?1 IS NULL OR id>?1 ORDER BY id LIMIT ?2",
        )?;
        let mut rows = statement.query(params![
            after.map(|id| id.to_be_bytes().to_vec()),
            i64::from(limit) + 1
        ])?;
        while let Some(row) = rows.next()? {
            let id = PublicationId::from_be_slice(reader::borrow_blob(row, 0, 32)?)
                .map_err(|_| Fault::Corrupt)?;
            candidates
                .try_push((id, decode(row, 1)?))
                .map_err(|_| ReadError::LimitExceeded)?;
        }
    }
    let more = candidates.len() > limit as usize;
    let mut result = MessageExpirySweep {
        removed: BoundedVec::with_capacity(MESSAGE_EXPIRY_SWEEP_DELIVERIES)
            .map_err(|_| ReadError::AllocationFailed)?,
        more,
        next: if more {
            candidates
                .as_slice()
                .get(limit as usize - 1)
                .map(|(id, _)| *id)
        } else {
            None
        },
        ..MessageExpirySweep::default()
    };
    for (id, expiry) in candidates.as_slice().iter().take(limit as usize) {
        let Some(expiry) = expiry else {
            continue;
        };
        match expiry.state(now).map_err(|_| Fault::Corrupt)? {
            ExpiryState::Fresh { .. } => (),
            ExpiryState::TemporalUncertain => {
                result.quarantined += 1;
            }
            ExpiryState::Expired => {
                let topic: Option<Vec<u8>> = conn
                    .prepare_cached("SELECT topic FROM retained WHERE publication=?1 LIMIT 1")?
                    .query_row([id.to_be_bytes().as_slice()], |row| {
                        reader::copy_blob(row, 0, meta.limits.max_topic_bytes)
                    })
                    .optional()?;
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
