//! Immutable lifetime metadata of publications and deliveries: how it is
//! decoded, validated and guarded. The bounded maintenance that deletes
//! expired messages is the store's (`SqliteStore::expire_messages`).
use super::*;
use mqttiumd_contract::message_expiry::{MessageExpiry, MessageTime};

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

/// An inadmissible input domain is a business guard; an unreadable durable
/// clock remains a typed read failure rather than a fabricated rejection.
pub(super) fn guard_observation(conn: &Connection, now: MessageTime) -> Guard<()> {
    let failed = RejectReason::PreconditionFailed;
    let anchor = sessions::clock(conn)?.ok_or(Abort::Refused(failed))?;
    if now.domain == [0; 32] || now.domain != anchor.reading.domain {
        return refused(failed);
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
        "SELECT EXISTS(SELECT 1 FROM outbound c LEFT JOIN obligations o ON o.publication=c.publication AND o.idx=c.idx WHERE (?1 IS NULL OR c.target=?1) AND (typeof(c.started)!='integer' OR c.started NOT IN (0,1) OR o.publication IS NULL OR o.target!=c.target OR o.target_revision!=c.target_revision OR o.qos NOT IN (1,2) OR (o.phase=2 AND o.qos!=2) OR o.cause NOT IN (1,2)) LIMIT 1)",
        [target.as_ref().map(|b| b.as_slice())], |row| row.get(0),
    )?;
    if invalid {
        return Err(Fault::Corrupt);
    }
    Ok(())
}
